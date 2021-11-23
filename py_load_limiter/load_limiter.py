import collections
import time
import math
import threading
import logging
import contextlib, functools
from typing import ContextManager, Optional

from .types import LoadLimiterSubmitResult, LoadLimitExceeded
from .persistence import LoadLimiterSerializedStatus, LoadLimiterStorageAdapter


class LoadLimiter(object):

    def __init__(
        self, 
        name: str = None,
        maxload: float = 60,
        period: int = 60,
        fragmentation: float = 0.05,
        penalty_factor: float = 0.10,
        penalty_distribution_factor: float = 0.2,
        request_overhead_penalty_factor: float = 0.00,
        request_overhead_penalty_distribution_factor: float = 0.30,
        max_penalty_cap_factor: float = 0.33,
        compute_tta: bool = True,
        logger: logging.Logger = None,
        storage_adapter: Optional[LoadLimiterStorageAdapter] = None
    ):
        self.logger = logger if logger is not None else logging.getLogger("loadlimiter")

        if maxload <= 0:
            raise ValueError('maxload should be a positive integer')
        if period <= 0:
            raise ValueError('period should be a positive integer')
        if fragmentation < 0.01 or fragmentation > 1.0:
            raise ValueError('fragmentation should be a positive float in the range 0.01 - 1.0')
        if penalty_factor < 0:
            raise ValueError('penalty_factor should not be negative')
        if penalty_distribution_factor < 0 or penalty_distribution_factor > 1:
            raise ValueError('penalty_distribution_factor should be a positive float in the range 0.0 - 1.0')
        if request_overhead_penalty_factor < 0:
            raise ValueError('request_overhead_penalty_factor should not be negative')
        if request_overhead_penalty_distribution_factor < 0 or request_overhead_penalty_distribution_factor > 1:
            raise ValueError('request_overhead_penalty_distribution_factor should be a positive float in the range 0.0 - 1.0')
        if max_penalty_cap_factor < 0:
            raise ValueError('max_penalty_cap_factor should not be negative')
        
        overstep_penalty = int(maxload * penalty_factor)
        if overstep_penalty <= 0:
            overstep_penalty = 0
        
        step_period = math.ceil(period * fragmentation)
        if step_period <= 1:
            step_period = 1

        num_max_buckets = math.ceil(period / step_period)

        max_cap = maxload * (1.0 + max_penalty_cap_factor)

        self.num_max_buckets = num_max_buckets
        self.max_cap = max_cap
        self.compute_tta = compute_tta
        self.name = name
        self.overstep_penalty = overstep_penalty
        self.step_period = step_period
        self.maxload = maxload
        self.period = period
        self.penalty_distribution_factor = penalty_distribution_factor
        self.request_overhead_penalty_factor = request_overhead_penalty_factor
        self.request_overhead_penalty_distribution_factor = request_overhead_penalty_distribution_factor

        self.queue = collections.deque()
        self.window_total = 0
        self.num_calls = 0
        self.total_overhead = 0
        self.was_over = False

        self.status_dirty = False

        self.lock = threading.Lock()
        self.storage_adapter = storage_adapter
    
    def __call__(self, load: int = 1, wait: bool = True, timeout: int = 60):
        """
        The __call__ function allows the LoadLimiter object to be used as a
        regular function decorator.
        """
        def command_handler_decorator(func):
            @functools.wraps(func)
            def command_func(*args, **kwargs):
                self._submitting(load=load, wait=wait, timeout=timeout, task_name=func.__name__)
                return func(*args, **kwargs)

            return command_func
        return command_handler_decorator

    def attempting(self, load: int = 1) -> ContextManager[LoadLimiterSubmitResult]:
        return self.submitting(load=load, wait=False)

    def waiting(self, load: int = 1, timeout: int = 60) -> ContextManager[LoadLimiterSubmitResult]:
        return self.submitting(load=load, wait=True, timeout=timeout)

    @contextlib.contextmanager
    def submitting(self, load: int = 1, wait: bool = True, timeout: int = 60) -> ContextManager[LoadLimiterSubmitResult]:
        submit_result = self._submitting(load=load, wait=wait, timeout=timeout)
        yield submit_result  # NOSONAR

    def submit(self, load: float = 1) -> LoadLimiterSubmitResult:
        with self.lock:
            return self._submit(load=load)

    def instant_load_factor(self) -> float:
        with self.lock:
            return self._instant_load_factor()
     
    def distribute(self, amount):
        with self.lock:
            self._rotate_window_to_current_time()
            self._distribute_penalty(amount, 1.0)
            self._status_dirty()

    def flush(self, force = False) -> bool:
        if not self.storage_adapter:
            self.logger.warning('flush called but no storage adapter is available. status will not be dumped')
            return False

        if not self.status_dirty:
            if not force:
                self.logger.debug('flush called but instance is not dirty. status will not be dumped')
                return False
            else:
                self.logger.debug('flush called but instance is not dirty. dumping anyway because force = True')

        with self.lock:
            status_dump = self._dump_status()
            try:
                self.storage_adapter.save(status_dump)
            except Exception as e:
                self.logger.error('error flushing status to storage adapter', exc_info=1)
                raise e
            self.logger.debug('status flushed to storage adapter')
            self.status_dirty = False
            return True

    def restore(
        self, 
        from_status: Optional[LoadLimiterSerializedStatus] = None, 
        from_adapter: Optional[LoadLimiterStorageAdapter] = None
    ) -> bool:
        if not self.storage_adapter and not from_adapter and not from_status:
            self.logger.warning('restore called but no storage adapter is available. status will not be restored')
            return False
        if from_status and from_adapter:
            raise ValueError('restore called with conflicting arguments. Only one of from_status and from_adapter allowed')

        to_restore: Optional[LoadLimiterSerializedStatus] = None
        if from_status:
            to_restore = from_status
        elif from_adapter:
            try:
                to_restore = from_adapter.read()
            except Exception as e:
                self.logger.error('error reading status from specified storage adapter', exc_info=1)
                raise e
        else:
            try:
                to_restore = self.storage_adapter.read()
            except Exception as e:
                self.logger.error('error reading status from embedded storage adapter', exc_info=1)
                raise e

        if not to_restore:
            self.logger.debug('restore called but not status was found. status will not be restored')
            return False

        self.logger.debug('restoring status from dump')
        with self.lock:
            self._restore_from_status(to_restore)
        self.logger.info('status restored from dump')

        self.status_dirty = False
        return True

    def _status_dirty(self):
        self.status_dirty = True

    def _submitting(self, load: int = 1, wait: bool = True, timeout: int = 60, task_name: str = None):
        _start = time.time()
        while True:
            submit_result = self.submit(load)
            if submit_result.accepted:
                break

            if submit_result.retry_in is None or submit_result.retry_in <= 0 or not wait:
                self.logger.debug('submit of task {}failed and can\'t retry'.format(
                    task_name + ' ' if task_name is not None else ''
                ))
                raise LoadLimitExceeded(submit_result.retry_in)

            will_wait = math.ceil(submit_result.retry_in)

            if timeout is not None and (time.time() - _start + will_wait) >= timeout:
                raise TimeoutError()

            self.logger.debug('submit of task {}failed, waiting {} sec and retrying'.format(
                task_name + ' ' if task_name is not None else '',
                will_wait
            ))
            time.sleep(will_wait)

        return submit_result
   
    def _instant_load_factor(self) -> float:
        self._rotate_window_to_current_time()
        if self.window_total == 0:
            return 0
        v = self.window_total / self.maxload
        return v

    def _rotate_window_to_current_time(self):
        t = time.time()
        t_start = int(int(t / self.step_period) * self.step_period)        

        if len(self.queue) <= 0 or self.queue[-1][0] != t_start:
            entry = [t_start, 0]
            self.queue.append(entry)

            # remove old entries
            remove_before = t - self.period
            while True:
                first_el = self.queue[0]
                if first_el[0] < remove_before:
                    self.window_total -= first_el[1]
                    self._correct_drifting_descending()
                    self.queue.popleft()
                    self._status_dirty()
                else:
                    break

    def _correct_drifting_descending(self): # pragma: defensive
        if self.window_total < 0:
            if abs(self.window_total) >= 0.1:
                self.logger.debug('corrected drift error (in descending direction): {} != 0'.format(self.window_total))
                self._status_dirty()
            self.window_total = 0
    
    def _correct_driftin_ascending(self):  # pragma: defensive
        retot = 0
        for el in self.queue:
            retot += el[1]
        diff_abs = abs(retot - self.window_total)
        if diff_abs > 0.001:
            if diff_abs >= 0.1:
                self.logger.debug('corrected drift error (in ascending direction): {} != {}'.format(self.window_total, retot))
                self._status_dirty()
            self.window_total = retot

    def _submit_probe(self, load: float):
        t = time.time()
        self.num_calls += 1

        self._rotate_window_to_current_time()
        
        total_would_be = self.window_total + load
        if total_would_be > self.maxload:
            ret = False
        else:
            ret = True

        self.total_overhead += (time.time() - t)
        return ret

    def _submit_accept(self, load: float):
        t = time.time()

        entry = self.queue[-1]
        response_tta = None
        p_before = 100.0 * self.window_total / self.maxload
        self.was_over = False
        self.window_total += load
        entry[1] += load

        over_max_cap = self.window_total - self.max_cap
        if over_max_cap > 0:
            self._remove_from_oldest(over_max_cap)

        p_after = 100.0 * self.window_total / self.maxload

        if self.logger.isEnabledFor(logging.DEBUG):
            self._print_range(p_before, p_after, True)

        self.total_overhead += (time.time() - t)

        self._status_dirty()
        return LoadLimiterSubmitResult(True, retry_in=response_tta)

    def _submit_reject(self, load: float):  # NOSONAR - single function because it must be performance - optimized
        t = time.time()

        response_tta = None
        p_before = 100.0 * self.window_total / self.maxload

        over_max_cap = self.window_total - self.max_cap
        if over_max_cap > 0:
            self._remove_from_oldest(over_max_cap)

        if not self.was_over:
            # RECOMPUTE window_total FROM QUEUE VALUES TO AVOID LONG-RUNNING ROUNDING ERRORS
            self._correct_driftin_ascending()

            if self.overstep_penalty > 0:
                # apply penalty to last buckets
                self._distribute_penalty(self.overstep_penalty, self.penalty_distribution_factor)
        else:
            # was already overhead. apply request_overhead_penalty_factor if needed
            if self.request_overhead_penalty_factor > 0:
                _overhead_penalty = load * self.request_overhead_penalty_factor
                if _overhead_penalty > 0:
                    self._distribute_penalty(_overhead_penalty, self.request_overhead_penalty_distribution_factor)

        self.was_over = True

        if self.compute_tta:
            # compute time to availability
            # required load was 'load'
            # read from left of queue until at least 'load' is accumulated in total bucket load
            # add to 'load' also everything over the current maxload
            if load > self.maxload:
                # load will never be allowed
                response_tta = None
            else:
                acc_tta = 0
                to_free_for_tta = load
                if self.window_total > self.maxload:
                    to_free_for_tta += (self.window_total - self.maxload)
                else:
                    to_free_for_tta -= (self.maxload - self.window_total)
                if to_free_for_tta <= 0:
                    self.logger.warning('error in TTA computing: inconsistent TTA compute base. a default value will be returned')
                    response_tta = 1
                else:
                    last_bucket = None
                    for el in self.queue:
                        last_bucket = el
                        acc_tta += el[1]
                        if acc_tta >= to_free_for_tta:
                            break
                    
                    if acc_tta < to_free_for_tta:
                        # no TTA can be computed (requested load > maxload ?)
                        response_tta = None
                    else:
                        # get the time of the last read bucket
                        # that bucket will be removed when bucket[0] < (t - self.period)
                        # so find minimum future 't' for which 't' > bucket[0] + self.period
                        response_tta = last_bucket[0] + self.period - time.time()

        p_after = 100.0 * self.window_total / self.maxload

        if self.logger.isEnabledFor(logging.DEBUG):
            self._print_range(p_before, p_after, False)
            #self._print_window()

        self.total_overhead += (time.time() - t)

        self._status_dirty()
        return LoadLimiterSubmitResult(False, retry_in=response_tta)

    def _submit(
        self, load: float = 1
    ) -> LoadLimiterSubmitResult:
        if self._submit_probe(load=load):
            return self._submit_accept(load=load)
        else:
            return self._submit_reject(load=load)

    def _remove_from_oldest(self, amount):
        # try to remove from the left
        for el in self.queue:
            if el[1] > 0:
                to_sub_from_bucket = min(el[1], amount)
                el[1] -= to_sub_from_bucket
                amount -= to_sub_from_bucket
                self.window_total -= to_sub_from_bucket
                self._status_dirty()
            if amount <= 0:
                break
        if amount > 0:
            # should never happen. just emit a warning
            self.logger.warning('cannot sub excess over max cap starting from oldest entryies')

    def _distribute_penalty(self, amount, distribution_factor):
        qlen = len(self.queue)
        if qlen < 1:
            # no buckets!
            return

        if amount <= 0:
            return

        num_buckets_to_penalty = int(self.num_max_buckets * distribution_factor)
        amount_for_bucket = (amount / num_buckets_to_penalty) if num_buckets_to_penalty > 1 else 0

        if num_buckets_to_penalty <= 1 or amount_for_bucket <= 1:
            # fallback on placing all the penalty on the last bucket
            num_buckets_to_penalty = 1
            amount_for_bucket = amount

        self.window_total += amount
        last_bucket_start = self.queue[-1][0]
        self._status_dirty()
        for ix in range(0, num_buckets_to_penalty):
            # check if the bucket exists
            expected_bucket_start_time = last_bucket_start - ix * self.step_period
            if qlen <= ix:
                # can't access from right index (not enough elements)
                # create the bucket
                buck_empty = [expected_bucket_start_time, 0]
                # insert the new bucket at the left
                self.queue.appendleft(buck_empty)
                qlen += 1
                # will operate on the newly created bucket
                b = buck_empty
            else:
                b = self.queue[-(ix + 1)]
                if b[0] < expected_bucket_start_time:
                    # bucket exists but is older than expected. create a middle-bucket
                    buck_empty = [expected_bucket_start_time, 0]
                    # insert the new bucket at the left
                    self.queue.insert(-ix, buck_empty)
                    qlen += 1
                    # will operate on the newly created bucket
                    b = buck_empty

            b[1] += amount_for_bucket

        over_max_cap = self.window_total - self.max_cap
        if over_max_cap > 0:
            self._remove_from_oldest(over_max_cap)
        self._status_dirty()

    def _print_window(self):
        window_bucket_format = '{:' + str(len(str(self.maxload))) + '.2f}'
        line = 'current window: ['
        for bucket in self.queue:
            line += window_bucket_format.format(bucket[1]) + ' '
        line += ']'
        self.logger.debug(line)

    def _print_range(self, rmin, rmax, ret):
        ilf = self._instant_load_factor()
        p_step = 5
        acc = 0
        name_raw = self.name if self.name is not None else self.__class__.__name__
        if len(name_raw) > 12:
            name_raw = name_raw[:4] + '...' + name_raw[-4:]
        line = '[{:12s}] ['.format(name_raw)
        while acc < min(rmin, 100):
            line += '='
            acc += p_step
        while acc < min(rmax, 100):
            line += '-'
            acc += p_step
        while acc < 100:
            line += ' '
            acc += p_step
        line += '] '
        if ret:
            line += '[a] '
        else:
            line += '[R] '
        line += '[{:3.0f}/{:3.0f}] '.format(self.window_total, self.maxload)
        line += '['
        for el in self.queue:
            pcg = math.ceil(10 * el[1] / self.maxload)
            if pcg > 9:
                pcg = 9
            line += str(pcg)[0]
        line += '] '

        avg_oh = 1000 * (self.total_overhead / self.num_calls)
        line += '({:1.2f}inst {:1.0f}r {:1.2f}ms/r)'.format(ilf, self.num_calls, avg_oh)
        self.logger.debug(line)

    def _dump_status(self) -> LoadLimiterSerializedStatus:
        return LoadLimiterSerializedStatus(
            name = self.name,
            maxload = self.maxload,
            period = self.period,
            num_max_buckets = self.num_max_buckets,
            max_cap = self.max_cap,
            compute_tta = self.compute_tta,
            overstep_penalty = self.overstep_penalty,
            step_period = self.step_period,
            penalty_distribution_factor = self.penalty_distribution_factor,
            request_overhead_penalty_factor = self.request_overhead_penalty_factor,
            request_overhead_penalty_distribution_factor = self.request_overhead_penalty_distribution_factor,
            window_total = self.window_total,
            num_calls = self.num_calls,
            total_overhead = self.total_overhead,
            was_over = self.was_over,
            window = [e for e in self.queue]
        )

    def _restore_from_status(self, status: LoadLimiterSerializedStatus):
        self.name = status.name
        self.maxload = status.maxload
        self.period = status.period
        self.num_max_buckets = status.num_max_buckets
        self.max_cap = status.max_cap
        self.compute_tta = status.compute_tta
        self.overstep_penalty = status.overstep_penalty
        self.step_period = status.step_period
        self.penalty_distribution_factor = status.penalty_distribution_factor
        self.request_overhead_penalty_factor = status.request_overhead_penalty_factor
        self.request_overhead_penalty_distribution_factor = status.request_overhead_penalty_distribution_factor
        self.window_total = status.window_total
        self.num_calls = status.num_calls
        self.total_overhead = status.total_overhead
        self.was_over = status.was_over

        self.queue = collections.deque()
        for e in status.window:
            self.queue.append(e)

        self.status_dirty = False