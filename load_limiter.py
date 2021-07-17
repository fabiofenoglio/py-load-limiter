import collections
import time
import math
import threading
import logging
import contextlib, functools
from typing import ContextManager, Optional

file_logger = logging.getLogger(__name__)

class LoadLimitExceeded(Exception):
    def __init__(self, retry_in: Optional[float] = None):
        super().__init__()
        self.retry_in = retry_in
    def __str__(self) -> str:
        return self.__repr__()
    def __repr__(self) -> str:
        s = 'LoadLimitExceeded'
        if self.retry_in is not None:
            s += ' (load capacity available in {:.3f} seconds)'.format(self.retry_in) 
        return s

class LoadLimiterSubmitResult:
    def __init__(self, accepted: bool, retry_in: Optional[float] = None):
        self.accepted = accepted
        self.retry_in = retry_in

class LoadLimiter(object):

    def __init__(
        self, 
        name: str = None,
        maxload: int = 60,
        period: int = 60,
        fragmentation: float = 0.075,
        penalty_factor: float = 0.30,
        penalty_distribution_factor: float = 0.2,
        request_overhead_penalty_factor: float = 0.30,
        request_overhead_penalty_distribution_factor: float = 0.30,
        max_penalty_cap_factor: float = 0.5,
        compute_tta: bool = True,
        logger: logging.Logger = None
    ):
        self.logger = logger if logger is not None else file_logger

        if maxload < 1:
            raise ValueError('maxload should be a positive integer')
        if period < 1:
            raise ValueError('period should be a positive integer')
        if fragmentation < 0.05 or fragmentation > 1.0:
            raise ValueError('fragmentation should be a positive float in the range 0.05 - 1.0')
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
        if overstep_penalty < 0:
            overstep_penalty = 0
        
        step_period = math.ceil(period * fragmentation)
        if step_period < 1:
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

        self.lock = threading.Lock()
    
    def attempting(self, load: int = 1) -> ContextManager[LoadLimiterSubmitResult]:
        return self.submitting(load=load, wait=False)

    def waiting(self, load: int = 1, timeout: int = 60) -> ContextManager[LoadLimiterSubmitResult]:
        return self.submitting(load=load, wait=True, timeout=timeout)

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

    @contextlib.contextmanager
    def submitting(self, load: int = 1, wait: bool = True, timeout: int = 60) -> ContextManager[LoadLimiterSubmitResult]:
        submit_result = self._submitting(load=load, wait=wait, timeout=timeout)
        yield submit_result  # NOSONAR

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

    def instant_load_factor(self) -> float:
        with self.lock:
            if self.window_total == 0:
                return 0
            v = self.window_total / self.maxload
            if v > 1.0:
                v = 1.0
            return v

    def submit(self, load: float = 1) -> LoadLimiterSubmitResult:
        with self.lock:
            return self._submit(load=load)

    def _submit( # NOSONAR - single function because it must be performance - optimized
        self, 
        load: float = 1
    ) -> LoadLimiterSubmitResult:
        self.num_calls += 1
        t = time.time()
        t_start = int(int(t / self.step_period) * self.step_period)        

        if len(self.queue) > 0 and self.queue[-1][0] == t_start:
            # still same entry
            entry = self.queue[-1]
        else:
            entry = [t_start, 0]
            self.queue.append(entry)

            # remove old entries
            remove_before = t - self.period
            while True:
                first_el = self.queue[0]
                if first_el[0] < remove_before:
                    self.window_total -= first_el[1]
                    if self.window_total < 0:
                        if abs(self.window_total) >= 0.1:
                            self.logger.debug('corrected drift error (in descending direction): {} != 0'.format(self.window_total))
                        self.window_total = 0
                    self.queue.popleft()
                else:
                    break
        
        p_before = 100.0 * self.window_total / self.maxload
        response_tta = None

        self.window_total += load
        entry[1] += load

        if self.window_total <= self.maxload:        
            ret = True
            self.was_over = False
        else:
            ret = False
            if not self.was_over:
                # RECOMPUTE window_total FROM QUEUE VALUES TO AVOID LONG-RUNNING ROUNDING ERRORS
                retot = 0
                for el in self.queue:
                    retot += el[1]
                diff_abs = abs(retot - self.window_total)
                if diff_abs > 0.001:
                    if diff_abs >= 0.1:
                        self.logger.debug('corrected drift error (in ascending direction): {} != {}'.format(self.window_total, retot))
                    self.window_total = retot

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
                acc_tta = 0
                to_free_for_tta = load
                if self.window_total > self.maxload:
                    to_free_for_tta += (self.window_total - self.maxload)
                last_bucket = None
                for el in self.queue:
                    last_bucket = el
                    acc_tta += el[1]
                    if acc_tta >= to_free_for_tta:
                        break
                
                if acc_tta < load:
                    # no TTA can be computed (requested load > maxload ?)
                    response_tta = None
                else:
                    # get the time of the last read bucket
                    # that bucket will be removed when bucket[0] < (t - self.period)
                    # so find minimum future 't' for which 't' > bucket[0] + self.period
                    response_tta = last_bucket[0] + self.period - time.time()

        p_after = 100.0 * self.window_total / self.maxload

        if self.logger.isEnabledFor(logging.DEBUG):
            self._print_range(p_before, p_after, ret)

        self.total_overhead += (time.time() - t)
        return LoadLimiterSubmitResult(ret, retry_in=response_tta)

    def _distribute_penalty(self, amount, distribution_factor):
        qlen = len(self.queue)
        if qlen < 1:
            # no buckets!
            return

        exceeds_cap = (self.window_total + amount) - self.max_cap
        if exceeds_cap > 0:
            amount -= exceeds_cap

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

    def _print_range(self, rmin, rmax, ret):
        p_step = 5
        acc = 0
        name_raw = self.name if self.name is not None else self.__class__
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
        line += '({:1.0f}r {:1.2f}ms/r)'.format(self.num_calls, avg_oh)
        self.logger.debug(line)
