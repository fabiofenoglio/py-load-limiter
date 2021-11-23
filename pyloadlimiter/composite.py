import threading
import logging
from typing import List, Optional

from .types import LoadLimiterSubmitResult
from .load_limiter import LoadLimiter


class CompositeLoadLimiter(LoadLimiter):
    def __init__(self,
        name: str = None,
        limiters: List[LoadLimiter] = None,
        logger: logging.Logger = None
    ):
        if limiters is None or len(limiters) < 1:
            raise ValueError('At least one limiter is required for composition')

        self.name = name
        self.limiters = limiters
        self.logger = logger if logger is not None else logging.getLogger("loadlimiter")

        self.lock = threading.Lock()

        widest_limiter: Optional[LoadLimiter] = None
        for candidate in limiters:
            if widest_limiter is None or candidate.period > widest_limiter.period:
                widest_limiter = candidate
        self.widest_limiter = widest_limiter

    def __getattr__(self, name):
        if name == 'maxload':
            return self.widest_limiter.maxload
        elif name == 'period':
            return self.widest_limiter.period
        elif name == 'window_total':
            return self.widest_limiter.window_total
        elif name == 'overstep_penalty':
            return self.widest_limiter.overstep_penalty
        elif name == 'request_overhead_penalty_factor':
            return self.widest_limiter.request_overhead_penalty_factor

    def submit(self, load: float = 1) -> LoadLimiterSubmitResult:
        all_accepted = True
        highest_wait_time = None
        rejections: List[LoadLimiterSubmitResult] = []
        probes_accepted: List[LoadLimiter] = []
        probes_rejected: List[LoadLimiter] = []
        with self.lock:
            
            for limiter in self.limiters:
                probe_result = limiter._submit_probe(load = load)
                if probe_result:
                    probes_accepted.append(limiter)
                else:
                    all_accepted = False
                    probes_rejected.append(limiter)
                    rejection_result = limiter._submit_reject(load = load)
                    rejections.append(rejection_result)
                    if rejection_result.retry_in is not None and (highest_wait_time is None or rejection_result.retry_in > highest_wait_time):
                        highest_wait_time = rejection_result.retry_in

            if all_accepted:
                # if all accepted, confirm
                for limiter in probes_accepted:
                    limiter._submit_accept(load = load)
            else:
                # if at least one rejected, do not confirm.
                # no need to apply rejection as that is done in the previous cycle, on the spot
                pass

        return LoadLimiterSubmitResult(all_accepted, retry_in=highest_wait_time)

    def instant_load_factor(self) -> float:
        factors = []
        with self.lock:
            for limiter in self.limiters:
                factors.append(limiter._instant_load_factor())
        return max(factors)
     
    def distribute(self, amount):
        with self.lock:
            for limiter in self.limiters:
                limiter.distribute(amount)
