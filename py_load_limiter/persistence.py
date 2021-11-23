from typing import List, Optional
from abc import abstractmethod
import os, json

class LoadLimiterSerializedStatus:
    def __init__(
        self,  # NOSONAR - constructor
        name: str = None,
        maxload: float = None,
        period: int = None,
        num_max_buckets: int = None,
        max_cap: float = None,
        compute_tta: bool = None,
        overstep_penalty: int = None,
        step_period: int = None,
        penalty_distribution_factor: float = None,
        request_overhead_penalty_factor: float = None,
        request_overhead_penalty_distribution_factor: float = None,
        window_total: float = None,
        num_calls: int = None,
        total_overhead: float = None,
        was_over: bool = None,
        window: List[List[float]] = None
    ):
        self.name = name
        self.maxload = maxload
        self.period = period
        self.num_max_buckets = num_max_buckets
        self.max_cap = max_cap
        self.compute_tta = compute_tta
        self.overstep_penalty = overstep_penalty
        self.step_period = step_period
        self.penalty_distribution_factor = penalty_distribution_factor
        self.request_overhead_penalty_factor = request_overhead_penalty_factor
        self.request_overhead_penalty_distribution_factor = request_overhead_penalty_distribution_factor
        self.window_total = window_total
        self.num_calls = num_calls
        self.total_overhead = total_overhead
        self.was_over = was_over
        self.window = [e for e in window] if window else []


class LoadLimiterStorageAdapter():

    @abstractmethod
    def save(self, status: LoadLimiterSerializedStatus):
        pass

    @abstractmethod
    def read(self) -> Optional[LoadLimiterSerializedStatus]:
        pass


class InMemoryLoadLimiterStorageAdapter(LoadLimiterStorageAdapter):
    def __init__(self):
        self.stored: Optional[LoadLimiterSerializedStatus] = None
        super().__init__()

    def save(self, status: LoadLimiterSerializedStatus):
        self.stored = status

    def read(self) -> Optional[LoadLimiterSerializedStatus]:
        return self.stored


class FileSystemLoadLimiterStorageAdapter(LoadLimiterStorageAdapter):
    def __init__(self, file_location: Optional[str]=None):
        self.encoding = 'utf-8'
        self.file_location = file_location if file_location is not None else './.load-limiter.json'
        super().__init__()

    def save(self, status: LoadLimiterSerializedStatus):
        serialized_json = json.dumps(status.__dict__)
        f = open(self.file_location, "w", encoding=self.encoding)
        f.write(serialized_json)
        f.close()

    def read(self) -> Optional[LoadLimiterSerializedStatus]:
        if not os.path.exists(self.file_location):
            return None

        raw_data = None
        with open(self.file_location, encoding=self.encoding) as f:
            raw_data = json.load(f)

        instance = LoadLimiterSerializedStatus()
        for k, v in raw_data.items():
            setattr(instance, k, v)
        return instance
