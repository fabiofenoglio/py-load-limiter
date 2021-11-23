from typing import Optional

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
