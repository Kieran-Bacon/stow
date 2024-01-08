import functools
import concurrent.futures
import dataclasses
from typing import (List)

@dataclasses.dataclass
class WorkerPoolConfig:
    executor: concurrent.futures.Executor
    join: bool = True
    shutdown: bool = False
    futures: List[concurrent.futures.Future] = dataclasses.field(default_factory=list)

    @functools.wraps(concurrent.futures.Executor.submit)
    def submit(self, *args, **kwargs):
        self.futures.append(
            self.executor.submit(*args, **kwargs)
        )