import functools
import concurrent.futures
import dataclasses
from typing import (List, Optional)
from typing_extensions import Self
import time
import queue

import logging
logger = logging.getLogger(__name__)

class WorkerPoolConfig:

    __WORKER_POOL: Optional[concurrent.futures.ThreadPoolExecutor] = None
    @classmethod
    def _workerPool(cls, max_workers: Optional[int] = None) -> concurrent.futures.ThreadPoolExecutor:
        if cls.__WORKER_POOL is None:
            cls.__WORKER_POOL = concurrent.futures.ThreadPoolExecutor(max_workers)
        return cls.__WORKER_POOL

    def __init__(
        self,
        executor: Optional[concurrent.futures.Executor] = None,
        join: bool = True,
        shutdown: bool = False,
        max_workers: Optional[int] = None
    ):

        if executor is None:
            self._executor = None
            self._externalExecutor = False

        else:
            self._executor = executor
            self._externalExecutor = True

        self.max_workers = max_workers

        self.will_join = join
        self.will_shutdown = shutdown

        self._futures: List[concurrent.futures.Future] = []

    @property
    def executor(self):
        if self._executor is None:
            self._executor = self._workerPool(self.max_workers)
        return self._executor

    def submit(self, *args, **kwargs):
        self._futures.append(
            self.executor.submit(*args, **kwargs)
        )

    def extend(self, join: bool = False, shutdown: bool = False) -> "WorkerPoolConfig":
        config = WorkerPoolConfig(self._executor, join=join, shutdown=shutdown)
        config._futures = self._futures
        return config

    def join(self):
        for future in concurrent.futures.as_completed(self._futures):
            future.result()

    def conclude(self, cancel: bool = False):
        try:
            if not cancel and self._executor is not None and (self.will_shutdown or self.will_join):
                self.join()

        except Exception:
            cancel = True
            raise

        finally:
            if self.will_shutdown or cancel:
                self.executor.shutdown(wait=cancel, cancel_futures=cancel)
                if not self._externalExecutor:
                    self.__class__.__WORKER_POOL = None