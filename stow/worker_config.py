import functools
import concurrent.futures
import dataclasses
from typing import (List, Optional, Callable, TypeVar)
from typing_extensions import Self, ParamSpec
import time
import queue

import logging
logger = logging.getLogger(__name__)

_P = ParamSpec('_P')
_T = TypeVar('_T')

class SequencialExecutor(concurrent.futures.Executor):

    def submit(self, __fn: Callable[_P, _T], *args: _P.args, **kwargs: _P.kwargs) -> concurrent.futures.Future[_T]:

        future = concurrent.futures.Future()
        future.set_result(__fn(*args, **kwargs))
        return future

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

        self._externalExecutor = True
        self.will_shutdown = shutdown
        if executor is None:
            if max_workers == 0:
                self._executor = SequencialExecutor()
            else:
                self._executor = None
                self._externalExecutor = False
                self.will_shutdown &= not self.__WORKER_POOL  # You will only be able to shutdown if a pool hasn't already been created

        else:
            self._executor = executor

        self.max_workers = max_workers

        self.will_join = join

        self.futures: List[concurrent.futures.Future] = []

    @property
    def executor(self):
        if self._executor is None:
            self._executor = self._workerPool(self.max_workers)
        return self._executor

    def submit(self, *args, **kwargs):
        self.futures.append(
            self.executor.submit(*args, **kwargs)
        )

    def extend(self, join: bool = False, shutdown: bool = False) -> "WorkerPoolConfig":
        config = WorkerPoolConfig(self._executor, join=join, shutdown=shutdown)
        config.futures = self.futures
        return config

    def detach(self):
        return WorkerPoolConfig(self._executor, join=True, shutdown=False)

    def join(self):
        for future in concurrent.futures.as_completed(self.futures):
            future.result()

    def conclude(self, cancel: bool = False):
        try:
            if not cancel and self._executor is not None and (self.will_shutdown or self.will_join):
                self.join()

        except Exception:
            cancel = True
            raise

        finally:
            if self._executor is not None and (self.will_shutdown or cancel):
                self._executor.shutdown(wait=cancel, cancel_futures=cancel)
                if not self._externalExecutor:
                    self.__class__.__WORKER_POOL = None