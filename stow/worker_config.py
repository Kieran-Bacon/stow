import functools
import concurrent.futures
import dataclasses
from typing import (List, Optional)
from typing_extensions import Self
import time

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

        self._parent: Optional[WorkerPoolConfig] = None
        self._children: List[WorkerPoolConfig] = []
        self._futures: List[concurrent.futures.Future] = []
        self._concluded = False
        self._stopped = False

    @property
    def executor(self):
        if self._executor is None:
            self._executor = self._workerPool(self.max_workers)
        return self._executor

    @property
    def futures(self) -> List[concurrent.futures.Future]:
        return self._futures

    def _enqueue(self, future: concurrent.futures.Future):
        self._futures.append(future)
        if self._parent is not None:
            self._parent._enqueue(future)

    def submit(self, *args, **kwargs):
        if self._stopped:
            raise RuntimeError('Executor stopped')

        self._enqueue(
            self.executor.submit(*args, **kwargs)
        )
        self._concluded = False

    def extend(self, join: bool = False, shutdown: bool = False) -> "WorkerPoolConfig":
        """Create a new worker pool config with different behaviour, that uses the same executor and futures store as
        the config extended

        e.g. Sync will use create new configs for the put and copy commands it issues as it will not want any to wait
        will manage the join in the first sync call

        Args:
            join (bool, optional): Whether the method should wait for their futures to complete. Defaults to False.
            shutdown (bool, optional): Whether the executor should be shutdown once finished. Defaults to False.

        Returns:
            WorkerPoolConfig: _description_
        """

        extended = WorkerPoolConfig(
            executor=self.executor if self._externalExecutor else None,
            join=join,
            shutdown=shutdown
        )
        extended._parent = self
        self._children.append(extended)

        return extended

    def join(self):

        for future in concurrent.futures.as_completed(self.futures):
            try:
                future.result()
            except:
                logger.exception('Worker experienced unhandled exception')
                self.forceStop()
                raise


    def conclude(self):
        try:
            if self._executor is not None and (self.will_shutdown or self.will_join):
                for child in self._children:
                    while not child._concluded:
                        time.sleep(0.0001)

                self.join()

                if self.will_shutdown:
                    self.executor.shutdown()
                    if not self._externalExecutor:
                        self.__class__.__WORKER_POOL = None
        except:
            self.forceStop()

        finally:
            self._concluded = True

    def forceStop(self):

        if self._stopped:
            return

        # Prevent any additional tasks from being added via this config
        self._stopped = True

        # Trigger children to do the same
        for child in self._children:
            child.forceStop()

        if self._parent is None:
            # It will start in the main thread which will be the parent
            self.executor.shutdown(wait=False, cancel_futures=True)
        else:
            self._parent.forceStop()

# import functools
# def isWorkerOptimised(function):

#     @functools.wraps(function)
#     def wrapper(*args, worker_config: Optional[WorkerPoolConfig], **kwargs):

#         if worker_config is None:
#             worker_config = WorkerPoolConfig(shutdown=True)

#         try:
#             return function(*args, worker_config=worker_config, **kwargs)
#         finally:
#             worker_config.conclude()

#     return wrapper
