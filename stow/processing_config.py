from typing import Optional, Self
from schedulable import SchedulableThreadPool, Task, TaskStatus
from concurrent.futures import Executor
from functools import lru_cache, wraps, partial

class ProcessingConfig:
    """ This object describes the way processing will happen in a way that can be passed through the interface of store
    and effect the consequential actions.

    Here it should be possible to specify whether the operations are going to be handled using threading
    You should be able to specify if the processing is to be finish before returning to the initial call
    You should be able to specify that objects should not be GCed as they are expected to be used again.

    Two modes - threading or sequential

    - When sequential operation:
    Tasks are completed as they are submitted or scheduled

    - When threading
    Tasks are passed onto the ScheduleThreadPool

    """

    @lru_cache
    @staticmethod
    def _thread_pool(max_counts: int, thread_count: int, executor: Optional[Executor] = None) -> SchedulableThreadPool:
        return SchedulableThreadPool(
            worker_count=thread_count,
            task_queue_size=max_counts,
            scheduler_queue_size=max_counts,
            backend=executor
        )

    def __init__(
            self,
            threading_disabled: bool = False,
            max_tasks: int = 1000,
            executor: Optional[Executor] = None,
            thread_count: int = 4,
            join: bool = True,
            shutdown: bool = True
        ):

        self._threading_disabled = threading_disabled
        self._max_tasks = max_tasks
        self._executor = executor
        self._thread_count = thread_count
        self._join = join
        self._shutdown = shutdown

        self._id = 0
        self._pool: Optional[SchedulableThreadPool] = None

        if self._threading_disabled:
            #  Threading has been disabled so simply do the tasks and activities as asked
            self.submit = self._sequential_submit
            self.schedule = self._sequential_schedule

        else:

    def __enter__(self) -> Self:
        return self

    def __exit__(self, a, b, c):
        pass

    def _sequential_submit(self, func, *args, **kwargs) -> Task:

        self._id += 1
        method = partial(func, *args, **kwargs)
        try:
            return Task(
                id=self._id,
                status=TaskStatus.COMPLETED,
                method=method,
                result=method()
            )

        except Exception as e:
            return Task(
                id=self._id,
                status=TaskStatus.FAILED,
                method=method,
                result=e
            )

    def _sequential_schedule(self, _, *args, **kwargs) -> Task:
        return self._sequential_submit(*args, **kwargs)


    @wraps(SchedulableThreadPool.submit)
    def submit(self, *args, **kwargs):

        # Elevate

        self._pool = self._thread_pool(self._max_tasks, self._thread_count, self._executor)
        self.submit = self._pool.submit
        self.schedule = self._pool.schedule
        self._pool.submit(*args, **kwargs)

    @wraps(SchedulableThreadPool.schedule)
    def schedule(self):
        self._pool = self._thread_pool(self._max_tasks, self._thread_count, self._executor)
        self.submit = self._pool.submit
        self.schedule = self._pool.schedule
        self._pool.submit(*args, **kwargs)

    def stop(self):

        if not self._threading_disabled and self._pool is not None:
            # We used a schedulable thread pool

            self._pool.stop()



