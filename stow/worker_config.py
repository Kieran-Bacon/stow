import os
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

# class ThreadPoolExecutorWithQueueSizeLimit(concurrent.futures.ThreadPoolExecutor):
#     def __init__(self, maxsize=50, *args, **kwargs):
#         super(ThreadPoolExecutorWithQueueSizeLimit, self).__init__(*args, **kwargs)
#         self._work_queue = queue.Queue(maxsize=maxsize) # type: ignore

class WorkerPoolConfig:

    __WORKER_POOL: Optional[concurrent.futures.ThreadPoolExecutor] = None
    @classmethod
    def _workerPool(cls, max_workers: Optional[int] = None) -> concurrent.futures.ThreadPoolExecutor:
        if cls.__WORKER_POOL is None:
            logger.debug('Initialising stow worker pool executor with %s workers', max_workers)
            cls.__WORKER_POOL = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
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

        self.executing: List[concurrent.futures.Future] = []
        self.futures: List[concurrent.futures.Future] = []

    @property
    def executor(self):
        if self._executor is None:
            self._executor = self._workerPool(self.max_workers)
        return self._executor

    def submit(self, *args, **kwargs):

        future = self.executor.submit(*args, **kwargs)

        # self.executing.append(future)
        self.futures.append(future)

        # while len(self.executing) > 100:
        #     self.executing = [future for future in self.executing if not future.done()]
        #     time.sleep(0.01)

    def extend(self, join: bool = False, shutdown: bool = False) -> "WorkerPoolConfig":
        config = WorkerPoolConfig(self._executor, join=join, shutdown=shutdown)
        config.futures = self.futures
        return config

    def detach(self):
        return WorkerPoolConfig(self._executor, join=True, shutdown=False)

    def join(self):
        """ Join the active tasks - for all enqueued futures iterate through them and wait for them to complete.
        """
        for future in concurrent.futures.as_completed(self.futures):
            future.result()

    def conclude(self, cancel: bool = False):
        """ Trigger as per the config the finalisation of the work - If the join and shutdown is False then do nothing
        else do what ever the config declares. Handle exceptions from the futures and ensure the internal worker pool is
        cleaned up

        Args:
            cancel (bool): Interupt the work, cancelling any unstarted tasks and return early. Will still wait in-progress
                tasks to finish.

        """
        exception = None
        try:
            if not cancel and self._executor is not None and (self.will_shutdown or self.will_join):
                self.join()

        except Exception as e:
            logger.exception('Exception in worker pool - cancelling command')
            exception = e
            cancel = True
            raise

        finally:
            if self._executor is not None and (self.will_shutdown or cancel):
                operation = "cancelling" if cancel else "waiting for"
                logger.debug(f'Shutting down worker pool executor and {operation} remaining tasks')
                if exception:
                    logger.warning('Shutting down command due to exception %s', exception)
                self._executor.shutdown(wait=cancel, cancel_futures=cancel)
                if not self._externalExecutor:
                    self.__class__.__WORKER_POOL = None

import os
import queue
from queue import Queue
import enum
from threading import Thread, Lock, get_ident
import functools
from typing import Optional, Any, Union, Tuple
from typing_extensions import Self
import dataclasses
import weakref

from concurrent.futures import Executor, ThreadPoolExecutor

import logging

logger = logging.getLogger(__name__)

class ThreadPoolStatus(enum.Enum):
    CONFIGURED = enum.auto()
    ACTIVE = enum.auto()
    SHUTTING_DOWN = enum.auto()
    SHUTDOWN = enum.auto()

class TaskStatus(enum.Enum):
    PENDING = enum.auto()
    STARTED = enum.auto()
    COMPLETED = enum.auto()
    FAILED = enum.auto()

@dataclasses.dataclass
class Task:
    id: int
    status: TaskStatus
    method: functools.partial
    result: Any

class ScheduleOption(enum.Enum):
    SUBMIT = enum.auto()
    AFTER_LAST_TASK = enum.auto()

class SchedulableThreadPool:

    def __init__(
            self,
            /,
            task_queue_size: int = 1000,
            scheduler_queue_size: int = 1000,
            worker_count: Optional[int] = os.cpu_count(),
            submit_timeout: Optional[float] = None,
            backend: Optional[Executor] = None,
        ):
        """ Implementation of a thread pool + wrapper for Executors, that gives a way of blocking on input + scheduling
        of tasks

        Params:
            task_queue_size (int): The max number of pending tasks permitted before blocking
            schedule_queue_size (int): The max number of scheduled tasks, before blocking occurs
            worker_count (int): The number of threads that will be spawened
            submit_timeout (Optional[float]): A timeout for the queue submit that will allowing for getting control back
            backend (Optional[Executor]): Backend threading implementation to use if the user wants to do that.
        """

        assert worker_count is not None and worker_count > 0

        self._open_context_id: Optional[int] = None
        self._open_contexts_count: int = 0

        self._last_task_id = 0
        self._input_queue: queue.Queue[Task] = Queue(maxsize=task_queue_size)
        self._schedule_queue: queue.Queue[Tuple[int, Task]] = Queue(maxsize=scheduler_queue_size)
        self._enqueued_task_ids = set()
        self._concluded_task_ids = {0}

        self._backend = backend
        self._worker_count = worker_count if self._backend is None else getattr(backend, '_max_workers', 0)
        self._submit_timeout = submit_timeout
        self._status = ThreadPoolStatus.CONFIGURED

        # Holds the thread state
        self._threads = []
        self._next_schedule_trigger_id: int = 0
        self._next_scheduled_task: Optional[Task] = None

        logger.debug('Initialised new pool with %s workers with total capacity of %s tasks', self._worker_count, task_queue_size + scheduler_queue_size)

    def _progress_scheduled_tasks(self) -> bool:

        while True:

            # Handle the next scheduled task insertion into the active input queue
            if self._next_scheduled_task is not None:

                # Check that the trigger task has been concluded (completed/failed)
                if self._next_schedule_trigger_id in self._concluded_task_ids:
                    self._enqueueTask(self._next_scheduled_task)

                else:
                    # The target hasn't finished - delay handling the scheduled task until the trigger has finished
                    return True

            # Fetch from the schedule queue the next job to work on
            try:
                self._next_schedule_trigger_id, self._next_scheduled_task = self._schedule_queue.get(timeout=1e-2)
            except queue.Empty:
                self._next_scheduled_task = None
                return False

    def _thread_master(self, backend: Executor):

        logger.debug('Starting thread master')

        while True:

            # Read the status and use for the remainder of the logic to ensure that it doesn't change during
            # process
            status = self._status

            if not (status is ThreadPoolStatus.ACTIVE or status is ThreadPoolStatus.SHUTTING_DOWN):
                # We are not in an active state
                return

            # Process the schedule input to ensure it is up to date
            self._progress_scheduled_tasks()

            # Extract the next task to work on - while sure that status of processing has not yet changed
            try:
                task = self._input_queue.get(timeout=1e-2)

            except queue.Empty:
                if status is ThreadPoolStatus.SHUTTING_DOWN and self._next_scheduled_task is None:
                    # No work to do - conclude the thread
                    break
                continue

            def backendHandler(task: Task):

                # Do the work of the task
                try:
                    task.result = task.method()
                    task.status = TaskStatus.COMPLETED

                except BaseException as e:
                    logger.exception('Thread [%s] Experienced error while processing task [%s]', task)
                    task.status = TaskStatus.FAILED
                    task.result = e

                self._concluded_task_ids.add(task.id)
                self._enqueued_task_ids.remove(task.id)

            backend.submit(backendHandler, task)

        logger.debug('Thread master concluded')

    def _thread_worker(self, id_: int):

        # Setup parameters for thread
        is_master_thread = id_ == 0
        logger.debug('Starting thread [%s] - master=%s', id_, is_master_thread)

        while True:
            status = self._status

            if not (status is ThreadPoolStatus.ACTIVE or status is ThreadPoolStatus.SHUTTING_DOWN):
                # We are not in an active state
                break

            # What if there is lots to do and nothing in schedule
            # What if there is nothing to do but lots to schedule then do

            # We should not change to shutdown

            if is_master_thread:
                # Review the schedule queue
                scheduled_progressed = self._progress_scheduled_tasks()
                # if status is ThreadPoolStatus.SHUTTING_DOWN and not scheduled_progressed:
                #     # If we are shutting down and there was no more scheduled tasks to process - there is at most
                #     # one task that needs to be finished (which this thread can do)
                #     # Signal other threads they can now directly conclude
                #     self._status = ThreadPoolStatus.SHUTDOWN
                #     logger.debug('Master thread has identify no new scheduled tasks')

            # Extract the next task to work on
            try:
                task = self._input_queue.get(timeout=1e-2)

            except queue.Empty:
                if status is ThreadPoolStatus.SHUTTING_DOWN and self._next_scheduled_task is None:
                    # No next task to work on and no scheduled task to add to workload - end
                    break
                continue

            # Do the work of the task
            try:
                task.result = task.method()
                task.status = TaskStatus.COMPLETED

            except BaseException as e:
                logger.exception('Thread [%s] Experienced error while processing task [%s]', id_, task)
                task.status = TaskStatus.FAILED
                task.result = e

            self._concluded_task_ids.add(task.id)
            self._enqueued_task_ids.remove(task.id)

        logger.debug('Thread [%s] concluded', id_)

    @staticmethod
    def _resetQueue(queue_: queue.Queue):
        """ Empty a queue """
        while True:
            try:
                queue_.get_nowait()
            except queue.Empty:
                return

    def __enter__(self) -> Self:

        if self._open_context_id is None:
            self._open_context_id = get_ident()
            self._open_contexts_count += 1
            self.start()

        elif self._open_context_id == get_ident():
            self._open_contexts_count += 1

        return self

    def __exit__(self, ex, ed, ad):

        if get_ident() == self._open_context_id:
            self._open_contexts_count -= 1
            if not self._open_contexts_count:
                self.stop()
                self._open_context_id = None

    def _createTask(self, func, *args, **kwargs):

        self._last_task_id += 1

        return Task(
            id=self._last_task_id,
            status=TaskStatus.PENDING,
            method=functools.partial(func, *args, **kwargs),
            result=None
        )

    def _enqueueTask(self, task: Task):

        self._input_queue.put(task, timeout=self._submit_timeout)
        self._enqueued_task_ids.add(task.id)

    @property
    def last_task_id(self) -> int:
        return self._last_task_id

    def start(self):

        logger.debug('Pool id=[%s] starting - workers=%s backend=%s', id(self), self._worker_count, self._backend)

        # Ensure that the pool isn't already performing some processing
        assert self._status is ThreadPoolStatus.CONFIGURED
        self._status = ThreadPoolStatus.ACTIVE

        if self._backend is not None:
            # Start the master thread to schedule tasks into the backend

            master_thread = Thread(target=self._thread_master, args=(self._backend,))
            master_thread.start()
            self._threads.append(master_thread)

        else:
            # Create the thread pool threads

            for i in range(self._worker_count):

                t = Thread(target=self._thread_worker, args=(i,))
                t.start()
                self._threads.append(t)

    def wait(self, poll_frequency: float = 1e-2, task: Union[Task, int, None] = None):
        """ Wait for the tasks in the pool to complete before moving on """

        if task is None:
            # Wait for all active tasks to complete

            # Once there are no enqueued tasks - all tasks must have concluded and been removed
            while (self._enqueued_task_ids):
                time.sleep(poll_frequency)

        else:
            # Wait for a specific task to finish

            # Assert that the task id is valid
            task_id = task.id if isinstance(task, Task) else task
            assert task_id in self._enqueued_task_ids or task_id in self._concluded_task_ids

            # Wait for the task to appear in the concluded list
            while task_id not in self._concluded_task_ids:
                time.sleep(poll_frequency)


    def stop(self, send_to_background: bool = False, cancel: bool = False):
        """ Stop the pool from further processing - does not close down the backend provded.

        Args:
            send_to_background (bool, optional): Tasks run in the background, allowing this call to return immediately. Defaults to False.
            cancel (bool, optional): Pending tasks are canceled and will not be run, currently running tasks will finish. Defaults to False.
        """
        logger.debug('Pool id=[%s] closing - background_task=%s cancel=%s', id(self), send_to_background, cancel)


        try:

            if cancel:
                # Cancel the tasks that are currently running and shutdown

                self._status = ThreadPoolStatus.SHUTDOWN  # Stop the threads from processing new jobs

                if send_to_background:
                    return  # Return immediately, don't wait for the threads to conclude

                else:
                    # Wait for the threads to join

                    for t in self._threads:
                        t.join()

            elif send_to_background:
                # We are not cancelling tasks left in the pipeline but we are not waiting for them to finish

                self._status = ThreadPoolStatus.SHUTTING_DOWN

            else:
                # We will wait for all enqueued tasks to be completed
                self.wait()

                self._status = ThreadPoolStatus.SHUTDOWN

                for t in self._threads:
                    t.join()

        finally:
            self._threads = []

    def reset(self):

        assert self._status is ThreadPoolStatus.SHUTDOWN
        self._status = ThreadPoolStatus.CONFIGURED

        self._last_task_id = 0
        self._resetQueue(self._input_queue)
        self._resetQueue(self._schedule_queue)
        self._enqueued_task_ids = set()
        self._concluded_task_ids = {0}
        self._next_schedule_trigger_id = 0
        self._next_scheduled_task = None


    def submit(
            self,
            func: Callable[_P, _T],
            *args: _P.args,
            **kwargs: _P.kwargs
        ) -> Task:
        """ Submit the task into the working queue """

        task = self._createTask(func, *args, **kwargs)
        self._enqueueTask(task)
        return task

    def schedule(
            self,
            trigger_task: Union[None, ScheduleOption, int, Task],
            func: Callable[_P, _T],
            *args: _P.args,
            **kwargs: _P.kwargs
        ):
        """ Schedule a task to be added to the task queue when another task is concluded

        e.g.
            `pool.schedule(None, print, 'Hello')` defaults to submit
            `pool.schedule(ScheduleOption.SUBMIT, print, 'Hello')` defaults to submit
            `pool.schedule(ScheduleOption.AFTER_LAST_TASK, print, 'Hello')` Adds the task after the current known last task
            `pool.schedule(pool.last_task_id, print, 'after')` run task after all tasks have been concluded
            `pool.schedule(pool.submit(print, 'first'), print, 'second')` Run after first task has been complete

        Note: You can schedule the scheduling of a method:
            `pool.schedule(task, pool.schedule, ScheduleOption.AFTER_LAST_TASK, print, 'runs last')`
        Useful if the first task will be creating and scheduling tasks, and you need to have this second task run after.
        Can be nested any number of times

        Args:
            trigger_task (None | ScheduleOption | int | Task): Identification of the task that must conclude before this
              new task is eligable for processing.
            func (callable): The task method
            args: The task method arguments
            kwargs: The task keyword arguments
        """

        # Resolve the trigger target task
        if trigger_task is None or trigger_task is ScheduleOption.SUBMIT:
            return self.submit(func, *args, **kwargs)
        elif trigger_task is ScheduleOption.AFTER_LAST_TASK:
            trigger_task_id = self._last_task_id
        elif isinstance(trigger_task, Task):
            trigger_task_id = trigger_task.id
        else:
            trigger_task_id = trigger_task

        # Create the scheduled task
        task = self._createTask(func, *args, **kwargs)

        # Add the new scheduled task into the appriopriate queue
        if trigger_task_id in self._enqueued_task_ids:
            # The task has yet to be completed - we need to delay this task
            self._schedule_queue.put((trigger_task_id, task))
            self._enqueued_task_ids.add(task.id)

        elif trigger_task_id in self._concluded_task_ids:
            # The trigger task has already been concluded - we can enqueue it without delay for it to be worked on
            self._enqueueTask(task)

        else:
            raise ValueError(f'Attempted to schedule task for an invalid task id - task {task.id} can be scheduled for unknown task {trigger_task_id}')

        return task