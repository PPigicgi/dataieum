"""Bounded FIFO dependency work with exact in-flight sharing and owned cleanup."""
import asyncio
from collections import Counter
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
import json
import math
import time

from .errors import CapacityExceeded, HarnessError
from .resources import bounded_json


class DependencyBusy(CapacityExceeded):
    code = 'dependency_busy'


class DependencyTimeout(HarnessError):
    code = 'dependency_timeout'


@dataclass
class _Job:
    task: asyncio.Task
    waiters: int = 0
    abandoning: bool = False


@dataclass
class _ScheduledJob:
    started: asyncio.Future
    task: asyncio.Task | None = None
    waiters: int = 0
    abandoning: bool = False
    started_at: float | None = None
    finished_at: float | None = None


class _Ticket:
    def __init__(self, job, queue_seconds, hard_deadline):
        self.job = job
        self.joined_at = time.monotonic()
        self.queue_deadline = self.joined_at + queue_seconds
        self.hard_deadline = hard_deadline

    @property
    def wait_seconds(self):
        until = self.job.started_at or self.job.finished_at or time.monotonic()
        return max(0., until - self.joined_at)

    @property
    def execution_seconds(self):
        if self.job.started_at is None:
            return 0.
        return max(0., (self.job.finished_at or time.monotonic()) - self.job.started_at)

    async def started(self):
        if self.job.started.done():
            return self.job.started.result()
        deadline = min(self.queue_deadline, self.hard_deadline)
        scope = asyncio.timeout(max(0., deadline - time.monotonic()))
        try:
            async with scope:
                return await asyncio.shield(self.job.started)
        except TimeoutError:
            if not scope.expired():
                raise
            raise DependencyBusy('Dependency caller queue wait expired') from None

    async def result(self):
        await self.started()
        try:
            async with asyncio.timeout(max(0., self.hard_deadline - time.monotonic())):
                return json.loads(await asyncio.shield(self.job.task))
        except TimeoutError:
            raise DependencyTimeout('Dependency caller deadline exceeded') from None


class DependencyStage:
    def __init__(self, *, workers, queued, wait_seconds=3., operation_seconds=7., max_waiters=25,
                 prepare=None):
        if (type(workers) is not int or not 1 <= workers <= 100 or
                type(queued) is not int or not 0 <= queued <= 1024 or workers+queued > 1025 or
                type(wait_seconds) not in (int, float) or not math.isfinite(wait_seconds) or
                not 0 < wait_seconds <= 7200 or
                type(operation_seconds) not in (int, float) or not math.isfinite(operation_seconds) or
                not 0 < operation_seconds <= 60 or
                type(max_waiters) is not int or not 1 <= max_waiters <= 1025 or
                (prepare is not None and not callable(prepare))):
            raise ValueError('invalid dependency bounds')
        self.workers, self.queue_limit, self.wait_seconds = workers, queued, wait_seconds
        self.operation_seconds, self.max_waiters = operation_seconds, max_waiters
        # Scheduled jobs own prepared resources, including during cancellation.
        # Capacity preparation belongs to queue time, before the execution clock.
        self.prepare = prepare
        self.slots = asyncio.Semaphore(workers)
        self.jobs = {}
        self.active = self.queued = self.waiters = 0
        self.peak_active = self.peak_queued = 0
        self.stats = Counter()
        self.closed = False

    def status(self):
        return {**self.stats, 'active':self.active, 'queued':self.queued, 'waiters':self.waiters,
                'jobs':len(self.jobs), 'workers':self.workers, 'queue_limit':self.queue_limit,
                'peak_active':self.peak_active, 'peak_queued':self.peak_queued,
                'max_wait_seconds':self.wait_seconds, 'operation_seconds':self.operation_seconds,
                'max_waiters':self.max_waiters}

    @staticmethod
    def _consume(task):
        if not task.cancelled():
            task.exception()

    @staticmethod
    def _finish_scheduled(job):
        # A task can be cancelled before its coroutine first runs, so its
        # coroutine's finally block is not sufficient to wake start waiters.
        if job.finished_at is None:
            job.finished_at = time.monotonic()
        if not job.started.done():
            if job.task.cancelled():
                job.started.cancel()
            else:
                error = job.task.exception()
                job.started.set_exception(error or DependencyTimeout('Dependency did not start'))
        if not job.task.cancelled():
            job.task.exception()

    async def _execute_scheduled(self, job, operation, operation_seconds):
        acquired = False
        resources = AsyncExitStack()
        waiting = time.monotonic()
        queued = self.slots.locked() or self.prepare is not None
        if queued:
            self.queued += 1
            self.peak_queued = max(self.peak_queued, self.queued)
        try:
            queue_scope = asyncio.timeout(self.wait_seconds)
            try:
                async with queue_scope:
                    await self.slots.acquire()
                    acquired = True
                    if self.prepare is not None:
                        await resources.enter_async_context(self.prepare())
            except TimeoutError:
                if not queue_scope.expired():
                    self.stats['preparation_timeout'] += 1
                    raise
                self.stats['queue_expired'] += 1
                raise DependencyBusy('Dependency queue wait expired') from None
            finally:
                if queued:
                    self.queued -= 1
                self.stats['queue_wait_ms'] += round((time.monotonic()-waiting)*1000)
            self.active += 1
            self.peak_active = max(self.peak_active, self.active)
            self.stats['executed'] += 1
            job.started_at = time.monotonic()
            job.started.set_result(job.started_at)
            deadline = job.started_at + operation_seconds
            scope = asyncio.timeout(operation_seconds)
            try:
                async with scope:
                    result = await operation(deadline)
                if scope.expired() or time.monotonic() >= deadline:
                    raise DependencyTimeout('Dependency execution deadline exceeded')
                encoded = bounded_json(result, 524288)
                self.stats['completed'] += 1
                return encoded
            except TimeoutError:
                if scope.expired():
                    self.stats['execution_timeout'] += 1
                    raise DependencyTimeout('Dependency execution deadline exceeded') from None
                raise
            except Exception:
                self.stats['failed'] += 1
                raise
            finally:
                self.active -= 1
        except BaseException as error:
            if not job.started.done():
                if isinstance(error, asyncio.CancelledError):
                    job.started.cancel()
                else:
                    job.started.set_exception(error)
            raise
        finally:
            try:
                await resources.aclose()
            finally:
                job.finished_at = time.monotonic()
                if acquired:
                    self.slots.release()

    async def _detach(self, key, job):
        self.waiters -= 1
        job.waiters -= 1
        if not job.waiters:
            if not job.task.done():
                job.abandoning = True
                self.stats['cancelled_without_waiters'] += 1
                job.task.cancel()
                drain = asyncio.gather(job.task, return_exceptions=True)
                while not drain.done():
                    try:
                        await asyncio.shield(drain)
                    except asyncio.CancelledError:
                        pass
            if self.jobs.get(key) is job:
                self.jobs.pop(key)

    @asynccontextmanager
    async def ticket(self, key, operation, *, operation_seconds=None, queue_seconds=None,
                     hard_deadline=None):
        seconds = self.operation_seconds if operation_seconds is None else operation_seconds
        waiting = self.wait_seconds if queue_seconds is None else queue_seconds
        if (type(seconds) not in (int, float) or not math.isfinite(seconds)
                or not 0 < seconds <= self.operation_seconds):
            raise ValueError('invalid scheduled operation budget')
        if (type(waiting) not in (int, float) or not math.isfinite(waiting)
                or not 0 < waiting <= self.wait_seconds):
            raise ValueError('invalid scheduled queue budget')
        hard_deadline = time.monotonic()+waiting+seconds if hard_deadline is None else hard_deadline
        if type(hard_deadline) not in (int, float) or not math.isfinite(hard_deadline):
            raise ValueError('invalid scheduled hard deadline')
        if hard_deadline <= time.monotonic():
            raise DependencyTimeout('No remaining caller budget')
        # Legacy run() deadlines include waiting; these jobs must never share.
        identity = ('scheduled', key, seconds)
        if self.closed or self.waiters >= self.max_waiters:
            self.stats['rejected'] += 1
            raise DependencyBusy('Dependency admission is full')
        job = self.jobs.get(identity)
        if job is not None and job.abandoning:
            self.stats['rejected'] += 1
            raise DependencyBusy('Dependency cancellation is still draining')
        if job is None:
            if len(self.jobs) >= self.workers+self.queue_limit:
                self.stats['rejected'] += 1
                raise DependencyBusy('Dependency queue is full')
            started = asyncio.get_running_loop().create_future()
            started.add_done_callback(self._consume)
            job = _ScheduledJob(started)
            job.task = asyncio.create_task(self._execute_scheduled(job, operation, seconds))
            job.task.add_done_callback(lambda _: self._finish_scheduled(job))
            self.jobs[identity] = job
        else:
            self.stats['coalesced'] += 1
        self.waiters += 1
        job.waiters += 1
        self.stats['calls'] += 1
        try:
            yield _Ticket(job, waiting, hard_deadline)
        finally:
            await self._detach(identity, job)

    async def _execute(self, operation, deadline, queue_seconds):
        acquired = False
        waiting = time.monotonic()
        queued = self.slots.locked()
        if queued:
            self.queued += 1
            self.peak_queued = max(self.peak_queued, self.queued)
        try:
            try:
                async with asyncio.timeout(min(queue_seconds, max(0, deadline-time.monotonic()))):
                    await self.slots.acquire()
                acquired = True
            except TimeoutError:
                self.stats['queue_expired'] += 1
                raise DependencyBusy('Dependency queue wait expired') from None
            finally:
                if queued:self.queued -= 1
                self.stats['queue_wait_ms'] += round((time.monotonic()-waiting)*1000)
            self.active += 1
            self.peak_active = max(self.peak_active, self.active)
            self.stats['executed'] += 1
            try:
                async with asyncio.timeout(max(0, deadline-time.monotonic())):
                    result = await operation(deadline)
                # Give each waiter its own detached result, never a mutable shared object.
                encoded = bounded_json(result, 524288)
                self.stats['completed'] += 1
                return encoded
            except TimeoutError:
                self.stats['execution_timeout'] += 1
                raise DependencyTimeout('Dependency execution deadline exceeded') from None
            except Exception:
                self.stats['failed'] += 1
                raise
            finally:
                self.active -= 1
        finally:
            if acquired:self.slots.release()

    async def run(self, key, operation, *, budget_seconds=None, queue_seconds=None):
        budget = self.operation_seconds if budget_seconds is None else min(self.operation_seconds, budget_seconds)
        queue_seconds=self.wait_seconds if queue_seconds is None else queue_seconds
        if not 0<queue_seconds<=self.wait_seconds:raise ValueError('invalid queue wait')
        if budget <= 0:raise DependencyTimeout('No remaining dependency budget')
        if self.closed or self.waiters >= self.max_waiters:
            self.stats['rejected'] += 1
            raise DependencyBusy('Dependency admission is full')
        job = self.jobs.get(key)
        if job is not None and job.abandoning:
            self.stats['rejected'] += 1
            raise DependencyBusy('Dependency cancellation is still draining')
        if job is None:
            if len(self.jobs) >= self.workers+self.queue_limit:
                self.stats['rejected'] += 1
                raise DependencyBusy('Dependency queue is full')
            task = asyncio.create_task(self._execute(operation, time.monotonic()+budget,queue_seconds))
            task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)
            job = self.jobs[key] = _Job(task)
        else:self.stats['coalesced'] += 1
        self.waiters += 1
        job.waiters += 1
        self.stats['calls'] += 1
        try:
            async with asyncio.timeout(budget):
                return json.loads(await asyncio.shield(job.task))
        except TimeoutError:
            self.stats['waiter_timeout'] += 1
            raise DependencyTimeout('Dependency caller deadline exceeded') from None
        finally:
            self.waiters -= 1
            job.waiters -= 1
            if job.waiters == 0:
                if job.task.done():
                    # Cleanup and slot release already completed in _execute.
                    # Remove without yielding so a new identical request cannot
                    # observe a false "cancellation is still draining" state.
                    if self.jobs.get(key) is job:self.jobs.pop(key)
                else:
                    job.abandoning = True
                    self.stats['cancelled_without_waiters'] += 1
                    job.task.cancel()
                    drain = asyncio.gather(job.task, return_exceptions=True)
                    while not drain.done():
                        try:await asyncio.shield(drain)
                        except asyncio.CancelledError:pass
                    if self.jobs.get(key) is job:self.jobs.pop(key)

    async def aclose(self):
        self.closed = True
        tasks = [job.task for job in self.jobs.values()]
        for task in tasks:task.cancel()
        if tasks:await asyncio.gather(*tasks,return_exceptions=True)
