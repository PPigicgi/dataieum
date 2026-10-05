"""Request-scoped accounting and cooperative async execution."""
import asyncio
import inspect
import json
import time
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TypeVar

from .errors import (AdapterContractError, BudgetExceeded, CapacityExceeded, CleanupFailed,
                     DeadlineExceeded, PolicyError, RetryableError, SessionClosed)
from .policy import Policy
from .resources import Memory, SelectiveCache, Workspace, _text_size, bounded_json

T = TypeVar('T')


class EscalationReason(str, Enum):
    AMBIGUOUS_CONCEPT = 'ambiguous_concept'
    CONFLICTING_CANDIDATES = 'conflicting_candidates'
    INVALID_RESPONSE = 'invalid_response'


@dataclass(frozen=True)
class LLMRequest:
    model: str
    prompt: str
    max_output_tokens: int
    store: bool = False


@dataclass(frozen=True)
class LLMResponse:
    value: object
    input_tokens: int
    output_tokens: int


@dataclass(frozen=True)
class Usage:
    turns: int = 0
    llm_calls: int = 0
    tool_calls: int = 0
    retries: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    nodes: int = 0
    edges: int = 0
    datasets: int = 0
    neighbor_items: int = 0
    stream_items: int = 0


class Session:
    """Use only inside Harness.run on its event loop. Adapters must cooperate."""
    def __init__(self, policy: Policy, root: Path, capacity=None, on_cleanup_failure=None):
        self._policy = policy
        self._capacity = capacity
        self._on_cleanup_failure = on_cleanup_failure
        self._loop = asyncio.get_running_loop()
        self._owner = asyncio.current_task()
        self._deadline = time.monotonic() + min(policy.agent.timeout_seconds + policy.agent.max_queue_seconds,
                                               policy.workspace.ttl_seconds)
        self._execution_spent = self._queue_spent = 0.
        self._scheduled_tickets = set()
        self._execution_started = {}
        self._queue_started = {}
        self._counts = {name: 0 for name in Usage.__dataclass_fields__}
        self._closed = False
        self._operations = set()
        self._base_attempted = False
        self.memory = Memory(policy.memory)
        self.workspace = Workspace(root, policy.workspace,
                                   on_cleanup_failure=self._mark_cleanup_failed)

    @property
    def policy(self) -> Policy:
        return self._policy

    @property
    def usage(self) -> Usage:
        return Usage(**self._counts)

    @property
    def remaining_seconds(self) -> float:
        if not self.policy.agent.max_queue_seconds:
            return self.hard_remaining_seconds
        return max(0., min(self.hard_remaining_seconds,
                           self.policy.agent.timeout_seconds - self.execution_seconds))

    @property
    def hard_remaining_seconds(self) -> float:
        return max(0., self._deadline - time.monotonic())

    @property
    def execution_seconds(self) -> float:
        now = time.monotonic()
        return (self._execution_spent + sum(ticket.execution_seconds for ticket in self._scheduled_tickets)
                + sum(max(0., now-started) for started in self._execution_started.values()))

    @property
    def queue_seconds(self) -> float:
        now = time.monotonic()
        return (self._queue_spent + sum(ticket.wait_seconds for ticket in self._scheduled_tickets)
                + sum(max(0., now-started) for started in self._queue_started.values()))

    @property
    def queue_remaining_seconds(self) -> float:
        return max(0., min(self.hard_remaining_seconds, self.policy.agent.max_queue_seconds-self.queue_seconds))

    def _check(self) -> None:
        if self._closed:
            raise SessionClosed('session no longer accepts work')
        if asyncio.get_running_loop() is not self._loop:
            raise SessionClosed('session belongs to another event loop')
        if self._owner.cancelling() or asyncio.current_task().cancelling():
            raise asyncio.CancelledError
        if not self.remaining_seconds:
            raise DeadlineExceeded('request deadline exceeded')
        if self.policy.agent.max_queue_seconds and self.queue_seconds > self.policy.agent.max_queue_seconds:
            raise DeadlineExceeded('request queue budget exceeded')

    def _reserve(self, **amounts: int) -> None:
        self._check()
        # No await between checking all resources and committing all debits.
        for resource, amount in amounts.items():
            if type(amount) is not int or amount < 0:
                raise PolicyError('budget increments must be non-negative integers')
            section = (self.policy.ontology if resource in
                       {'nodes', 'edges', 'datasets', 'neighbor_items'} else self.policy.agent)
            limit = getattr(section, f'max_{resource}')
            if self._counts[resource] + amount > limit:
                raise BudgetExceeded(resource, limit)
        for resource, amount in amounts.items():
            self._counts[resource] += amount

    def next_turn(self) -> None:
        self._reserve(turns=1)

    async def _invoke(self, factory: Callable[[], Awaitable[T]], *, _resource='tool',
                      _consume=None, **costs: int) -> T:
        retry = False
        while True:
            lease = self._capacity.acquire_operation(_resource) if self._capacity else None
            try:
                self._reserve(**costs, retries=int(retry))
            except BaseException:
                if lease:
                    lease.release()
                raise
            started = time.monotonic()
            timeout = min(self.policy.agent.call_timeout_seconds, self.remaining_seconds)
            if self._capacity and _resource == 'db':
                timeout = min(timeout, self.policy.server.db_timeout_seconds)

            consuming = False

            async def execute():
                nonlocal consuming
                self._check()
                if self._capacity:
                    self._capacity.check()
                operation = factory()
                if not inspect.isawaitable(operation):
                    raise AdapterContractError('adapter must return an awaitable')
                value = await operation
                if _consume is not None:
                    # A lazy DB cursor remains inside this lease and timeout.
                    # Once consumed, replaying it could duplicate graph state.
                    consuming = True
                    return await _consume(value)
                return value

            coroutine = execute()
            try:
                task = asyncio.create_task(coroutine)
            except BaseException:
                coroutine.close()
                if lease:
                    lease.release()
                raise
            if lease:
                # Captured default binds THIS attempt, including retry attempts.
                task.add_done_callback(lambda _, owned=lease: owned.release())
            self._operations.add(task)
            if self.policy.agent.max_queue_seconds:
                self._execution_started[task] = started
            caller = asyncio.current_task()
            cancellation_count = caller.cancelling()
            timeout_scope = asyncio.timeout(timeout)
            try:
                async with timeout_scope:
                    value = await task
                if caller.cancelling() > cancellation_count:
                    raise asyncio.CancelledError
                if timeout_scope.expired() or time.monotonic() - started >= timeout:
                    raise DeadlineExceeded('adapter returned after its deadline')
                self._check()
                return value
            except TimeoutError as exc:
                if timeout_scope.expired():
                    raise DeadlineExceeded('operation deadline exceeded') from exc
                raise
            except RetryableError:
                if consuming:
                    raise
                if caller.cancelling() > cancellation_count:
                    raise asyncio.CancelledError
                if timeout_scope.expired():
                    raise DeadlineExceeded('adapter failed after its deadline')
                # Only this explicit exception is eligible. A failed or uncertain
                # dispatch keeps its full reservation, including token ceilings.
                retry = True
            finally:
                if task in self._execution_started:
                    self._execution_spent += time.monotonic()-self._execution_started.pop(task)
                if task.done():
                    self._operations.discard(task)
                    if lease:
                        lease.release()

    async def tool(self, operation: Callable[[], Awaitable[T]]) -> T:
        value = await self._invoke(operation, tool_calls=1)
        encoded = bounded_json(value, self.policy.agent.max_result_bytes)
        detached = json.loads(encoded)
        self._check()
        return detached

    async def database(self, operation: Callable[[], Awaitable[T]]) -> T:
        """Lease includes pool checkout, execution, cancellation, rollback and return."""
        value = await self._invoke(operation, _resource='db', tool_calls=1)
        detached = json.loads(bounded_json(value, self.policy.agent.max_result_bytes))
        self._check()
        return detached

    @asynccontextmanager
    async def waiting_slot(self, semaphore):
        """Queue for a bounded retained-result permit, without charging its hold time."""
        self._check()
        if not self.policy.agent.max_queue_seconds:
            raise PolicyError('scheduled waiting needs an explicit queue budget')
        seconds = self.queue_remaining_seconds
        if seconds <= 0:
            raise DeadlineExceeded('request queue budget exceeded')
        task = asyncio.create_task(semaphore.acquire())
        self._operations.add(task)
        self._queue_started[task] = time.monotonic()
        acquired = False
        try:
            scope = asyncio.timeout(seconds)
            try:
                async with scope:
                    await task
                acquired = True
            except TimeoutError:
                acquired = task.done() and not task.cancelled() and task.exception() is None
                if scope.expired():
                    raise DeadlineExceeded('request capacity queue wait exceeded') from None
                raise
            except BaseException:
                # Semaphore.acquire restores its permit when cancellation wins
                # during handoff. If it already returned, this context owns it.
                acquired = task.done() and not task.cancelled() and task.exception() is None
                raise
            finally:
                self._queue_spent += time.monotonic()-self._queue_started.pop(task)
                self._operations.discard(task)
            self._check()
            yield
        finally:
            if acquired:
                semaphore.release()

    async def _scheduled_invoke(self, stage, key, operation, *, operation_seconds,
                                _resource='tool', _on_started=None, **costs):
        self._check()
        if not self.policy.agent.max_queue_seconds:
            raise PolicyError('scheduled work needs an explicit queue budget')
        seconds = min(self.policy.agent.call_timeout_seconds, stage.operation_seconds)
        if operation_seconds is not None:
            if type(operation_seconds) not in (int, float) or not 0 < operation_seconds <= seconds:
                raise PolicyError('invalid scheduled operation budget')
            seconds = operation_seconds
        attempt = 0
        while True:
            self._reserve(**costs, retries=int(attempt > 0))
            waiting = min(stage.wait_seconds, self.queue_remaining_seconds)
            if waiting <= 0:
                raise DeadlineExceeded('request queue budget exceeded')

            async def dispatched(deadline):
                # Shared work must not depend on the first waiter's session
                # staying open. The global capacity object outlives its owner.
                lease = self._capacity.acquire_operation(_resource) if self._capacity else None
                try:
                    if self._capacity:
                        self._capacity.check()
                    awaitable = operation(deadline)
                    if not inspect.isawaitable(awaitable):
                        raise AdapterContractError('adapter must return an awaitable')
                    return await awaitable
                finally:
                    if lease:
                        lease.release()

            async def execute():
                ticket = None
                try:
                    async with stage.ticket((key, attempt), dispatched, operation_seconds=seconds,
                                            queue_seconds=waiting, hard_deadline=self._deadline) as ticket:
                        self._scheduled_tickets.add(ticket)
                        await ticket.started()
                        if _on_started:
                            _on_started()
                        self._check()
                        remaining = min(seconds-ticket.execution_seconds, self.remaining_seconds)
                        if remaining <= 0:
                            raise DeadlineExceeded('scheduled operation budget exceeded')
                        scope = asyncio.timeout(remaining)
                        try:
                            async with scope:
                                result = await ticket.result()
                            if scope.expired() or ticket.execution_seconds > seconds:
                                raise DeadlineExceeded('scheduled operation deadline exceeded')
                        except TimeoutError:
                            if scope.expired():
                                raise DeadlineExceeded('scheduled operation deadline exceeded') from None
                            raise
                        self._check()
                        return result
                finally:
                    if ticket is not None:
                        self._scheduled_tickets.remove(ticket)
                        self._queue_spent += ticket.wait_seconds
                        self._execution_spent += ticket.execution_seconds

            task = asyncio.create_task(execute())
            self._operations.add(task)
            try:
                return await task
            except RetryableError:
                self._check()
                attempt += 1
            finally:
                if task.done():
                    self._operations.discard(task)

    async def scheduled_tool(self, stage, key, operation, *, operation_seconds=None):
        value = await self._scheduled_invoke(stage, key, operation, operation_seconds=operation_seconds,
                                             tool_calls=1)
        detached = json.loads(bounded_json(value, self.policy.agent.max_result_bytes))
        self._check()
        return detached

    async def scheduled_llm(self, stage, key, provider, prompt, *, input_token_bound,
                            max_output_tokens, escalation=None, operation_seconds=None):
        self._check()
        _text_size(prompt, self.policy.agent.max_prompt_bytes, 'prompt_bytes')
        if (type(input_token_bound) is not int or input_token_bound <= 0 or
                type(max_output_tokens) is not int or max_output_tokens <= 0):
            raise PolicyError('LLM token bounds must be positive integers')
        if escalation is not None and (not isinstance(escalation, EscalationReason) or not self._base_attempted):
            raise PolicyError('escalation needs a base attempt and an allowed reason')
        model = self.policy.model.escalation if escalation else self.policy.model.default
        request = LLMRequest(model, prompt, max_output_tokens)

        def started():
            if escalation is None:
                self._base_attempted = True

        async def call_provider(deadline):
            response = await provider(request, deadline)
            if (not isinstance(response, LLMResponse) or
                    type(response.input_tokens) is not int or type(response.output_tokens) is not int or
                    response.input_tokens < 0 or response.output_tokens < 0):
                return {'invalid_usage':True}
            return {'value':response.value, 'input_tokens':response.input_tokens,
                    'output_tokens':response.output_tokens}

        identity = (key, model, prompt, input_token_bound, max_output_tokens)
        response = await self._scheduled_invoke(stage, identity, call_provider, _resource='llm',
            _on_started=started, operation_seconds=operation_seconds, llm_calls=1,
            input_tokens=input_token_bound, output_tokens=max_output_tokens)
        if (not isinstance(response, dict) or set(response) != {'value', 'input_tokens', 'output_tokens'} or
                type(response['input_tokens']) is not int or type(response['output_tokens']) is not int or
                not 0 <= response['input_tokens'] <= input_token_bound or
                not 0 <= response['output_tokens'] <= max_output_tokens):
            self._closed = True
            raise AdapterContractError('provider usage is missing, invalid, or exceeds its reservation')
        self._counts['input_tokens'] -= input_token_bound-response['input_tokens']
        self._counts['output_tokens'] -= max_output_tokens-response['output_tokens']
        detached = json.loads(bounded_json(response['value'], self.policy.agent.max_result_bytes))
        self._check()
        return LLMResponse(detached, response['input_tokens'], response['output_tokens'])

    async def tool_bytes(self, factory, *, max_bytes=None) -> bytes:
        from .streams import collect_bytes, positive_limit
        limit = positive_limit(max_bytes, self.policy.agent.max_result_bytes)
        return await self._invoke(lambda: collect_bytes(self, factory, limit), tool_calls=1)

    async def database_rows(self, factory, *, max_rows=None):
        from .streams import collect_rows, positive_limit
        limit = positive_limit(max_rows, self.policy.agent.max_result_rows)
        return await self._invoke(lambda: collect_rows(self, factory, limit), _resource='db', tool_calls=1)

    def _mark_cleanup_failed(self):
        self._closed = True
        if self._capacity:
            self._capacity.poison('adapter_cleanup_failed')
        if self._on_cleanup_failure:
            self._on_cleanup_failure()

    async def llm(self, provider: Callable[[LLMRequest], Awaitable[LLMResponse]],
                  prompt: str, *, input_token_bound: int, max_output_tokens: int,
                  escalation: EscalationReason | None = None) -> LLMResponse:
        self._check()
        _text_size(prompt, self.policy.agent.max_prompt_bytes, 'prompt_bytes')
        if (type(input_token_bound) is not int or input_token_bound <= 0 or
                type(max_output_tokens) is not int or max_output_tokens <= 0):
            raise PolicyError('LLM token bounds must be positive integers')
        if escalation is not None and (not isinstance(escalation, EscalationReason) or
                                       not self._base_attempted):
            raise PolicyError('escalation needs a base attempt and an allowed reason')
        model = self.policy.model.escalation if escalation else self.policy.model.default
        request = LLMRequest(model, prompt, max_output_tokens)

        async def call_provider():
            if escalation is None:
                self._base_attempted = True
            return await provider(request)

        response = await self._invoke(call_provider, _resource='llm', llm_calls=1,
                                      input_tokens=input_token_bound, output_tokens=max_output_tokens)
        if (not isinstance(response, LLMResponse) or
                type(response.input_tokens) is not int or type(response.output_tokens) is not int or
                not 0 <= response.input_tokens <= input_token_bound or
                not 0 <= response.output_tokens <= max_output_tokens):
            self._closed = True
            raise AdapterContractError('provider usage is missing, invalid, or exceeds its reservation')
        # Only the successful attempt is reconciled. Prior failed attempts retain
        # their conservative reservations because their actual usage is unknown.
        self._counts['input_tokens'] -= input_token_bound - response.input_tokens
        self._counts['output_tokens'] -= max_output_tokens - response.output_tokens
        encoded = bounded_json(response.value, self.policy.agent.max_result_bytes)
        detached = json.loads(encoded)
        self._check()
        return LLMResponse(detached, response.input_tokens, response.output_tokens)

    async def traverse(self, roots, neighbors):
        from .graph import traverse
        return await traverse(self, roots, neighbors)

    async def traverse_local(self, roots, neighbors):
        """Bounded adjacency traversal inside an enclosing local graph tool."""
        from .graph import traverse
        return await traverse(self, roots, neighbors, in_memory=True)

    async def traverse_stream(self, roots, neighbors):
        from .graph import traverse
        return await traverse(self, roots, neighbors, streamed=True)

    async def _close(self) -> None:
        self._closed = True
        cancelled = False
        tasks = tuple(self._operations)
        for task in tasks:
            task.cancel()
        if tasks:
            # Keep the admission lease until children really stop. Shielding the
            # drain prevents a second caller cancellation from orphaning them.
            drain = asyncio.gather(*tasks, return_exceptions=True)
            while not drain.done():
                try:
                    await asyncio.shield(drain)
                except asyncio.CancelledError:
                    cancelled = True
            self._operations.clear()
        try:
            self.memory.close()
        finally:
            self.workspace.close()
        if cancelled:
            raise asyncio.CancelledError


class Harness:
    """One instance per process/event loop; reject excess requests immediately."""
    def __init__(self, policy: Policy | None = None, *,
                 workspace_root: str | Path = '.harness-workspaces', telemetry=None):
        self._policy = policy if policy is not None else Policy()
        if not isinstance(self._policy, Policy):
            raise PolicyError('Harness requires a validated Policy')
        self._root = Path(workspace_root).resolve()
        if self._root.exists() and any(path.name.startswith('request-') for path in self._root.iterdir()):
            raise CleanupFailed('workspace root contains old or active request directories')
        self._active = 0
        self._loop = None
        self._failed_workspaces = []
        self._adapter_cleanup_failed = False
        self.cache = SelectiveCache(self.policy.cache)
        self.capacity = None
        if self.policy.server.enabled:
            from .capacity import CapacityController
            if telemetry is None:
                if self.policy.server.telemetry == 'linux_host':
                    from .host_telemetry import LinuxHostTelemetry
                    telemetry = LinuxHostTelemetry(self.policy.server.proc_root,
                                                   disk_path=self.policy.server.disk_path)
                else:
                    from .telemetry import FargateTelemetry
                    telemetry = FargateTelemetry()
            self.capacity = CapacityController(self.policy, telemetry)

    async def start(self):
        loop = asyncio.get_running_loop()
        if self._loop is not None and self._loop is not loop:
            raise PolicyError('create one Harness per event loop')
        self._loop = loop
        if self.capacity:
            await self.capacity.start()

    async def aclose(self):
        if self._active or not self.healthy:
            raise CleanupFailed('active work or failed cleanup prevents shutdown')
        if self.capacity:
            await self.capacity.close()
        self.cache.clear()

    @property
    def policy(self) -> Policy:
        return self._policy

    @property
    def active_requests(self) -> int:
        return self._active

    @property
    def healthy(self) -> bool:
        return not self._failed_workspaces and not self._adapter_cleanup_failed

    def _mark_adapter_cleanup_failed(self):
        self._adapter_cleanup_failed = True

    def retry_cleanup(self) -> None:
        """Retry only this instance's failed workspaces, after the cause is fixed."""
        for workspace in tuple(self._failed_workspaces):
            try:
                workspace.close()
            except Exception as exc:
                raise CleanupFailed('workspace cleanup still failing') from exc
            self._failed_workspaces.remove(workspace)

    async def run(self, workflow: Callable[[Session], Awaitable[T]]) -> T:
        loop = asyncio.get_running_loop()
        if asyncio.current_task().cancelling():
            raise asyncio.CancelledError
        if self._loop is not None and self._loop is not loop:
            raise PolicyError('create one Harness per event loop')
        self._loop = loop
        if not self.healthy:
            raise CleanupFailed('new requests blocked because cleanup is unconfirmed')
        if self._active >= self.policy.admission.max_concurrent_requests:
            raise CapacityExceeded('all request slots are occupied')
        lease = self.capacity.acquire_request() if self.capacity else None
        self._active += 1
        session = None
        try:
            session = Session(self.policy, self._root, self.capacity, self._mark_adapter_cleanup_failed)
            session.next_turn()
            caller = asyncio.current_task()
            cancellation_count = caller.cancelling()
            timeout_scope = asyncio.timeout(session.hard_remaining_seconds)
            try:
                async with timeout_scope:
                    result = await workflow(session)
                if caller.cancelling() > cancellation_count:
                    raise asyncio.CancelledError
                if timeout_scope.expired():
                    raise DeadlineExceeded('workflow returned after its deadline')
                session._check()
                return result
            except TimeoutError as exc:
                if timeout_scope.expired():
                    raise DeadlineExceeded('request deadline exceeded') from exc
                raise
        finally:
            try:
                if session is not None:
                    try:
                        await session._close()
                    except Exception as exc:
                        self._failed_workspaces.append(session.workspace)
                        raise CleanupFailed('workspace cleanup failed; new requests are blocked') from exc
            finally:
                self._active -= 1
                if lease:
                    lease.release()
