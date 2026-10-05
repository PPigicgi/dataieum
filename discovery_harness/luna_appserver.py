"""Bounded shared Codex process; each query gets its own ephemeral thread.

Requests share one process with independent ephemeral threads. Optional idle
reuse is bounded by a short TTL, a generation age limit and an issued-thread cap.
The transport never writes authentication or user configuration.
"""
import asyncio
from collections import Counter
import json
import math
import os
from pathlib import Path
import re
import tempfile
import time
import tomllib

from .codex_luna import CodexLunaProvider
from .errors import AdapterContractError, CapacityExceeded, CleanupFailed
from .catalog_discovery import country_list
from .resources import bounded_json
from .runtime import LLMResponse


INITIALIZATION_TIMEOUT_SECONDS = 30.0
AUTH_REFRESH_ERRORS = ('refresh_token_reused', 'refresh_token_expired', 'refresh_token_invalidated')


class ModelInitializationTimeout(AdapterContractError):
    code = 'model_initialization_timeout'


class ModelAuthenticationRequired(AdapterContractError):
    code = 'authentication_required'


def failure_kind(error):
    text = json.dumps(error, ensure_ascii=True).lower()
    if any(code in text for code in AUTH_REFRESH_ERRORS) or 'sign in again' in text or 'log in again' in text:
        return 'authentication'
    for needles, label in ((('429', 'ratelimit', 'rate_limit'), 'rate_limit'),
                           (('quota', 'usage limit'), 'quota'),
                           (('401', '403', 'unauthorized'), 'authentication'),
                           (('out of memory', 'allocation failed'), 'memory'),
                           (('connection', 'stream', 'websocket'), 'connection')):
        if any(word in text for word in needles):
            return label
    return 'upstream'


def record_failure(failures, error):
    kind = failure_kind(error)
    failures[kind] += 1
    if kind == 'authentication':
        text = json.dumps(error, ensure_ascii=True).lower()
        for code in AUTH_REFRESH_ERRORS:
            if code in text:
                failures['authentication_' + code] += 1
                break
    return kind


class _Generation:
    def __init__(self, owner):
        self.owner = owner
        self.process = self.creation = self.reader = self.stderr = None
        self.temporary = tempfile.TemporaryDirectory(prefix='codex-shared-', dir=owner.root)
        self.directory = Path(self.temporary.name)
        self.pending, self.threads = {}, {}
        self.sequence = self.users = self.issued = 0
        self.closing = False
        self.failed = False
        self.closed = asyncio.Event()
        self.ready = asyncio.create_task(self.start())

    def request(self, method, params):
        if self.process is None or self.process.returncode is not None:
            raise AdapterContractError('Codex shared process is not running')
        self.sequence += 1
        future = asyncio.get_running_loop().create_future()
        # Cancelled owners can leave an in-flight response. Consume exceptions
        # without changing what a remaining waiter observes.
        future.add_done_callback(lambda f: f.exception() if not f.cancelled() else None)
        self.pending[self.sequence] = future
        try:
            self.process.stdin.write(bounded_json({'id':self.sequence,'method':method,'params':params},131072)+b'\n')
        except BaseException:
            self.pending.pop(self.sequence, None)
            future.cancel()
            raise
        return future

    async def start(self):
        command = [self.owner.executable, 'app-server', '--stdio', '-c', f'model="{self.owner.model}"',
                   '-c', 'model_reasoning_effort="low"', '-c', 'project_doc_max_bytes=0',
                   '-c', 'web_search="disabled"', '-c', 'developer_instructions=""']
        # App-server has no exec --ignore-user-config flag. Disable installed
        # MCP entries for this child only; never change the user's config file.
        config_path = Path(os.environ.get('CODEX_HOME', str(Path.home()/'.codex')))/'config.toml'
        config = tomllib.loads(config_path.read_text(encoding='utf-8')) if config_path.exists() else {}
        for name in config.get('mcp_servers', {}):
            if not re.fullmatch(r'[A-Za-z0-9_-]+', name):
                raise AdapterContractError('Unsupported MCP config key; shared transport not started')
            command += ['-c', f'mcp_servers.{name}.enabled=false']
        for feature in ('shell_tool','shell_snapshot','unified_exec','apps','plugins','hooks','multi_agent',
                        'browser_use','computer_use','image_generation','view_image','skill_search',
                        'sleep_tool','unbounded_connection_retries','js_repl'):
            command += ['--disable', feature]
        self.creation = asyncio.create_task(asyncio.create_subprocess_exec(*command, cwd=self.directory,
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            limit=262144, **({'start_new_session':True} if os.name != 'nt' else {})))
        self.process = await asyncio.shield(self.creation)
        self.reader = asyncio.create_task(self.read())
        self.stderr = asyncio.create_task(self.discard_stderr())
        # Process initialization is bounded preparation, before any model turn.
        # Keep it separate from queue saturation and the model execution clock.
        try:
            await asyncio.wait_for(asyncio.shield(self.request('initialize', {
                'clientInfo':{'name':'dataieum_luna_harness','version':'0.1.0'},
                'capabilities':{'experimentalApi':True}})), INITIALIZATION_TIMEOUT_SECONDS)
        except TimeoutError:
            self.owner.failures['initialization_timeout'] += 1
            raise ModelInitializationTimeout('Codex initialization timed out') from None
        self.process.stdin.write(b'{"method":"initialized","params":{}}\n')

    async def discard_stderr(self):
        total = 0
        while chunk := await self.process.stderr.read(4096):
            total += len(chunk)
            if total > 65536:
                self.fail(AdapterContractError('Codex shared stderr budget exceeded'))
                return

    def fail(self, error):
        self.failed = True
        for future in self.pending.values():
            if not future.done():
                future.set_exception(error)
        for state in self.threads.values():
            if not state['done'].done():
                state['done'].set_exception(error)

    async def read(self):
        try:
            while line := await self.process.stdout.readline():
                event = json.loads(line)
                if 'id' in event and 'method' not in event:
                    future = self.pending.pop(event['id'], None)
                    if future and not future.done():
                        if 'error' in event:
                            kind = record_failure(self.owner.failures, event['error'])
                            error_type = ModelAuthenticationRequired if kind == 'authentication' else AdapterContractError
                            future.set_exception(error_type('Codex RPC failed: '+kind))
                        else:
                            future.set_result(event.get('result'))
                    continue
                params, method = event.get('params', {}), event.get('method')
                if 'id' in event:
                    self.process.stdin.write(bounded_json({'id':event['id'],'error':{
                        'code':-32601,'message':'Tools disabled'}},4096)+b'\n')
                    raise AdapterContractError('Unexpected Codex server tool request')
                state = self.threads.get(params.get('threadId'))
                if state is None:
                    continue
                state['bytes'] += len(line)
                if state['bytes'] > 262144:
                    raise AdapterContractError('Codex shared event budget exceeded')
                if method == 'thread/tokenUsage/updated':
                    state['usage'] = params['tokenUsage']['last']
                if method in {'item/started','item/completed'}:
                    item = params.get('item', {})
                    if item.get('type') not in {'userMessage','agentMessage','reasoning'}:
                        raise AdapterContractError('Unexpected Codex tool activity')
                    if method == 'item/completed' and item.get('type') == 'agentMessage':
                        state['text'] = item['text']
                if method == 'turn/completed':
                    state['terminal'].set()
                    if not state['done'].done():
                        state['done'].set_result(params['turn'])
        except (ValueError, KeyError, TypeError, OSError, asyncio.LimitOverrunError, AdapterContractError):
            self.fail(AdapterContractError('Codex shared protocol failed'))
        finally:
            self.fail(AdapterContractError('Codex shared event stream closed'))

    async def close(self):
        try:
            try:
                await self.ready
            except Exception:
                pass
            if self.creation is not None and self.process is None:
                try:
                    self.process = await self.creation
                except Exception:
                    pass
            if self.process is not None:
                if self.process.stdin:
                    self.process.stdin.close()
                try:
                    await asyncio.wait_for(self.process.wait(), .5)
                except TimeoutError:
                    await CodexLunaProvider._stop(self, self.process)
            for task in (self.reader, self.stderr):
                if task is not None:
                    task.cancel()
            await asyncio.gather(*(t for t in (self.reader,self.stderr) if t is not None),return_exceptions=True)
            self.temporary.cleanup()
        finally:
            self.closed.set()


class SharedLunaTransport:
    def __init__(self, executable, root, *, max_threads=75, idle_seconds=0, max_age_seconds=300,
                 model='gpt-5.6-luna'):
        if model not in {'gpt-5.6-luna', 'gpt-6-luna', 'gpt-6-sol'}:
            raise ValueError('Unsupported shared model')
        self.model = model
        if type(max_threads) is not int or not 1 <= max_threads <= 300:
            raise ValueError('generation supports at most 300 issued threads')
        if (type(idle_seconds) not in (int, float) or not math.isfinite(idle_seconds)
                or not 0 <= idle_seconds <= 30):
            raise ValueError('generation idle reuse supports 0..30 seconds')
        if (type(max_age_seconds) not in (int, float) or not math.isfinite(max_age_seconds)
                or not 0 < max_age_seconds <= 300):
            raise ValueError('generation age supports at most 300 seconds')
        self.executable = str(Path(executable).resolve(strict=True))
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.current = None
        self.lock = asyncio.Lock()
        self.healthy = True
        self.generations = 0
        self.failures = Counter()
        self.max_threads = max_threads
        self.idle_seconds = float(idle_seconds)
        self.max_age_seconds = float(max_age_seconds)
        self._idle_task = self._close_task = None
        self._idle_deadline = None
        self.waiters = self.peak_waiters = 0

    @property
    def pid(self):
        return self.current.process.pid if self.current and self.current.process and self.current.process.returncode is None else None

    @property
    def idle(self):
        return bool(self.current is not None and not self.current.users
                    and not self.current.closing and self._idle_task is not None)

    @property
    def idle_remaining_seconds(self):
        return max(0., self._idle_deadline - time.monotonic()) if self.idle else 0.

    @property
    def generation_age_seconds(self):
        return max(0., time.monotonic() - self.current._transport_started_at) if self.current else 0.

    @staticmethod
    def _consume_task(task):
        if not task.cancelled():
            task.exception()

    @staticmethod
    def _failed(generation):
        if getattr(generation, 'failed', False):
            return True
        process = getattr(generation, 'process', None)
        if process is not None and process.returncode is not None:
            return True
        ready = getattr(generation, 'ready', None)
        if ready is not None and ready.done() and (ready.cancelled() or ready.exception() is not None):
            return True
        reader = getattr(generation, 'reader', None)
        return reader is not None and reader.done()

    def _cancel_idle_locked(self):
        task, self._idle_task = self._idle_task, None
        self._idle_deadline = None
        if task is not None:
            task.cancel()

    def _start_close_locked(self, generation):
        self._cancel_idle_locked()
        generation.closing = True
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close_generation(generation))
            self._close_task.add_done_callback(self._consume_task)
        return self._close_task

    async def _close_generation(self, generation):
        cleaned = False
        try:
            await generation.close()
            cleaned = True
        except BaseException:
            self.healthy = False
            raise CleanupFailed('Shared Codex process cleanup failed') from None
        finally:
            async with self.lock:
                # Keep a failed generation visible: a replacement must not
                # start while termination of the owned process is uncertain.
                if cleaned and self.current is generation:
                    self.current = None
                self._close_task = None
                generation._transport_closed.set()

    @staticmethod
    async def _wait_close(task):
        cancelled = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                cancelled = True
        task.result()
        if cancelled:
            raise asyncio.CancelledError

    async def _expire_idle(self, generation, deadline):
        await asyncio.sleep(max(0., deadline - time.monotonic()))
        async with self.lock:
            if (self._idle_task is not asyncio.current_task()
                    or self.current is not generation or generation.users):
                return
            # Detach this timer before retiring, so it cannot cancel its own
            # cleanup. Process cleanup is independently owned and shielded.
            self._idle_task = None
            self._idle_deadline = None
            self._start_close_locked(generation)

    async def acquire(self):
        while True:
            async with self.lock:
                if not self.healthy:
                    raise CleanupFailed('Shared Codex cleanup unconfirmed')
                if self.current is None:
                    self.current = _Generation(self)
                    self.current._transport_started_at = time.monotonic()
                    self.current._transport_closed = asyncio.Event()
                    self.generations += 1
                generation = self.current
                retired = self._failed(generation) or self.generation_age_seconds >= self.max_age_seconds
                if not generation.closing and not retired and generation.issued < self.max_threads:
                    self._cancel_idle_locked()
                    generation.users += 1
                    generation.issued += 1
                    return generation
                generation.closing = True
                if not generation.users:
                    self._start_close_locked(generation)
            self.waiters += 1
            self.peak_waiters = max(self.peak_waiters,self.waiters)
            try:await generation._transport_closed.wait()
            finally:self.waiters -= 1

    async def release(self, generation):
        async with self.lock:
            if self.current is not generation or generation.users <= 0:
                raise CleanupFailed('Shared Codex generation lease was not owned')
            generation.users -= 1
            if generation.users:
                return
            ready = getattr(generation, 'ready', None)
            now = time.monotonic()
            deadline = min(now + self.idle_seconds, generation._transport_started_at + self.max_age_seconds)
            delay = deadline - now
            if (delay > 0 and not generation.closing and generation.issued < self.max_threads
                    and not self._failed(generation) and ready is not None and ready.done()
                    and getattr(generation, 'process', None) is not None):
                self._idle_deadline = deadline
                self._idle_task = asyncio.create_task(self._expire_idle(generation, deadline))
                self._idle_task.add_done_callback(self._consume_task)
                return
            closing = self._start_close_locked(generation)
        await self._wait_close(closing)

    async def aclose(self):
        async with self.lock:
            if not self.healthy or (self.current is not None and self.current.users):
                raise CleanupFailed('Shared Codex transport has active work or failed cleanup')
            if self.current is None:
                return
            closing = self._start_close_locked(self.current)
        await self._wait_close(closing)


class AppServerLunaProvider(CodexLunaProvider):
    def __init__(self, transport, workspace_root, concepts, countries):
        super().__init__(transport.executable, workspace_root, concepts, countries)
        self.transport = transport
        self.model = getattr(transport, 'model', self.model)

    def status(self):
        return {**super().status(), 'pid':self.transport.pid,
                'healthy':self.healthy and self.transport.healthy,
                'transport':'shared_app_server', 'generations':self.transport.generations,
                'generation_thread_limit':getattr(self.transport,'max_threads',75),
                'generation_issued':self.transport.current.issued if getattr(self.transport,'current',None) else 0,
                'generation_waiters':getattr(self.transport,'waiters',0),
                'peak_generation_waiters':getattr(self.transport,'peak_waiters',0),
                'generation_idle_seconds':getattr(self.transport,'idle_seconds',0),
                'generation_idle':getattr(self.transport,'idle',False),
                'generation_idle_remaining_seconds':getattr(self.transport,'idle_remaining_seconds',0),
                'generation_age_seconds':getattr(self.transport,'generation_age_seconds',0),
                'generation_max_age_seconds':getattr(self.transport,'max_age_seconds',300),
                'failure_reasons':dict(self.transport.failures)}

    async def __call__(self, request, *, prepared_generation=None):
        if request.model != self.model or request.store or len(request.prompt.encode()) > (24000 if getattr(self, '_response_contract', None) else 8192):
            raise AdapterContractError('Invalid shared Luna request')
        if self.busy:
            raise CapacityExceeded('Shared Luna worker is already leased')
        self.busy = True
        generation = prepared_generation
        owns_generation = generation is None
        state = thread_future = turn_future = thread_id = None
        try:
            if owns_generation:
                generation = await self.transport.acquire()
            await asyncio.shield(generation.ready)
            schema, instructions = self.classification_contract()
            thread_future = generation.request('thread/start', {'model':self.model,'ephemeral':True,
                'cwd':str(generation.directory),'baseInstructions':instructions,'developerInstructions':'',
                'sandbox':'read-only','approvalPolicy':'never','environments':[],'dynamicTools':[],
                'allowProviderModelFallback':False,'config':{'model_reasoning_effort':'low','project_doc_max_bytes':0}})
            thread = await asyncio.shield(thread_future)
            if thread.get('model') != self.model or thread['thread'].get('ephemeral') is not True:
                raise AdapterContractError('Shared Codex model or persistence contract changed')
            thread_id = thread['thread']['id']
            state = {'done':asyncio.get_running_loop().create_future(),'terminal':asyncio.Event(),'bytes':0}
            state['done'].add_done_callback(lambda f:f.exception() if not f.cancelled() else None)
            generation.threads[thread_id] = state
            turn_future = generation.request('turn/start', {'threadId':thread_id,'model':self.model,'effort':'low',
                'input':[{'type':'text','text':request.prompt}],'outputSchema':schema,'environments':[]})
            self._stats['started'] += 1
            await asyncio.shield(turn_future)
            turn = await asyncio.shield(state['done'])
            if turn['status'] != 'completed':
                kind = record_failure(self.transport.failures, turn.get('error'))
                error_type = ModelAuthenticationRequired if kind == 'authentication' else AdapterContractError
                raise error_type('Codex shared turn failed: '+kind)
            usage = state.get('usage', {})
            for key in ('inputTokens','outputTokens','cachedInputTokens'):
                if type(usage.get(key)) is not int or usage[key] < 0:
                    raise AdapterContractError('Shared Codex did not report token usage')
            # A completed but invalid answer still consumed tokens. Record the
            # reported usage before validating its content, never invent zeroes.
            for dest, source in [('input_tokens','inputTokens'),('output_tokens','outputTokens'),('cached_input_tokens','cachedInputTokens')]:
                self._stats[dest] += usage[source]
            self._stats['usage_reports'] = self._stats.get('usage_reports', 0) + 1
            value = json.loads(state['text'])
            self.validate_value(value)
            if usage['outputTokens'] > request.max_output_tokens:
                raise AdapterContractError('Shared Codex output token budget exceeded')
            self._stats['completed'] += 1
            self.last_model_success_at = time.time()
            return LLMResponse(value,usage['inputTokens'],usage['outputTokens'])
        except BaseException:
            self._stats['failed'] += 1
            raise
        finally:
            async def cleanup():
                try:
                    if generation and thread_future is not None and thread_id is None:
                        # A cancelled thread/start may have created a remote
                        # thread whose ID was never received. Drain this
                        # generation instead of retaining unknown idle work.
                        generation.closing = True
                    if generation and state and not state['terminal'].is_set():
                        try:
                            async with asyncio.timeout(1.5):
                                turn = await asyncio.shield(turn_future)
                                await asyncio.shield(generation.request('turn/interrupt',{
                                    'threadId':thread_id,'turnId':turn['turn']['id']}))
                                await state['terminal'].wait()
                        except Exception:
                            # If interruption cannot be confirmed, terminate
                            # only this owned generation; other calls fail closed.
                            if generation.process is not None:
                                await CodexLunaProvider._stop(self,generation.process)
                    if generation and thread_id:
                        generation.threads.pop(thread_id,None)
                finally:
                    if generation and owns_generation:
                        await self.transport.release(generation)
            task = asyncio.create_task(cleanup())
            cancelled = False
            while not task.done():
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    cancelled = True
            try:
                task.result()
            except BaseException:
                self.healthy = False
                raise CleanupFailed('Shared Luna request cleanup failed') from None
            finally:
                self.busy = False
            if cancelled:
                raise asyncio.CancelledError
