"""Private Codex-login gateway: Luna selection -> existing Docker catalog.

The gateway binds loopback by default or a private container network explicitly.
Credentials are supplied at runtime and must never be baked into an image.
"""
import argparse
import asyncio
from collections import Counter
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import asdict, replace
import json
import hashlib
import os
from pathlib import Path
import shutil
import time
from urllib.parse import parse_qs, urlencode, urlsplit

from discovery_harness import Harness, Policy
from discovery_harness.codex_luna import CodexLunaProvider
from discovery_harness.luna_pool import LunaPool
from discovery_harness.luna_appserver import SharedLunaTransport, AppServerLunaProvider
from discovery_harness.catalog_discovery import country_list
from discovery_harness.site_search import SiteRecommendations, parse_context, intent_prompt, node_plan
from discovery_harness.dataieum import _until_disconnect
from discovery_harness.errors import AdapterContractError, BudgetExceeded, HarnessError, CapacityExceeded
from discovery_harness.process_memory import working_set_bytes
from discovery_harness.resources import bounded_json
from discovery_harness.dependency_stage import DependencyStage
from discovery_harness.runtime import LLMResponse
from discovery_harness.security import UnsafePrompt, check_prompt, load_gateway_token, gateway_authorized


HOST_MIN_FREE_BYTES = 4 * 1024 ** 3


def failure_reason(error):
    """Bounded internal categories only; never retain an input/response/exception text."""
    reasons={
        'Codex initialization timed out':'model_initialization_timeout',
        'Invalid relevance decision':'relevance_shape',
        'Invalid relevance selection':'relevance_fields',
        'Unknown or repeated relevance candidate':'relevance_id',
        'Relevance selection changed requested scope':'relevance_scope',
        'Invalid or excluded relevance tier':'relevance_tier',
        'Invalid related exclusion judgment':'relevance_exclusion',
        'Missing visible metadata evidence':'relevance_evidence',
        'Shared Codex output token budget exceeded':'model_output_budget',
        'Shared Codex did not report token usage':'model_usage_missing',
        'Invalid site search plan':'intent_contract',
    }
    for _ in range(3):
        if error is None:break
        if getattr(error,'code',None)=='authentication_required':return 'authentication_required'
        reason=reasons.get(str(error))
        if reason:return reason
        error=error.__cause__
    return 'other_harness_error'


def intro_quota_snapshot(value):
    """Sanitized included-usage only; no reserve/credits or inferred recovery."""
    buckets = value.get('rateLimitsByLimitId')
    bucket = buckets.get('codex') if isinstance(buckets, dict) else value.get('rateLimits')
    windows = []
    if isinstance(bucket, dict):
        for name in ('primary', 'secondary'):
            window = bucket.get(name)
            if window is None:
                continue
            if (type(window) is not dict or type(window.get('usedPercent')) is not int or
                    not 0 <= window['usedPercent'] <= 100 or type(window.get('resetsAt')) is not int or
                    window['resetsAt'] <= time.time()):
                return {'allowed': False, 'state': 'unavailable', 'windows': []}
            windows.append({'used_percent': window['usedPercent'], 'resets_at': window['resetsAt']})
    allowed = (value.get('ordinaryUsageAllowed') is True and bool(windows) and
               not bucket.get('spendControlReached') and not bucket.get('rateLimitReachedType') and
               all(w['used_percent'] < 99 for w in windows))
    return {'allowed': allowed, 'state': 'ready' if allowed else 'quota_stop', 'windows': windows}


async def catalog_json(url, path):
    target = urlsplit(url)
    if (target.scheme != 'http' or target.hostname not in {'127.0.0.1', 'localhost', '::1', 'atlas'}
            or target.username is not None or target.password is not None
            or target.path not in {'','/'} or target.query or target.fragment):
        raise ValueError('catalog target must be a local HTTP application')
    try:
        reader, writer = await asyncio.open_connection(target.hostname, target.port or 80, limit=16384)
    except OSError:
        raise CatalogHTTPError(503) from None
    try:
        writer.write(f'GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n'.encode())
        await writer.drain()
        head = (await reader.readuntil(b'\r\n\r\n')).decode('latin1')
        lines = head.split('\r\n')
        status = int(lines[0].split()[1])
        if status != 200:
            raise CatalogHTTPError(status)
        headers = {k.lower(): v.strip() for k, v in (line.split(':', 1) for line in lines[1:] if ':' in line)}
        size = int(headers['content-length'])
        if not 0 <= size <= 1048576:
            raise ValueError('catalog response byte budget exceeded')
        value = json.loads(await reader.readexactly(size))
        return value
    except (ValueError, KeyError, asyncio.IncompleteReadError, asyncio.LimitOverrunError):
        raise CatalogHTTPError(502, retryable=False) from None
    except OSError:
        raise CatalogHTTPError(503) from None
    finally:
        writer.close()
        try: await writer.wait_closed()
        except OSError: pass


class CatalogHTTPError(Exception):
    def __init__(self, status, *, retryable=None):
        self.status = status
        self.retryable = status in {502, 503, 504} if retryable is None else retryable


async def load_bootstrap(url, *, timeout=60., retry_delay=.5, stop_file=None):
    """Retry only transient catalogue startup failures under one total deadline."""
    attempt = 0
    async with asyncio.timeout(timeout):
        while True:
            if stop_file is not None and stop_file.exists():
                raise asyncio.CancelledError
            attempt += 1
            try:
                async with asyncio.timeout(10):
                    value = await catalog_json(url, '/api/bootstrap')
            except CatalogHTTPError as error:
                if not error.retryable:
                    raise
            except TimeoutError:
                pass
            else:
                if (not isinstance(value, dict) or not isinstance(value.get('concepts'), list)
                        or not value['concepts'] or not isinstance(value.get('sources'), list)
                        or any(not isinstance(item, dict) or
                               not all(isinstance(item.get(k), str) and item[k] for k in ('id','name'))
                               for item in value['concepts'])
                        or any(not isinstance(item, dict) or not isinstance(item.get('id'), str)
                               or (item.get('country') is not None and not isinstance(item['country'], str))
                               for item in value['sources'])):
                    raise ValueError('Invalid catalog bootstrap')
                return value
            print(json.dumps({'event':'catalog_bootstrap_wait','attempt':attempt}), flush=True)
            await asyncio.sleep(min(4., retry_delay * 2 ** min(attempt-1, 3)))


class LunaDiscovery:
    def __init__(self, provider, catalog_url, workspace_root, *, request_seconds=20., call_seconds=12., sites=None, gateway_token=None, vector_tools=None, topic_lookup=None):
        self.provider, self.catalog_url = provider, catalog_url
        self.sites = sites
        self.gateway_token=gateway_token
        self.vector_tools=vector_tools
        self.topic_lookup=topic_lookup
        self.login_readiness = None
        # Vector discovery uses one intent call and one bounded relevance call.
        # The older lexical approval helper remains outside this workflow.
        model_workers=len(provider.providers) if isinstance(provider,LunaPool) else 1
        intent_workers=model_workers if vector_tools else 20 if model_workers==25 else model_workers
        self._prepared_model = ContextVar('prepared_luna_worker', default=None)
        preparation = self.prepare_model_worker if isinstance(provider, LunaPool) else None
        self.model_stage=DependencyStage(workers=intent_workers,queued=1025-intent_workers,
            wait_seconds=7200,operation_seconds=30,max_waiters=1025,prepare=preparation)
        self.judge_stage=(DependencyStage(workers=5,queued=1020,wait_seconds=7200,operation_seconds=30,max_waiters=1025,
                                         prepare=preparation)
                          if model_workers==25 and not vector_tools else self.model_stage)
        first=provider.providers[0] if isinstance(provider,LunaPool) else provider
        self.intent_signature=first.classification_contract() if hasattr(first,'classification_contract') else ['intent']
        self.output_tokens = 1024 if sites else 512
        self.workspace_root = Path(workspace_root).resolve()
        self.tasks = set()
        self.catalog_lock = asyncio.Lock()
        self.catalog_calls = 0
        self.catalog_cache_skipped = 0
        policy = Policy.from_dict({
            'agent': {'timeout_seconds': request_seconds, 'call_timeout_seconds': call_seconds,
                      'max_turns':1,
                      'max_tool_calls':(3 if topic_lookup else 2) if vector_tools else 1,
                      'max_llm_calls': 2 if vector_tools else 1, 'max_retries': 0,
                      'max_input_tokens': 28000 if vector_tools else 12000,
                      'max_output_tokens': self.output_tokens + (2048 if vector_tools else 0), 'max_result_bytes': 1048576},
            'ontology': {'max_depth':7,'max_nodes':192,'max_edges':384,'max_datasets':32,'max_neighbor_items':1024},
            'admission': {'max_concurrent_requests': min(25,len(provider.providers) + provider.queue_size)
                          if isinstance(provider, LunaPool) else 2},
            'workspace': {'max_size_mb': 1, 'max_files': 4, 'ttl_seconds': 30.0},
            'cache': {'max_entries': 128, 'max_size_bytes': 1048576,
                      'max_entry_bytes': 262144, 'ttl_seconds': 30.0},
        })
        self.harness = Harness(policy, workspace_root=workspace_root)
        # Queue time is separate; one intent plus one relevance call, no retries.
        queued_policy=replace(policy,agent=replace(policy.agent,timeout_seconds=60.,call_timeout_seconds=30.,max_queue_seconds=7200,
            max_llm_calls=min(2,policy.agent.max_llm_calls)),
            workspace=replace(policy.workspace,ttl_seconds=7270.),admission=replace(policy.admission,max_concurrent_requests=1000))
        self.queued_harness=Harness(queued_policy,workspace_root=Path(workspace_root)/'queued')
        related_policy=replace(policy,agent=replace(policy.agent,timeout_seconds=20.,call_timeout_seconds=12.,
            max_tool_calls=6,max_llm_calls=1,max_input_tokens=16000,max_output_tokens=2048),
            admission=replace(policy.admission,max_concurrent_requests=1))
        self.related_harness=Harness(related_policy,workspace_root=Path(workspace_root)/'related')
        self.related_task=None
        self.related_active=False
        self.related_outcomes=Counter()
        intro_policy = replace(policy, agent=replace(policy.agent, timeout_seconds=32.,
            call_timeout_seconds=30., max_output_tokens=4096, max_input_tokens=12000),
            admission=replace(policy.admission, max_concurrent_requests=1),
            workspace=replace(policy.workspace, ttl_seconds=40.))
        self.intro_harness = Harness(intro_policy, workspace_root=Path(workspace_root)/'intros')
        from discovery_harness.dataset_intro import DatasetIntros
        self.intros = DatasetIntros(self.intro_metadata, self.generate_intros, model=policy.model.default)
        self.precompute_active = False
        self.precompute_batches = 0
        self.inflight_requests=0
        self.outcomes = Counter()
        self.failure_reasons = Counter()
        self.connected = 0
        self.peak_connected = 0
        self.peak_rss = 0
        self.started_at = time.monotonic()
        self.instance = None
        self.site_workflow = None
        self.judgment_paths = Counter()
        from discovery_harness.resources import SelectiveCache
        from discovery_harness.policy import CachePolicy
        self.judgment_cache = SelectiveCache(CachePolicy(max_entries=128, max_size_bytes=1048576,
            max_entry_bytes=262144, ttl_seconds=600.))
        if sites:
            from .site_workflow import SiteWorkflow
            self.site_workflow = SiteWorkflow(sites, self.interpret, vector_tools=vector_tools,
                                              judge=self.assess_relevance, topic_lookup=topic_lookup)

    def disk_status(self):
        """Sample the gateway host, not the free space inside a thin Docker VHD."""
        result = {'ready': False, 'state': 'host_disk_unavailable', 'free_bytes': None,
                  'min_free_bytes': HOST_MIN_FREE_BYTES, 'basis': 'gateway_workspace_filesystem'}
        try:
            # Harness creates new workspace directories lazily. Their nearest
            # existing ancestor is on the filesystem that would hold that work.
            # Permission/stat failures are not treated as a missing directory.
            path = self.workspace_root
            for _ in range(64):
                try:
                    path.stat()
                    break
                except FileNotFoundError:
                    if path.parent == path:
                        return result
                    path = path.parent
            else:
                return result
            free = shutil.disk_usage(path).free
            if type(free) is not int or free < 0:
                return result
            ready = free >= HOST_MIN_FREE_BYTES
            return {**result, 'ready': ready, 'state': 'ready' if ready else 'host_disk_low',
                    'free_bytes': free}
        except (OSError, ValueError, TypeError, AttributeError):
            return result

    async def intro_metadata(self, identifier):
        return await catalog_json(self.catalog_url, '/api/ontology/metadata?' + urlencode({'id': identifier}))

    async def precompute_intros(self, records=None):
        """Private offline worker only. Reuse the existing authenticated Luna pool."""
        from discovery_harness.dataset_intro import contract
        if records is not None:
            if type(records) is not list or not 1 <= len(records) <= 5:
                raise ValueError('invalid records')
            for record in records:
                if (type(record) is not dict or set(record) != {'id','fields','truncated'} or
                        type(record['fields']) is not dict or type(record['truncated']) is not bool or
                        set(record['fields']) - {'title','description','classification_paths','survey_name','tags'} or
                        not record['fields'].get('title') or
                        any(type(v) is not str for v in record['fields'].values())):
                    raise ValueError('invalid record')
            schema, instructions, prompt, check = contract(records)
        if (self.precompute_active or self.inflight_requests or not self.disk_status()['ready'] or
                not isinstance(self.provider, LunaPool) or self.provider.waiters or self.provider.slots.empty()):
            return 503, {'code': 'intro_chat_busy'}
        self.precompute_active = True
        try:
            async with self.provider.lease(.001) as worker:
                if not isinstance(worker, AppServerLunaProvider):
                    return 503, {'code': 'intro_quota_unavailable'}
                generation = await worker.transport.acquire()
                try:
                    try:
                        async with asyncio.timeout(10):
                            await asyncio.shield(generation.ready)
                            value = await asyncio.shield(generation.request('account/rateLimits/read',
                                {'excludeResetCreditDetails': True, 'supportsLunaReserve': False}))
                        quota = intro_quota_snapshot(value)
                    except (HarnessError, TimeoutError, ValueError, TypeError):
                        return 503, {'code': 'intro_quota_unavailable'}
                    if records is None:
                        return 200, quota
                    if not quota['allowed']:
                        return 503, {'code': 'intro_quota_stop'}
                    if self.inflight_requests or self.provider.waiters:
                        return 503, {'code': 'intro_chat_busy'}
                    async def workflow(session):
                        previous = getattr(worker, '_response_contract', None)
                        worker._response_contract = (schema, instructions, check)
                        async def invoke(request):
                            return await worker(request, prepared_generation=generation)
                        try:
                            response = await session.llm(invoke, prompt, input_token_bound=12000, max_output_tokens=4096)
                            return check(response.value)
                        finally:
                            worker._response_contract = previous
                    result = await self.intro_harness.run(workflow)
                    self.precompute_batches += 1
                    return 200, result
                finally:
                    # Own the lease until provider cancellation/cleanup has drained.
                    cleanup = asyncio.create_task(worker.transport.release(generation))
                    while not cleanup.done():
                        try: await asyncio.shield(cleanup)
                        except asyncio.CancelledError: pass
                    cleanup.result()
        finally:
            self.precompute_active = False

    async def generate_intros(self, records):
        from discovery_harness.dataset_intro import contract
        schema, instructions, prompt, check = contract(records)
        if (self.inflight_requests or not self.disk_status()['ready'] or
                not isinstance(self.provider, LunaPool) or self.provider.waiters or self.provider.slots.empty()):
            raise CapacityExceeded('Chat takes priority over introductions')
        # The nonempty check and immediate lease acquisition contain no IO await.
        # Do not enter the long-lived chat/judgment dependency queues.
        async with self.provider.lease(.001) as worker:
            async def workflow(session):
                if worker.busy:
                    raise CapacityExceeded('Introduction worker is busy')
                previous = getattr(worker, '_response_contract', None)
                worker._response_contract = (schema, instructions, check)
                try:
                    response = await session.llm(worker, prompt, input_token_bound=12000, max_output_tokens=4096)
                    return check(response.value)
                finally:
                    worker._response_contract = previous
            return await self.intro_harness.run(workflow)

    def metrics(self):
        rss = working_set_bytes()
        self.peak_rss = max(self.peak_rss, rss or 0)
        return {'service': 'dataieum-luna', 'instance': self.instance, 'pid': os.getpid(),
                'related_suggestions':{'active':self.related_active,'limit':1,'timeout_seconds':20,
                                       'outcomes':dict(self.related_outcomes)},
                'uptime_seconds': round(time.monotonic()-self.started_at, 3),
                'provider': self.provider.status(), 'cache': self.harness.cache.status(),
                'dataset_intros': {'generated_batches': self.intros.generated, 'cache_hits': self.intros.hits,
                    'cache': self.intros.cache.status(), 'active': int(self.intros.flight is not None or self.precompute_active),
                    'precomputed_batches': self.precompute_batches},
                'catalog_calls': self.catalog_calls,
                'catalog_cache_skipped': self.catalog_cache_skipped,
                'active_requests': self.inflight_requests,
                'max_queued_sessions':1000,'max_legacy_sessions':25,
                'queued_active_requests':self.queued_harness.active_requests,
                'workspace_directories': len(list(self.workspace_root.glob('request-*')))+len(list((self.workspace_root/'queued').glob('request-*'))),
                'http_status': dict(self.outcomes), 'failure_reasons':dict(self.failure_reasons), 'connected': self.connected,
                'peak_connected': self.peak_connected, 'gateway_rss_bytes': rss,
                'gateway_peak_sampled_rss_bytes': self.peak_rss,
                'host_disk': self.disk_status(),
                'workflow': {'engine': 'langgraph' if self.site_workflow else 'legacy',
                             'recursion_limit': self.site_workflow.recursion_limit if self.site_workflow else None,
                             'vector_tools': dict(self.vector_tools.calls) if self.vector_tools else None,
                             'metadata_buffers':self.site_workflow.memory_status() if self.site_workflow else None},
                'dependencies': self.vector_tools.status() if self.vector_tools else None,
                'topic_classification': self.topic_lookup.status() if self.topic_lookup else None,
                'model_requests':self.model_stage.status(),
                'judgment_requests':self.judge_stage.status(),
                'shared_model_lane':self.judge_stage is self.model_stage,
                'judgment_paths': dict(self.judgment_paths),
                'judgment_cache': self.judgment_cache.status(),
                'host_gateway': True, 'docker_cpu_ram_limits_cover_gateway': False}

    async def lookup(self, concept, countries):
        # Cache only public catalog results AFTER every request has used Luna.
        # Serialize cold misses to the single DB worker. Cancelling the owner
        # releases the lock; no detached background fetch is retained.
        plan = json.dumps([concept, countries], ensure_ascii=False, separators=(',', ':'))
        key = 'public-catalog:' + hashlib.sha256(plan.encode()).hexdigest()
        cached = self.harness.cache.get('metadata', key)
        if cached is not None:
            return cached
        async with self.catalog_lock:
            cached = self.harness.cache.get('metadata', key)
            if cached is not None:
                return cached
            self.catalog_calls += 1
            result = await catalog_json(self.catalog_url, '/api/discovery?' + urlencode({
                'concept': concept, 'countries': json.dumps(countries, ensure_ascii=False)}))
            if (not isinstance(result, dict) or type(result.get('total')) is not int or result['total'] < 0 or
                    not isinstance(result.get('datasets'), list) or len(result['datasets']) > 30 or
                    not isinstance(result.get('scope'), dict) or result['scope'].get('countries') != countries or
                    result['scope'].get('basis') != 'provider_country'):
                raise AdapterContractError('Invalid catalog response shape')
            try:
                self.harness.cache.put('metadata', key, result)
            except BudgetExceeded:
                # A valid result may fit the response budget but not one cache
                # entry. Return it without retaining it or changing cache caps.
                self.catalog_cache_skipped += 1
            return result

    async def interpret(self, session, query, context=None):
        prompt = intent_prompt(query,context) if self.sites else query
        session.memory.append('user', query)
        if self.harness.cache.get('metadata', 'verified-concepts') is None:
            self.harness.cache.put('metadata', 'verified-concepts', self.provider.concepts)
        try:
            if isinstance(self.provider, LunaPool):
                response = await self.shared_model_call(session,prompt,input_bound=12000,output_bound=self.output_tokens)
                value = self.decode_intent_response(self.provider.providers[0], response.value)
            else:
                response = await session.llm(self.provider, prompt, input_token_bound=12000, max_output_tokens=self.output_tokens)
                value = self.decode_intent_response(self.provider, response.value)
        except (ValueError, TypeError):
            raise AdapterContractError('Invalid Codex event or final response') from None
        return value

    @asynccontextmanager
    async def prepare_model_worker(self):
        """The scheduled job owns this lease; generation drain is queue time."""
        async with self.provider.lease(self.provider.wait_seconds) as worker:
            generation = token = None
            try:
                if isinstance(worker, AppServerLunaProvider):
                    generation = await worker.transport.acquire()
                    await asyncio.shield(generation.ready)
                token = self._prepared_model.set((worker, generation))
                yield
            finally:
                if token is not None:
                    self._prepared_model.reset(token)
                if generation is not None:
                    await worker.transport.release(generation)

    async def shared_model_call(self,session,prompt,*,input_bound,output_bound,contract=None):
        """Share exact in-flight calls; the job owns its worker, never a waiter.

        Each session reserves/checks the full logical usage independently.
        Provider counters record actual upstream calls, including shared work.
        """
        stage=self.judge_stage if contract else self.model_stage
        if session.policy.agent.max_queue_seconds:
            signature=['judgment',*contract[:2]] if contract else ['intent',self.intent_signature]
            key=hashlib.sha256(bounded_json([prompt,signature,session.policy.model.default,input_bound,output_bound],262144)).hexdigest()
            async def provider(request,deadline):
                worker, generation = self._prepared_model.get()
                if contract:worker._response_contract=contract
                try:
                    if generation is not None:
                        return await worker(request, prepared_generation=generation)
                    return await worker(request)
                finally:
                    if contract:worker._response_contract=None
            return await session.scheduled_llm(stage,key,provider,prompt,
                input_token_bound=input_bound,max_output_tokens=output_bound,operation_seconds=30)
        async def provider(request):
            signature=['judgment',*contract[:2]] if contract else ['intent',self.intent_signature]
            key=hashlib.sha256(bounded_json([asdict(request),signature,session.policy.agent.timeout_seconds,session.policy.agent.call_timeout_seconds],262144)).hexdigest()
            async def operation(deadline):
                async with self.provider.lease(max(0,deadline-time.monotonic())) as worker:
                    if contract:worker._response_contract=contract
                    try:
                        result=await worker(request)
                        return {'value':result.value,'input_tokens':result.input_tokens,'output_tokens':result.output_tokens}
                    finally:
                        if contract:worker._response_contract=None
            value=await stage.run(key,operation,budget_seconds=min(session.remaining_seconds,session.policy.agent.call_timeout_seconds),queue_seconds=2)
            return LLMResponse(**value)
        return await session.llm(provider,prompt,input_token_bound=input_bound,max_output_tokens=output_bound)

    def decode_intent_response(self, worker, value):
        if self.sites and getattr(worker, 'site_intent_format', None) == 'compact-v1':
            from discovery_harness.compact_intent import decode_intent
            return decode_intent(value)
        return value

    async def _run_request(self,operation,queued):
        # Admission only: declining new work must not interrupt active requests.
        if not self.disk_status()['ready']:
            raise CapacityExceeded('Host workspace filesystem has insufficient or unknown free space')
        if queued:
            if self.queued_harness.active_requests>=1000:raise CapacityExceeded('Async session capacity is full')
        elif self.harness.active_requests>=25:raise CapacityExceeded('Legacy request capacity is full')
        # Optional enrichment yields its owned calls when a real question arrives.
        if self.related_task is not None and not self.related_task.done():self.related_task.cancel()
        self.inflight_requests+=1
        try:return await (self.queued_harness if queued else self.harness).run(operation)
        finally:self.inflight_requests-=1

    async def explore(self, indicator, countries=None, context=None, *, queued=False):
        plan = node_plan(indicator, countries, context)
        if self.site_workflow is None:
            raise AdapterContractError('Site workflow is not configured')
        async def workflow(session):
            result = await self.site_workflow.run(session, plan=plan)
            result['usage'] = asdict(session.usage)
            if queued:result['scheduling']=self.session_timing(session)
            return result
        return await self._run_request(workflow,queued)

    @staticmethod
    def session_timing(session):
        return {'mode':'independent_async_session','stage_wait_seconds':round(session.queue_seconds,4),
                'charged_execution_seconds':round(session.execution_seconds,4),
                'execution_budget_seconds':session.policy.agent.timeout_seconds,
                'queue_budget_seconds':session.policy.agent.max_queue_seconds}

    async def related(self,plan,exclude_ids,exclude_url_hashes=()):
        from discovery_harness.related_suggestions import retrieve_related,empty_result
        from discovery_harness.site_search import validate_plan
        validate_plan(plan)
        if (not isinstance(exclude_ids,list) or len(exclude_ids)>10 or
                any(not isinstance(i,str) or not 1<=len(i)<=512 for i in exclude_ids)):
            raise ValueError('Invalid related exclusions')
        if (not isinstance(exclude_url_hashes,(tuple,list)) or len(exclude_url_hashes)>10 or
                any(not isinstance(v,str) or len(v)!=64 or any(c not in '0123456789abcdef' for c in v) for v in exclude_url_hashes)):
            raise ValueError('Invalid related URL exclusions')
        if (not self.vector_tools or not self.site_workflow or self.inflight_requests or
                self.related_active or not self.disk_status()['ready']):
            self.related_outcomes['skipped_busy']+=1
            return empty_result('skipped')
        self.related_active=True
        started=time.monotonic()
        async def run(session):
            result=await retrieve_related(session,self,plan,exclude_ids,exclude_url_hashes)
            result['usage']=asdict(session.usage)
            result['seconds']=round(time.monotonic()-started,3)
            self.related_outcomes[result['status']]+=1
            return result
        task=None
        try:
            await self.related_harness.start()
            task=asyncio.create_task(self.related_harness.run(run))
            self.related_task=task
            return await task
        except asyncio.CancelledError:
            if asyncio.current_task().cancelling():raise
            self.related_outcomes['preempted']+=1
            return empty_result('skipped')
        except HarnessError as error:
            self.related_outcomes[error.code]+=1
            return empty_result('skipped')
        finally:
            if task is not None:
                if not task.done():task.cancel()
                await asyncio.gather(task,return_exceptions=True)
            if self.related_task is task:self.related_task=None
            self.related_active=False

    async def assess_relevance(self, session, query, plan, candidates, *, per_need_limit=None):
        from discovery_harness.relevance import contract as relevance_contract
        schema, instructions, prompt, validate, apply = relevance_contract(query, plan, candidates, self.sites.sources,per_need_limit=per_need_limit,compact=True)
        if not json.loads(prompt)['candidates_data']:
            return apply({'candidate_ids': []} if 'candidate_ids' in schema['properties'] else {'matches': []})

        def check(value):
            try:
                return validate(value)
            except (ValueError, TypeError, KeyError) as error:
                raise AdapterContractError('Unsupported dataset relevance decision') from error

        key = hashlib.sha256(bounded_json(['relevance', schema, instructions, prompt], 262144)).hexdigest()
        cached = self.judgment_cache.get('metadata', key)
        if cached is not None:
            self.judgment_paths['relevance_cache'] += 1
            return apply(check(cached))
        self.judgment_paths['relevance_single'] += 1
        if isinstance(self.provider, LunaPool):
            response = await self.shared_model_call(session, prompt, input_bound=16000, output_bound=2048,
                contract=(schema, instructions, check))
        else:
            if self.provider.busy:
                raise AdapterContractError('Cannot replace an active worker contract')
            self.provider._response_contract = (schema, instructions, check)
            try:
                response = await session.llm(self.provider, prompt, input_token_bound=16000, max_output_tokens=2048)
            finally:
                self.provider._response_contract = None
        decision = check(response.value)
        try:
            self.judgment_cache.put('metadata', key, decision)
        except BudgetExceeded:
            self.judgment_paths['relevance_cache_skipped'] += 1
        return apply(decision)

    async def judge(self, session, query, plan, candidates):
        from discovery_harness.vector_judge import approval_judgment, validate_decision
        contract = approval_judgment(query, plan, candidates)
        # Interpret with Luna and retrieve fresh metadata on EVERY request.
        # Reuse only a previous judgment of the exact same complete contract,
        # query, contextual meaning, ordered candidates, scores and metadata.
        # Keys are digests; values contain only validated public citations.
        try:
            key = hashlib.sha256(bounded_json([*contract[:3], candidates], 262144)).hexdigest()
        except BudgetExceeded:
            self.judgment_paths['cache_key_skipped'] += 1
            return await self._judge_uncached(session, query, plan, candidates, contract)
        cached = self.judgment_cache.get('metadata', key)
        if cached is not None:
            self.judgment_paths['cache'] += 1
            return validate_decision(cached, plan, candidates)
        decision = await self._judge_uncached(session, query, plan, candidates, contract)
        decision = validate_decision(decision, plan, candidates)
        try:
            self.judgment_cache.put('metadata', key, decision)
        except BudgetExceeded:
            self.judgment_paths['cache_store_skipped'] += 1
        return decision

    async def _judge_uncached(self, session, query, plan, candidates, contract):
        from discovery_harness.vector_judge import approval_judgment, merge_decisions, option_candidates, parallel_input_bounds
        schema, instructions, prompt, decode = contract
        if not json.loads(prompt)['options_data']:
            self.judgment_paths['no_options'] += 1
            return decode({'accepted_options': []})
        session.next_turn()
        def validate(value, decoder):
            try:
                return decoder(value)
            except (ValueError, TypeError, KeyError) as error:
                raise AdapterContractError('Agent selected unsupported metadata evidence') from error
        async def call(worker, contract, input_bound=16000, output_bound=2048):
            schema, instructions, prompt, decoder = contract
            if not json.loads(prompt)['options_data']:
                return decoder({'accepted_options': []})
            if worker.busy:
                raise AdapterContractError('Cannot change an active worker contract')
            check = lambda value: validate(value, decoder)
            worker._response_contract = (schema, instructions, check)
            try:
                response = await session.llm(worker, prompt, input_token_bound=input_bound, max_output_tokens=output_bound)
                return check(response.value)
            finally:
                worker._response_contract = None
        if isinstance(self.provider, LunaPool):
            if session.policy.agent.max_queue_seconds or self.harness.active_requests+self.queued_harness.active_requests>1:
                self.judgment_paths['single']+=1
                check=lambda value:validate(value,decode)
                response=await self.shared_model_call(session,prompt,input_bound=16000,output_bound=2048,
                    contract=(schema,instructions,check))
                return check(response.value)
            # Even the lone-request parallel optimization uses the judgment
            # lane, so new intent arrivals cannot overbook the shared pool.
            spare = (self.harness.active_requests+self.queued_harness.active_requests == 1
                     and self.provider.slots.qsize() >= 2 and not self.provider.waiters)
            eligible = option_candidates(candidates, prompt) if spare else []
            if len(eligible) >= 2:
                midpoint = (len(eligible)+1)//2
                packets = (eligible[:midpoint], eligible[midpoint:])
                contracts = [approval_judgment(query, plan, packet) for packet in packets]
                bounds = parallel_input_bounds(contracts, session.policy.agent.max_input_tokens-session.usage.input_tokens)
            else:
                bounds = None
            async def shared(contract,bound=16000,output=2048):
                schema_,instructions_,prompt_,decoder=contract
                check=lambda value:validate(value,decoder)
                response=await self.shared_model_call(session,prompt_,input_bound=bound,output_bound=output,
                    contract=(schema_,instructions_,check))
                return check(response.value)
            if bounds:
                self.judgment_paths['parallel'] += 1
                jobs=[asyncio.create_task(shared(contract,bound,1024)) for contract,bound in zip(contracts,bounds)]
                try:
                    decisions=await asyncio.gather(*jobs)
                except BaseException:
                    for job in jobs:
                        if not job.done():job.cancel()
                    drain=asyncio.gather(*jobs,return_exceptions=True)
                    while not drain.done():
                        try:await asyncio.shield(drain)
                        except asyncio.CancelledError:pass
                    raise
                return merge_decisions(plan,candidates,decisions)
            self.judgment_paths['single'] += 1
            return await shared(contract)
        self.judgment_paths['single'] += 1
        return await call(self.provider, (schema, instructions, prompt, decode))

    async def discover(self, query, context=None, *, queued=False):
        check_prompt(query)
        if context is not None:check_prompt(json.dumps(context,ensure_ascii=False))
        if self.vector_tools and not self.vector_tools.configured():
            from discovery_harness.vector_tools import VectorUnavailable
            raise VectorUnavailable('Query embedding key is not configured')
        async def workflow(session):
            if self.site_workflow:
                result = await self.site_workflow.run(session, query=query, previous=context)
                result['usage'] = asdict(session.usage)
                if queued:result['scheduling']=self.session_timing(session)
                return result
            value = await self.interpret(session, query, context)
            try:
                concept = value['concept_id']
                countries = country_list(value['countries'])
                if concept not in {*self.provider.concepts, 'unknown'}:
                    raise ValueError('unknown concept')
            except (ValueError, KeyError, TypeError):
                raise AdapterContractError('Invalid discovery plan') from None
            if concept == 'unknown':
                return {'concept_id': concept, 'datasets': [], 'total': 0, 'usage': asdict(session.usage),
                        'scope': {'countries': countries, 'basis': 'provider_country', 'diversified': True,
                                  'unavailable_countries': []}}
            result = await session.tool(lambda: self.lookup(concept, countries))
            return {'concept_id': concept, 'datasets': result['datasets'], 'total': result['total'],
                    'scope': result['scope'], 'usage': asdict(session.usage)}
        return await self._run_request(workflow,queued)

    async def client(self, reader, writer):
        task = asyncio.current_task()
        self.tasks.add(task)
        admitted=self.connected<1100
        self.connected += 1
        self.peak_connected = max(self.peak_connected, self.connected)
        try:
            status, value = 503, {'code': 'connection_slots'}
            # Read the bounded GET headers before rejecting, so closing does not
            # reset a socket with unread request bytes on Windows.
            head = await asyncio.wait_for(reader.readuntil(b'\r\n\r\n'), 3)
            if admitted:
                lines = head.decode('ascii').split('\r\n')
                method, target, protocol = lines[0].split()
                route = urlsplit(target)
                async with asyncio.timeout(7264 if route.path in {'/api/jobs/discover','/api/jobs/explore'} else 65):
                    headers = [(k.lower(), v.strip()) for k, v in (line.split(':', 1) for line in lines[1:] if ':' in line)]
                    hosts = [value for key, value in headers if key == 'host']
                    allowed = len(hosts) == 1 and urlsplit('http://'+hosts[0]).hostname in {'127.0.0.1', 'localhost', '::1'}
                    if not allowed or any(key == 'origin' for key, _ in headers):
                        status, value = 403, {'code': 'local_access_only'}
                    elif method != 'GET' and not (method == 'POST' and route.path == '/api/internal/dataset-intros'):
                        status, value = 405, {'code': 'method_not_allowed'}
                    elif route.path == '/health/ready':
                        login_ready = await self.login_readiness.ready() if self.login_readiness else True
                        host_disk = self.disk_status()
                        service_ready = self.harness.healthy and self.queued_harness.healthy and self.provider.healthy
                        ready = login_ready and host_disk['ready'] and service_ready
                        provider_state = self.provider.status()
                        vector_health = await self.vector_tools.health() if self.vector_tools and host_disk['ready'] else None
                        if vector_health: ready = ready and vector_health['ready']
                        status, value = 200 if ready else 503, {'ready': ready, 'service': 'dataieum-luna',
                            'busy': provider_state['active'] > 0,
                            'state': 'authentication_required' if not login_ready else host_disk['state'] if not host_disk['ready'] else
                                     'service_recovering' if not service_ready else
                                     vector_health['state'] if vector_health else 'ready',
                            'host_disk': host_disk,
                            'authentication': self.login_readiness.status(last_model_success_at=provider_state.get('last_model_success_at')) if self.login_readiness else None}
                    elif route.path == '/health/metrics':
                        status, value = 200, self.metrics()
                    elif not gateway_authorized(headers,self.gateway_token):
                        status,value=403,{'code':'gateway_auth_required'}
                    elif self.login_readiness and not await self.login_readiness.ready():
                        status,value=503,{'code':'authentication_required'}
                    elif route.path in {'/api/internal/intro-quota','/api/internal/dataset-intros'}:
                        # Captions are now prepared in the user's Codex task and
                        # imported as reviewed text. Retired workers must never
                        # restart model generation on the serving host.
                        status, value = 410, {'code': 'intro_generation_disabled'}
                    elif route.path=='/api/related':
                        values=parse_qs(route.query,strict_parsing=True,max_num_fields=3,errors='strict')
                        if set(values)!={'context','exclude_ids','exclude_urls'} or any(len(v)!=1 for v in values.values()):raise ValueError('invalid fields')
                        context=parse_context(values['context'][0])
                        if context is None:raise ValueError('missing context')
                        excluded=json.loads(values['exclude_ids'][0])
                        async def disconnected():
                            await reader.read(1)
                            return {'type':'http.disconnect'}
                        value=await _until_disconnect(self.related(context,excluded,json.loads(values['exclude_urls'][0])),disconnected)
                        status=200
                    elif route.path in {'/api/explore','/api/jobs/explore'}:
                        values = parse_qs(route.query, strict_parsing=True, max_num_fields=3, errors='strict')
                        if set(values)-{'indicator','countries','context'} or any(len(v)!=1 for v in values.values()):raise ValueError('invalid fields')
                        indicator = values.get('indicator',[''])[0]
                        countries = country_list(json.loads(values['countries'][0])) if 'countries' in values else None
                        context = parse_context(values.get('context',[''])[0])
                        async def disconnected():
                            await reader.read(1)
                            return {'type': 'http.disconnect'}
                        value = await _until_disconnect(self.explore(indicator, countries, context,**({'queued':True} if route.path.startswith('/api/jobs/') else {})), disconnected)
                        status = 200
                    elif route.path in {'/api/discover','/api/jobs/discover'}:
                        values = parse_qs(route.query, strict_parsing=True, max_num_fields=2, errors='strict')
                        if set(values)-{'q','context'} or any(len(v)!=1 for v in values.values()):raise ValueError('invalid fields')
                        query = values.get('q', [''])[0].strip()
                        context = parse_context(values.get('context',[''])[0])
                        if not query or len(query.encode()) > 2048:
                            raise ValueError('invalid query')
                        async def disconnected():
                            await reader.read(1)
                            return {'type': 'http.disconnect'}
                        value = await _until_disconnect(self.discover(query, context,**({'queued':True} if route.path.startswith('/api/jobs/') else {})), disconnected)
                        status = 200
                    else:
                        status, value = 404, {'code': 'not_found'}
        except UnsafePrompt:
            status,value=400,{'code':'unsafe_prompt'}
        except HarnessError as error:
            self.failure_reasons[failure_reason(error)] += 1
            status = 503 if error.code in {'capacity_exceeded', 'queue_wait_timeout', 'server_overloaded', 'vector_unavailable','dependency_busy','authentication_required'} else 504 if error.code in {'deadline_exceeded','dependency_timeout'} else 502
            value = {'code': error.code}
        except CatalogHTTPError as error:
            status, value = error.status, {'code': 'catalog_http_error'}
        except (ValueError, KeyError, UnicodeError, asyncio.LimitOverrunError):
            status, value = 400, {'code': 'invalid_request'}
        except TimeoutError:
            status, value = 504, {'code': 'deadline_exceeded'}
        except (OSError, asyncio.IncompleteReadError, asyncio.CancelledError):
            status, value = 499, {'code': 'client_disconnected'}
        finally:
            try:
                body = bounded_json(value, 1048576)
                self.outcomes[str(status)] += 1
                extra = 'Retry-After: 2\r\n' if status == 503 else ''
                writer.write((f'HTTP/1.1 {status} Result\r\nContent-Type: application/json\r\nContent-Length: {len(body)}\r\n'
                              f'Cache-Control: no-store\r\nConnection: close\r\n{extra}\r\n').encode()+body)
                await writer.drain()
            except (OSError, ConnectionError):
                pass
            finally:
                self.connected -= 1
                self.tasks.discard(task)
                writer.close()
                try: await writer.wait_closed()
                except OSError: pass


def make_luna_pool(args, concepts, countries):
    """One managed-auth process; workers retain independent ephemeral threads."""
    workers = getattr(args, 'workers', 1)
    queued = getattr(args, 'queue_size', 0)
    shared = getattr(args, 'transport', 'exec') == 'shared'
    if type(workers) is not int or not 1 <= workers <= (100 if shared else 25):
        raise ValueError('Luna supports 1..100 shared workers or 1..25 exec workers')
    # Keep the existing aggregate thread budget (four processes x 75 at 100
    # workers), while only one process can refresh the managed login at a time.
    transports = [SharedLunaTransport(args.codex, args.state/'shared'/'0', idle_seconds=20,
                                     max_threads=max(75,3*workers))] if shared else []
    provider = LunaPool([AppServerLunaProvider(transports[0], args.state/'codex'/str(i), concepts, countries)
                         if shared else CodexLunaProvider(args.codex, args.state/'codex'/str(i), concepts, countries)
                         for i in range(workers)], queue_size=queued)
    return provider, transports


async def close_luna_resources(app, vector_tools, topic_lookup, transports, *, running_tasks=()):
    """Attempt every owned cleanup, including after errors or caller cancellation."""
    async def close_all():
        tasks=tuple(task for task in (*running_tasks,*getattr(app,'tasks',())) if task is not None)
        for task in tasks:task.cancel()
        if tasks:await asyncio.gather(*tasks,return_exceptions=True)
        errors=[]
        for resource in (app.harness,app.queued_harness,getattr(app,'related_harness',None),getattr(app,'intro_harness',None),app.model_stage,app.judge_stage,vector_tools,topic_lookup):
            if resource is not None:
                try:await resource.aclose()
                except BaseException as error:errors.append(error)
        outcomes=await asyncio.gather(*(t.aclose() for t in transports),return_exceptions=True)
        errors.extend(error for error in outcomes if isinstance(error,BaseException))
        if errors:raise errors[0]
    cleanup=asyncio.create_task(close_all())
    cancelled=False
    while not cleanup.done():
        try:await asyncio.shield(cleanup)
        except asyncio.CancelledError:cancelled=True
    cleanup.result()
    if cancelled:raise asyncio.CancelledError


async def serve(args):
    # Bootstrap only public concept IDs/names from the existing application.
    bootstrap = await load_bootstrap(args.catalog_url, stop_file=args.stop_file)
    concepts = {item['id']: item['name'] for item in bootstrap['concepts']}
    countries = {item['country'] for item in bootstrap['sources'] if item.get('country')}
    provider, transports = make_luna_pool(args, concepts, countries)
    for worker in provider.providers: worker.plan_sources = bootstrap['sources']
    # Leave one second of the public 20s budget for bridge IO and cancellation.
    vector_tools = None
    topic_lookup = None
    vector_config_file = args.state/'vector.json'
    if vector_config_file.exists():
        from discovery_harness.vector_tools import VectorTools
        config = json.loads(vector_config_file.read_text(encoding='utf-8-sig'))
        if set(config) != {'enabled', 'key_file'} or type(config['enabled']) is not bool:
            raise ValueError('Invalid vector tool configuration')
        if config['enabled']:
            vector_tools = VectorTools(config['key_file'], load_gateway_token(args.state/'gateway.token'))
            from discovery_harness.topic_context import TopicContextClient
            topic_lookup = TopicContextClient(catalog_url=args.catalog_url)
    app = LunaDiscovery(provider, args.catalog_url, args.state/'workspace', request_seconds=19.,
                        call_seconds=18. if transports else 12., sites=SiteRecommendations(bootstrap['sources'],catalog=bootstrap),
                        gateway_token=load_gateway_token(args.state/'gateway.token'), vector_tools=vector_tools,
                        topic_lookup=topic_lookup)
    app.instance = args.instance
    from discovery_harness.codex_login_readiness import CodexLoginReadiness
    app.login_readiness = CodexLoginReadiness(args.codex,
        failed=lambda: any(t.failures.get('authentication', 0) for t in transports))
    await app.harness.start()
    await app.queued_harness.start()
    server = await asyncio.start_server(app.client, args.host, args.port, limit=32768, backlog=2048)
    print(json.dumps({'ready': True, 'url': f'http://{args.host}:{args.port}', 'concepts': len(concepts)}), flush=True)
    async def wait_for_stop():
        while not args.stop_file.exists():
            await asyncio.sleep(.25)
    running = asyncio.create_task(server.serve_forever())
    stop = asyncio.create_task(wait_for_stop()) if args.stop_file else None
    try:
        async with server:
            if stop:
                done, _ = await asyncio.wait({running, stop}, return_when=asyncio.FIRST_COMPLETED)
                for task in done: task.result()
            else:
                await running
    finally:
        await close_luna_resources(app,vector_tools,topic_lookup,transports,running_tasks=(running,stop))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--codex', required=True, help='Native Codex executable with an existing login')
    parser.add_argument('--catalog-url', default='http://127.0.0.1:8090')
    parser.add_argument('--port', type=int, default=8091)
    parser.add_argument('--host', choices=['127.0.0.1','0.0.0.0'], default='127.0.0.1')
    parser.add_argument('--state', type=Path, required=True)
    parser.add_argument('--stop-file', type=Path, help='Owned launcher stop signal; no remote shutdown endpoint')
    parser.add_argument('--instance', help='Launch identity for idempotent local process management')
    parser.add_argument('--workers', type=int, default=1)
    parser.add_argument('--queue-size', type=int, default=0)
    parser.add_argument('--transport', choices=['exec','shared'], default='exec')
    asyncio.run(serve(parser.parse_args()))
