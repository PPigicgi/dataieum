"""ASGI adapter for the existing Dataieum catalog, without copying its logic.

Run one ASGI worker. The existing Caddy authentication and local Host boundary
remain in front of this app. Collection/classification CLIs are never started.
"""
from __future__ import annotations

import asyncio
from collections import Counter
from dataclasses import replace
import hashlib
import hmac
import json
import logging
import os
import time
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from . import CapacityMiddleware, Harness, Policy
from .branding import brand_document, request_brand
from .catalog_worker import CatalogUnavailable, CatalogWorker
from .capacity import ServerOverloaded
from .chat import ASSETS, ROUTES, FAVICON_LINKS, ChatBridge, ChatUnavailable
from .errors import BudgetExceeded, CleanupFailed, HarnessError
from .resources import bounded_json
from .security import AbusePolicy, ClientGuard, RateLimited, UnsafePrompt, check_prompt, load_gateway_token, same_origin


LOG = logging.getLogger(__name__)
ERRORS = {
    400: '검색 조건이 올바르지 않습니다.',
    403: '로컬 접근만 허용합니다.',
    404: '요청 경로를 찾을 수 없습니다.',
    413: '요청이 너무 큽니다.',
    422: '조회 범위가 제한을 초과했습니다. 조건을 좁혀 주세요.',
    429: '요청이 많습니다. 안내된 시간 후 다시 시도해 주세요.',
    503: '검색을 준비 중이거나 요청이 많습니다. 잠시 후 다시 시도해 주세요.',
    504: '조회 시간이 초과되었습니다. 조건을 좁혀 다시 시도해 주세요.',
}
SECURITY_HEADERS = [
    (b'cache-control', b'no-store'),
    (b'x-content-type-options', b'nosniff'),
    (b'referrer-policy', b'no-referrer'),
    (b'x-frame-options', b'DENY'),
    (b'content-security-policy', b"default-src 'self'; script-src 'self'; style-src 'self'; "
     b"img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"),
]
EMBED_SECURITY_HEADERS = [
    (name, b'SAMEORIGIN' if name == b'x-frame-options' else
     value.replace(b"frame-ancestors 'none'", b"frame-ancestors 'self'")
     if name == b'content-security-policy' else value)
    for name, value in SECURITY_HEADERS
]
I18N_ASSETS = (b'<script src="/i18n-data.js" defer></script>'
               b'<script src="/i18n.js" defer></script><link rel="stylesheet" href="/i18n.css">'
               b'<script src="/dataset-intros.js" defer></script><link rel="stylesheet" href="/dataset-intros.css">'
               b'<script src="/cooperation-mail.js" defer></script><link rel="stylesheet" href="/cooperation-mail.css">')


CATALOGUE_CHAT_ASSETS = (b'<link rel="stylesheet" href="/catalogue-chat.css">'
                       b'<script src="/catalogue-graph-focus.js" defer></script>'
                       b'<script src="/catalogue-chat.js" defer></script>'
                       b'<link rel="stylesheet" href="/catalogue-ui.css">'
                       b'<script src="/catalogue-ui.js" defer></script>'
                       b'<link rel="stylesheet" href="/catalogue-node-panel.css">'
                       b'<script src="/catalogue-node-panel.js" defer></script>')


async def _respond(send, status, body, mime='application/json; charset=utf-8', *, retry=False):
    headers = [(b'content-type', mime.encode()), (b'content-length', str(len(body)).encode())]
    if retry:
        headers.append((b'retry-after', b'2'))
    await send({'type': 'http.response.start', 'status': status, 'headers': headers})
    await send({'type': 'http.response.body', 'body': body})


class _Capacity(CapacityMiddleware):
    @staticmethod
    async def _reply(send, status, code, retry=False):
        # Preserve the existing browser's `data.error` error contract.
        body = bounded_json({'code': code, 'error': ERRORS.get(status, code)}, 1024)
        await _respond(send, status, body, retry=retry)


async def _file_chunks(path):
    with path.open('rb') as stream:
        while chunk := stream.read(65536):
            yield chunk
            await asyncio.sleep(0)


async def _until_disconnect(operation, receive):
    async def disconnected():
        while True:
            if (await receive())['type'] == 'http.disconnect':
                return
            await asyncio.sleep(0)
    work, watch = asyncio.create_task(operation), asyncio.create_task(disconnected())
    try:
        done, _ = await asyncio.wait({work, watch}, return_when=asyncio.FIRST_COMPLETED)
        if watch in done:
            watch.result()
            raise asyncio.CancelledError
        return work.result()
    finally:
        for task in (work, watch):
            if not task.done():
                task.cancel()
        draining = asyncio.gather(work, watch, return_exceptions=True)
        cancelled = False
        while not draining.done():
            try:
                await asyncio.shield(draining)
            except asyncio.CancelledError:
                cancelled = True
        if cancelled:
            raise asyncio.CancelledError


class DataieumApp:
    def __init__(self, harness, worker, source, database, *, prepare_timeout=180.0, chat_gateway=None, chat_max_active=2, abuse_policy=None, chat_gateway_token=None, jobs_path=None, job_workers=4, topic_graph_path=None, topic_api=None, intro_path=None):
        self.harness, self.worker = harness, worker
        self.source, self.database = Path(source), Path(database)
        self.overview_file = Path(os.environ.get('DATAIEUM_OVERVIEW_FILE') or self.database.parent / 'graph-overview.json')
        self.prepare_timeout = prepare_timeout
        self.ready = False
        self._maintenance = None
        from .http_api import create_read_api
        self._http = create_read_api(self)
        self.frontend = Path(os.environ.get("DATAIEUM_FRONTEND_DIR", "/app/frontend"))
        self._status_counts = Counter()
        self._cached_sends = 0
        self._cached_delivered = 0
        self._db_lock = asyncio.Lock()
        self._db_maintenance = False
        self._db_waiters = 0
        self._last_db_wait = 0.0
        self._db_queue_timeout = .5
        self.client_guard=ClientGuard(abuse_policy)
        self.intro_path = Path(intro_path) if intro_path is not None else None
        self._intro_reads = 0
        self.chat = ChatBridge(chat_gateway, max_active=chat_max_active, gateway_token=chat_gateway_token) if chat_gateway else None
        self.jobs=None
        self._job_owner_key=chat_gateway_token
        from .topic_api import TopicAPI
        self.topics = topic_api if topic_api is not None else TopicAPI(topic_graph_path, database)
        from .cooperation_api import CooperationAPI
        self.cooperation = CooperationAPI.from_env(self._cooperation_dataset,
            Path(harness.policy.server.lock_path).parent, self.client_guard.identity)
        if jobs_path is not None:
            if not self.chat or not chat_gateway_token:raise ValueError('job queue requires an authenticated gateway')
            if not 1<=job_workers<=1000:raise ValueError('invalid async session capacity')
            self.chat.max_queued=job_workers
            from .chat_jobs import ChatJobs
            self.jobs=ChatJobs(jobs_path,self._run_job,workers=job_workers,ready_async=self._jobs_ready,execution_timeout=7265,
                dispatcher_lock=os.environ.get('DATAIEUM_JOB_DISPATCH_LOCK'),
                dispatch_pause=os.environ.get('DATAIEUM_JOB_DISPATCH_PAUSE'))

    async def _cooperation_dataset(self, identifier):
        if not self.ready or not self.worker.alive:
            raise CatalogUnavailable('catalogue is not ready')
        async def query(session):
            return await self._database_query(session, 'cooperation_record', {'id': identifier})
        return await self.harness.run(query)

    async def _jobs_ready(self):
        if not (self.ready and self.worker.alive and self.harness.healthy and
                self.chat.queued_active<self.chat.max_queued and
                self.harness.capacity.status()['ready']):
            return False
        return await self.chat.health()

    async def _run_job(self,payload):
        from .site_search import validate_site_result
        if payload.get('kind')=='related':
            from .related_suggestions import empty_result,validate_result,url_identity,facts
            if time.time()>payload['expires_at'] or (await self.jobs.status())['pending']>1:
                return empty_result('skipped')
            async with asyncio.timeout(25):
                value=json.loads(await self.chat.related(payload,self.harness.capacity.check))
            validate_result(value)
            urls={url_identity(url) for url in payload['exclude_urls']}
            value['items']=[item for item in value['items'] if not any(
                f['dataset_id'] in payload['exclude_ids'] or url_identity(f['evidence_url']) in urls for f in facts(item['result']))]
            if not value['items'] and value['status']=='results':value['status']='empty'
            return validate_result(value)
        value=json.loads(await self.chat.queued(payload,self.harness.capacity.check,
                                              response_check=self.harness.capacity.check_response))
        return validate_site_result(value)

    async def _jobs_api(self,scope,receive,send):
        from .chat_jobs import JobError
        from .display_text import clean_response
        from .site_search import parse_context,intent_prompt,node_plan
        if not same_origin(scope) or [v for k,v in scope.get('headers',[]) if k.lower()==b'x-dataieum-chat']!=[b'1']:
            return await _Capacity._reply(send,403,'local_access_only')
        if self.jobs is None:return await _Capacity._reply(send,503,'jobs_unavailable',retry=True)
        path,method=scope['path'],scope['method']
        if scope.get('query_string'):return await _Capacity._reply(send,400,'invalid_query')
        try:
            if path in {'/api/chat/jobs','/api/chat/related'} and method=='POST':
                types=[v for k,v in scope.get('headers',[]) if k.lower()==b'content-type']
                if types!=[b'application/json']:return await _Capacity._reply(send,415,'json_required')
                body=bytearray()
                async with asyncio.timeout(5):
                    while True:
                        message=await receive()
                        if message['type']=='http.disconnect':return
                        body.extend(message.get('body',b''))
                        if len(body)>32768:return await _Capacity._reply(send,413,'request_too_large')
                        if not message.get('more_body'):break
                def unique(pairs):
                    result=dict(pairs)
                    if len(result)!=len(pairs):raise ValueError('duplicate field')
                    return result
                data=json.loads(body,object_pairs_hook=unique)
                if path=='/api/chat/related':
                    if not isinstance(data,dict) or set(data)!={'parent_job_id'}:raise ValueError('invalid fields')
                    tokens=[v for k,v in scope.get('headers',[]) if k.lower()==b'x-dataieum-job-token']
                    token=tokens[0].decode('ascii') if len(tokens)==1 else ''
                    owner=hmac.new(self._job_owner_key.encode(),self.client_guard.identity(scope).encode(),'sha256').hexdigest()
                    child=hmac.new(self._job_owner_key.encode(),('related-v1:'+token).encode(),'sha256').hexdigest()
                    value=await self.jobs.submit_related(data['parent_job_id'],token,owner,child)
                    return await _respond(send,202,bounded_json(clean_response({**value,'wait_supported':True}),1048576+8192))
                if not isinstance(data,dict) or set(data)-{'query','context','indicator','request_token'}:raise ValueError('invalid fields')
                query=data.get('query')
                if not isinstance(query,str):raise ValueError('invalid query')
                context=parse_context(json.dumps(data['context'],ensure_ascii=False)) if data.get('context') is not None else None
                intent_prompt(query,context)
                check_prompt(query)
                if context:check_prompt(json.dumps(context,ensure_ascii=False))
                payload={'query':query.strip(),'context':context}
                if 'indicator' in data:
                    node_plan(data['indicator'],None,context);payload['indicator']=data['indicator']
                owner=hmac.new(self._job_owner_key.encode(),self.client_guard.identity(scope).encode(),'sha256').hexdigest()
                value=await self.jobs.submit(payload,owner,data.get('request_token'))
                return await _respond(send,202,bounded_json(clean_response({**value,'wait_supported':True}),1048576+8192))
            if path.startswith('/api/chat/jobs/') and method in {'GET','DELETE'}:
                job_id=path.removeprefix('/api/chat/jobs/')
                tokens=[v for k,v in scope.get('headers',[]) if k.lower()==b'x-dataieum-job-token']
                token=tokens[0].decode('ascii') if len(tokens)==1 else ''
                waits=[v for k,v in scope.get('headers',[]) if k.lower()==b'x-dataieum-job-state']
                if waits and (method!='GET' or len(waits)!=1 or waits[0] not in {b'queued',b'running'}):
                    raise ValueError('invalid job wait state')
                if method=='GET' and waits:
                    value=await _until_disconnect(self.jobs.wait(job_id,token,waits[0].decode('ascii')),receive)
                else:
                    value=await (self.jobs.get(job_id,token) if method=='GET' else self.jobs.cancel(job_id,token))
                return await _respond(send,200,bounded_json(clean_response({**value,'wait_supported':True}),1048576+8192))
            return await _Capacity._reply(send,404,'not_found')
        except JobError as error:
            return await _Capacity._reply(send,error.status,error.code,retry=error.status in {429,503})
        except UnsafePrompt:
            return await _Capacity._reply(send,400,'unsafe_prompt')
        except (ValueError,UnicodeError,TypeError,RecursionError):
            return await _Capacity._reply(send,400,'invalid_query')
        except TimeoutError:
            return await _Capacity._reply(send,408,'request_timeout')

    async def _maintain(self):
        while True:
            try:
                if self.worker.busy or self._db_lock.locked() or self.harness.active_requests:
                    await asyncio.sleep(1)
                    continue
                self.harness.capacity.check()
                if not self.ready or not self.worker.alive:
                    self.ready = False
                    # Admission stays closed while preparing. Once admitted,
                    # preparation may finish its own CPU work within the fixed
                    # deadline, memory/disk checks and container resource caps.
                    await self.worker.call('prepare', timeout=self.prepare_timeout,
                                           check=self.harness.capacity.check_response)
                    self.ready = True
                else:
                    # Low-priority metadata work must not consume a foreground
                    # request slot. Foreground DB checkout waits for this short
                    # operation before acquiring its own database lease.
                    async with self._db_lock:
                        self._db_maintenance = True
                        lease = None
                        try:
                            lease = self.harness.capacity.acquire_operation('db')
                            changed = await self.worker.call('changed', timeout=2.0,
                                                            check=self.harness.capacity.check)
                        finally:
                            if lease is not None:
                                lease.release()
                            self._db_maintenance = False
                    if changed:
                        self.ready = False
                        continue
            except CleanupFailed:
                self.harness.capacity.poison('catalog_cleanup_failed')
                self.ready = False
                LOG.error('catalog cleanup failed; restart requires operator review')
                return
            except BudgetExceeded as error:
                self.ready = False
                if error.resource == 'catalog_index_bytes':
                    self.harness.capacity.poison('catalog_index_too_large')
                    LOG.error('catalog index exceeds its disk budget; preparation stopped')
                    return
                LOG.warning('catalog maintenance budget exceeded: %s', error.resource)
            except HarnessError as error:
                if not self.worker.alive:
                    self.ready = False
                LOG.warning('catalog maintenance deferred: %s (%s)', error.code,
                            getattr(error, 'reason', error.code))
            await asyncio.sleep(10)

    async def _lifespan(self, receive, send):
        while True:
            message = await receive()
            if message['type'] == 'lifespan.startup':
                try:
                    from .observability import initialize
                    initialize(Path(self.harness.policy.server.lock_path).parent/'requests.jsonl')
                    await self.harness.start()
                    if self.jobs:await self.jobs.start()
                    await self.cooperation.start()
                    self._maintenance = asyncio.create_task(self._maintain())
                except Exception:
                    await send({'type': 'lifespan.startup.failed', 'message': 'harness startup failed'})
                    return
                await send({'type': 'lifespan.startup.complete'})
            elif message['type'] == 'lifespan.shutdown':
                try:
                    if self.jobs:await self.jobs.aclose()
                    await self.cooperation.aclose()
                    await self.response_cache.close()
                    await self.topics.aclose()
                    if self._maintenance is not None:
                        self._maintenance.cancel()
                        try:
                            await self._maintenance
                        except asyncio.CancelledError:
                            pass
                    await self.worker.aclose()
                    await self.harness.aclose()
                    from .observability import shutdown
                    shutdown()
                except Exception:
                    await send({'type': 'lifespan.shutdown.failed', 'message': 'harness cleanup failed'})
                    return
                await send({'type': 'lifespan.shutdown.complete'})
                return

    async def __call__(self, scope, receive, send):
        lease = None
        try:
            if scope['type']=='http' and (scope.get('path','').startswith('/api/') or scope.get('path')=='/health/metrics') and scope.get('path')!='/api/health':
                lease=self.client_guard.acquire(scope,chat=scope['path'] in {'/api/chat','/api/ontology/sites'} or
                    (scope['path'] in {'/api/chat/jobs','/api/chat/related'} and scope.get('method')=='POST'))
        except RateLimited as error:
            self._status_counts[error.status]+=1
            body=bounded_json({'code':error.code,'error':ERRORS[error.status],'retry_after':error.retry_after},1024)
            await send({'type':'http.response.start','status':error.status,'headers':SECURITY_HEADERS+[
                (b'content-type',b'application/json; charset=utf-8'),(b'content-length',str(len(body)).encode()),
                (b'retry-after',str(error.retry_after).encode())]})
            await send({'type':'http.response.body','body':body})
            return
        except (ValueError,UnicodeError):
            # Invalid peer/proxy metadata fails closed before any model work.
            async def secure_send(message):
                if message['type']=='http.response.start':message={**message,'headers':message.get('headers',[])+SECURITY_HEADERS}
                await send(message)
            return await _Capacity._reply(secure_send,400,'invalid_request')
        try:
            return await self._handle(scope,receive,send)
        finally:
            self.client_guard.release(lease)

    async def _handle(self, scope, receive, send):
        if scope['type'] == 'lifespan':
            return await self._lifespan(receive, send)
        if scope['type'] != 'http':
            if scope['type'] == 'websocket':
                await send({'type': 'websocket.close', 'code': 1008})
            return

        async def secure_send(message):
            if message['type'] == 'http.response.start':
                self._status_counts[message['status']] += 1
                # Only the dedicated successful document may be framed by our
                # catalogue. API responses, failures and standalone pages stay denied.
                security = (EMBED_SECURITY_HEADERS if scope.get('path') == '/chat/embed'
                            and scope.get('method') == 'GET' and message['status'] == 200
                            else SECURITY_HEADERS)
                if scope.get('path','').startswith('/assets/') and message['status'] in {200,304}:
                    security=[(k,v) for k,v in security if k!=b'cache-control']
                message = {**message, 'headers': list(message.get('headers', [])) + security}
            await send(message)

        hosts = [v for k, v in scope.get('headers', []) if k.lower() == b'host']
        try:
            host = urlsplit('http://' + hosts[0].decode('ascii')).hostname if len(hosts) == 1 else None
        except (ValueError, UnicodeError):
            host = None
        if host not in {'localhost', '127.0.0.1', '::1'}:
            return await _Capacity._reply(secure_send, 403, 'local_access_only')
        try:
            brand = request_brand(scope)
        except ValueError:
            return await _Capacity._reply(secure_send, 400, 'invalid_presentation_host')
        scope={**scope,'dataieum_brand':brand.key}
        path, method = scope['path'], scope['method']
        if path in {'/api/chat/jobs','/api/chat/related'} or path.startswith('/api/chat/jobs/'):
            return await self._jobs_api(scope,receive,secure_send)
        if path.startswith('/api/account/') or path.startswith('/api/cooperation/'):
            return await self.cooperation(scope, receive, secure_send)
        if method != 'GET':
            return await _Capacity._reply(secure_send, 404, 'not_found')
        if path in {'/api/health', '/health/live'}:
            return await _respond(secure_send, 200, b'{"ok":true}')
        if path == '/health/ready':
            ready = (self.ready and self.worker.alive and self.harness.healthy
                     and (self.jobs is None or self.jobs.healthy)
                     and self.harness.capacity.status()['ready'])
            return await _respond(secure_send, 200 if ready else 503,
                                  b'{"ready":true}' if ready else b'{"ready":false}')
        if path == '/health/metrics':
            return await _respond(secure_send, 200, bounded_json({**self.metrics(),'jobs':await self.jobs.status() if self.jobs else None, 'cooperation':await self.cooperation.metrics()},16384))
        if (path in {'/', '/catalogue'} or path.startswith('/assets/')) and (self.frontend / 'index.html').is_file():
            from .frontend_assets import serve
            return await serve(scope, receive, secure_send, self.frontend, brand)
        if path in ROUTES:
            name, mime = ROUTES[path]
            body = (ASSETS / name).read_bytes()
            if mime == 'text/html':
                body = body.replace(b'<head>', b'<head>' + I18N_ASSETS, 1)
                body = body.replace(b'</head>', FAVICON_LINKS + b'</head>', 1)
                body = brand_document(body, brand)
            if path == '/chat/embed':
                body = body.replace(b'<body>', b'<body data-embedded-chat="true">', 1)
            return await _respond(secure_send, 200, body, mime + '; charset=utf-8' if mime.startswith('text/') else mime)
        if path in {'/api/ontology/topics', '/api/ontology/dataset-topics', '/api/ontology/unclassified'} or path.startswith('/api/ontology/topics/'):
            try:
                self.harness.capacity.check()
                if not self.harness.healthy or self._cached_sends >= 8:
                    raise ServerOverloaded('topic_response_slots')
            except (ServerOverloaded, OSError):
                return await _Capacity._reply(secure_send, 503, 'topic_busy', retry=True)
            self._cached_sends += 1
            try:
                status, value = await _until_disconnect(self.topics.query(path, scope.get('query_string', b'')), receive)
                body = bounded_json(value, min(262144, self.harness.policy.server.max_response_bytes))
                async with asyncio.timeout(3):
                    return await _respond(secure_send, status, body, retry=status == 503)
            except BudgetExceeded:
                return await _Capacity._reply(secure_send, 503, 'topic_response_bytes', retry=True)
            except TimeoutError:
                return
            finally:
                self._cached_sends -= 1
        if path == '/api/ontology/hierarchy':
            # Immutable, validated <=256KiB dictionary; no DB/model calls.
            if scope.get('query_string', b''):
                return await _Capacity._reply(secure_send, 400, 'invalid_query')
            from .concept_hierarchy import snapshot_bytes
            try:
                self.harness.capacity.check()
                if not self.harness.healthy or self._cached_sends >= 8:
                    raise ServerOverloaded('hierarchy_response_slots')
                body = snapshot_bytes()
                if len(body) > self.harness.policy.server.max_response_bytes:
                    raise ServerOverloaded('hierarchy_response_bytes')
            except ServerOverloaded:
                return await _Capacity._reply(secure_send, 503, 'hierarchy_busy', retry=True)
            self._cached_sends += 1
            try:
                async with asyncio.timeout(3):
                    return await _respond(secure_send, 200, body)
            except TimeoutError:
                return
            finally:
                self._cached_sends -= 1
        if path == '/api/chat/status':
            state = await self.chat.health() if self.chat else {'ready': False, 'state': 'offline'}
            if self.jobs is not None and not self.jobs.healthy:
                state = {'ready': False, 'state': 'service_recovering'}
            if state['ready'] and not (self.ready and self.worker.alive):
                state = {'ready': False, 'state': 'catalog_preparing'}
            if state['ready']:
                capacity = self.harness.capacity.status()
                if not capacity['ready']:
                    state = {'ready': False, 'state': 'service_recovering'}
            return await _respond(secure_send, 200, bounded_json(state, 1024))
        if path == '/api/dataset-intros':
            # Read prepared content even when Luna is offline. Browsing cannot
            # enqueue work, spend model quota, or acquire chat/vector slots.
            if not same_origin(scope) or [v for k,v in scope.get('headers',[]) if k.lower()==b'x-dataieum-chat'] != [b'1']:
                return await _Capacity._reply(secure_send, 403, 'local_access_only')
            try:
                raw = scope.get('query_string', b'')
                if len(raw) > 8192: raise ValueError('query too large')
                values = parse_qs(raw.decode('utf-8'), strict_parsing=True, max_num_fields=1, errors='strict')
                if set(values) != {'ids'} or len(values['ids']) != 1: raise ValueError('invalid fields')
                from .dataset_intro import validate_ids
                from .intro_store import missing
                from .region_store import lookup
                ids = validate_ids(json.loads(values['ids'][0]))
                if self._intro_reads >= 8:
                    result = {'items': [missing(i) for i in ids]}
                else:
                    self._intro_reads += 1
                    task = asyncio.create_task(asyncio.to_thread(lookup, self.intro_path, self.database, ids))
                    try:
                        result = await asyncio.shield(task)
                    finally:
                        while not task.done():
                            try: await asyncio.shield(task)
                            except asyncio.CancelledError: pass
                        self._intro_reads -= 1
                return await _respond(secure_send, 200, bounded_json(result, 65536))
            except (ValueError, UnicodeError):
                return await _Capacity._reply(secure_send, 400, 'invalid_query')
        if path in {'/api/chat', '/api/ontology/sites'}:
            # A same-origin fetch adds this header; cross-origin pages cannot
            # spend model calls through blind image/navigation GET requests.
            if not same_origin(scope) or [v for k, v in scope.get('headers', []) if k.lower() == b'x-dataieum-chat'] != [b'1']:
                return await _Capacity._reply(secure_send, 403, 'local_access_only')
            if self.chat is None:
                return await _Capacity._reply(secure_send, 503, 'chat_unavailable', retry=True)
            try:
                # Admit against all limits, then let this bounded request finish
                # when its own work raises CPU use. Memory/disk/deadlines still apply.
                self.harness.capacity.check()
                raw = scope.get('query_string', b'')
                if len(raw) > 24576:
                    raise ValueError('query too large')
                values = parse_qs(raw.decode('utf-8'), strict_parsing=True, max_num_fields=4, errors='strict')
                allowed = {'indicator','countries','context'} if path == '/api/ontology/sites' else {'q','context','selected','countries'}
                if set(values)-allowed or any(len(v)!=1 for v in values.values()):raise ValueError('invalid fields')
                if path == '/api/ontology/sites':
                    from .site_search import node_plan, parse_context
                    indicator = values.get('indicator',[''])[0]
                    countries = json.loads(values['countries'][0]) if 'countries' in values else None
                    context = parse_context(values.get('context',[''])[0])
                    node_plan(indicator, countries, context)
                    call = self.chat.explore(indicator, countries, self.harness.capacity.check_response, context)
                else:
                    query = values.get('q', [''])[0].strip()
                    from .site_search import parse_context, intent_prompt, node_plan, selection_plan
                    context = parse_context(values.get('context',[''])[0])
                    if 'selected' in values:
                        if context is not None and 'countries' in values:
                            raise ValueError('duplicate scope inputs')
                        countries=json.loads(values['countries'][0]) if 'countries' in values else None
                        base=context if context is not None else node_plan('population',countries)
                        context=selection_plan(base,values['selected'][0])
                    elif 'countries' in values:
                        raise ValueError('countries require a selected concept')
                    intent_prompt(query, context)
                    call = self.chat.query(query, self.harness.capacity.check_response, context) if context else self.chat.query(query, self.harness.capacity.check_response)
                body = await _until_disconnect(call, receive)
                from .display_text import clean_response
                body = bounded_json(clean_response(json.loads(body)),1048576)
                return await _respond(secure_send, 200, body)
            except UnsafePrompt:
                self.client_guard.rejected['unsafe_prompt']+=1
                return await _respond(secure_send,400,bounded_json({'code':'unsafe_prompt','error':'검색과 관계없는 지시가 포함되어 있어요. 필요한 데이터와 조건만 입력해 주세요.'},1024))
            except (ValueError, UnicodeError):
                return await _Capacity._reply(secure_send, 400, 'invalid_query')
            except ChatUnavailable as error:
                return await _Capacity._reply(secure_send, error.status, error.code, retry=error.status == 503)
            except ServerOverloaded as error:
                self.harness.capacity.record_rejection(error.reason)
                code = 'chat_busy' if error.reason.endswith('_slots') else 'chat_recovering'
                return await _Capacity._reply(secure_send, 503, code, retry=True)
            except asyncio.CancelledError:
                return
        static = {'/': ('index.html', 'text/html'), '/catalogue': ('index.html', 'text/html'), '/app.js': ('app.js', 'text/javascript'),
                  '/style.css': ('style.css', 'text/css')}
        if path in static:
            name, mime = static[path]
            try:
                # Static UI is bounded separately and remains usable under load.
                with (self.source / 'static' / name).open('rb') as stream:
                    body = stream.read(2 * 1024**2 + 1)
                if len(body) > 2 * 1024**2:
                    return await _Capacity._reply(secure_send, 503, 'static_asset_too_large')
            except OSError:
                return await _Capacity._reply(secure_send, 404, 'not_found')
            if path in {'/', '/catalogue'}:
                body = body.replace(b'<head>', b'<head>' + I18N_ASSETS + b'<script src="/catalogue-topic-layout.js" defer></script>', 1)
                body = body.replace(b'</head>', CATALOGUE_CHAT_ASSETS + FAVICON_LINKS + b'</head>', 1)
            return await _respond(secure_send, 200, body, mime + '; charset=utf-8')
        return await self._http(scope, receive, secure_send)

    def metrics(self):
        from .observability import status
        return {'observability':status(),'responses':self.response_cache.status(),'topics': self.topics.status(), 'security':self.client_guard.status(),'capacity': self.harness.capacity.status(), 'cache': self.harness.cache.status(),
                'database': {'max_active': 1, 'waiters': self._db_waiters,
                             'max_wait_seconds': self._db_queue_timeout,
                             'last_wait_seconds': round(self._last_db_wait, 4),
                             'worker': self.worker.diagnostics() if self.worker else None},
                'chat': {'configured': self.chat is not None, 'active': self.chat.active if self.chat else 0,
                         'max_active': self.chat.max_active if self.chat else 0,
                         'queued_active':self.chat.queued_active if self.chat else 0,
                         'max_queued':self.chat.max_queued if self.chat else 0,
                         'connections':self.chat.connection_status() if self.chat else None},
                'cached_responses': {'active': self._cached_sends, 'max_active': 8,
                                     'delivered': self._cached_delivered,
                                     'max_retained_bytes': 8 * 262144},
                'http_status': {str(k): v for k, v in self._status_counts.items()},
                'worker': {'ready': self.ready, 'alive': self.worker.alive,
                           'busy': self.worker.busy, 'pid': self.worker.pid}}

    def _graph_budget(self, graph):
        nodes, edges = graph.get('nodes', []), graph.get('edges', [])
        for resource, used, limit in (
            ('nodes', len(nodes), self.harness.policy.ontology.max_nodes),
            ('edges', len(edges), self.harness.policy.ontology.max_edges),
            ('datasets', sum(n.get('kind') in {'dataset','catalog_record'} for n in nodes),
             self.harness.policy.ontology.max_datasets),
        ):
            if used > limit:
                raise BudgetExceeded(resource, limit)

    async def _database_query(self, session, operation, query):
        # Admission already bounds foreground requests to two; no new backlog.
        # Cancellation before checkout never touches another request's worker.
        import time
        if self._db_lock.locked() and not self._db_maintenance:
            raise ServerOverloaded('db_slots')
        started = time.monotonic()
        self._db_waiters += 1
        try:
            try:
                async with asyncio.timeout(min(self._db_queue_timeout, session.remaining_seconds)):
                    await self._db_lock.acquire()
            except TimeoutError:
                raise ServerOverloaded('db_wait_timeout') from None
        finally:
            self._db_waiters -= 1
            self._last_db_wait = time.monotonic() - started
        try:
            return await session.database(lambda: self.worker.call(operation, query,
                timeout=min(session.remaining_seconds, self.harness.policy.server.db_timeout_seconds),
                check=self.harness.capacity.check_response))
        finally:
            self._db_lock.release()

    def _cache_key(self, path, query):
        generation = [self.worker.pid, 'topics_v1' if os.environ.get('DATAIEUM_TOPIC_VIEW') == '1' else 'bands_v1']
        artifacts=[self.database,self.database.with_name(self.database.name+'-wal'),self.overview_file]
        artifacts.extend(Path(os.environ[key]) for key in ('DATAIEUM_KEYWORD_INDEX','DATAIEUM_CONFIDENCE_GRAPH','DATAIEUM_PUBLICATION_FILE') if os.environ.get(key))
        for item in artifacts:
            try:
                stat = item.stat()
                generation.append((stat.st_ino, stat.st_size, stat.st_mtime_ns))
            except FileNotFoundError:
                generation.append(None)
        return hashlib.sha256(json.dumps([generation, path, query], sort_keys=True).encode()).hexdigest()


def create_app():
    """Uvicorn --factory entrypoint, loaded inside the existing application image."""
    source = Path(os.environ.get('DATAIEUM_SOURCE_DIR', '/app')).resolve(strict=True)
    database = Path(os.environ['DB_PATH']).resolve(strict=True)
    policy = Policy.load(os.environ.get('HARNESS_POLICY_FILE', '/harness-config/policy.json'))
    if policy.server.catalog_index_max_mib * 2 > policy.server.task_ephemeral_mib * policy.server.disk_high:
        raise ValueError('catalog index rebuild must fit the disk budget')
    control = Path(policy.server.lock_path).parent.resolve(strict=True)
    workspace = Path(os.environ.get('HARNESS_WORKSPACE', '/harness-workspace')).resolve(strict=True)
    disk = Path(policy.server.disk_path).resolve(strict=True)
    for path in (control, workspace):
        if path == database.parent or path in database.parents or database.parent in path.parents:
            raise ValueError('harness state must be separate from persistent catalog data')
    if control == workspace or control in workspace.parents or workspace in control.parents:
        raise ValueError('harness control and workspace directories must be separate')
    if not (database.is_file() and (source / 'catalog.py').is_file() and ((source / 'static').is_dir() or Path(os.environ.get('DATAIEUM_FRONTEND_DIR','/app/frontend'),'index.html').is_file())):
        raise ValueError('existing catalog database, module and frontend build are required')
    if len({path.stat().st_dev for path in (database, disk, workspace, control)}) != 1:
        raise ValueError('catalog, disk telemetry and harness state must share a filesystem')
    harness = Harness(policy, workspace_root=workspace)
    worker = CatalogWorker(source, database, result_limit=policy.agent.max_result_bytes,
                           index_directory=control / 'catalog-index',
                           index_max_bytes=policy.server.catalog_index_max_mib * 1024**2)
    return DataieumApp(harness, worker, source, database,
                       intro_path=Path(os.environ.get('DATAIEUM_INTRO_DB', str(control/'dataset-intros.sqlite3'))),
                       prepare_timeout=policy.server.catalog_prepare_timeout_seconds,
                       chat_gateway=os.environ.get('DATAIEUM_CHAT_GATEWAY'),
                       chat_max_active=int(os.environ.get('DATAIEUM_CHAT_MAX_ACTIVE', '2')),
                       abuse_policy=AbusePolicy.from_env(),
                       jobs_path=Path(os.environ.get('DATAIEUM_JOBS_PATH', str(control/'chat-jobs.sqlite3'))) if os.environ.get('DATAIEUM_CHAT_JOBS')=='1' else None,
                       job_workers=int(os.environ.get('DATAIEUM_CHAT_JOB_WORKERS','4')),
                       topic_graph_path=os.environ.get('DATAIEUM_TOPIC_GRAPH'),
                       chat_gateway_token=load_gateway_token(os.environ['DATAIEUM_CHAT_GATEWAY_TOKEN_FILE']) if os.environ.get('DATAIEUM_CHAT_GATEWAY') else None)
