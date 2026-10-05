"""Bounded same-origin bridge to the optional local, Codex-authenticated gateway."""
import asyncio
import errno
import json
import time
from collections import Counter
from pathlib import Path
from urllib.parse import urlencode, urlsplit
from .catalog_discovery import country_list
from .site_search import validate_site_result
from .capacity import ServerOverloaded


ASSETS = Path(__file__).with_name('web')
FAVICON_LINKS = (b'<link rel="icon" type="image/x-icon" sizes="16x16 32x32 48x48" href="/favicon.ico?v=di-1">'
                 b'<link rel="icon" type="image/png" sizes="32x32" href="/favicon-32.png?v=di-1">')
ROUTES = {'/favicon.ico': ('favicon.ico', 'image/x-icon'),
          '/cooperation-mail.js': ('cooperation-mail.js', 'text/javascript'),
          '/cooperation-mail.css': ('cooperation-mail.css', 'text/css'),
          '/i18n-data.js': ('i18n-data.js', 'text/javascript'),
          '/i18n.js': ('i18n.js', 'text/javascript'),
          '/i18n.css': ('i18n.css', 'text/css'),
          '/favicon-32.png': ('favicon-32.png', 'image/png'),
          '/chat': ('chat.html', 'text/html'),
          '/chat/embed': ('chat.html', 'text/html'),
          '/catalogue-chat.js': ('catalogue-chat.js', 'text/javascript'),
          '/catalogue-chat.css': ('catalogue-chat.css', 'text/css'),
          '/catalogue-ui.js': ('catalogue-ui.js', 'text/javascript'),
          '/catalogue-ui.css': ('catalogue-ui.css', 'text/css'),
          '/catalogue-topic-layout.js': ('catalogue-topic-layout.js', 'text/javascript'),
          '/catalogue-node-panel.js': ('catalogue-node-panel.js', 'text/javascript'),
          '/catalogue-node-panel.css': ('catalogue-node-panel.css', 'text/css'),
          '/catalogue-focus.js': ('catalogue-focus.js', 'text/javascript'),
          '/catalogue-focus.css': ('catalogue-focus.css', 'text/css'),
          '/catalogue-graph-focus.js': ('catalogue-graph-focus.js', 'text/javascript'),
          '/ontology': ('ontology.html', 'text/html'),
          '/ontology-explorer.js': ('ontology-explorer.js', 'text/javascript'),
          '/ontology.css': ('ontology.css', 'text/css'),
          '/ontology-view.js': ('ontology-view.js', 'text/javascript'),
          '/site-results.js': ('site-results.js', 'text/javascript'),
          '/search-progress.js': ('search-progress.js', 'text/javascript'),
          '/concept-map.js': ('concept-map.js', 'text/javascript'),
          '/concept-map.css': ('concept-map.css', 'text/css'),
          '/hierarchy-map.js': ('hierarchy-map.js', 'text/javascript'),
          '/hierarchy-map.css': ('hierarchy-map.css', 'text/css'),
          '/chat.js': ('chat.js', 'text/javascript'),
          '/chat.css': ('chat.css', 'text/css')}
ROUTES.update({'/topic-graph.js': ('topic-graph.js', 'text/javascript'),
               '/topic-graph.css': ('topic-graph.css', 'text/css'),
               '/dataset-intros.js': ('dataset-intros.js', 'text/javascript'),
               '/dataset-intros.css': ('dataset-intros.css', 'text/css')})


class ChatUnavailable(Exception):
    def __init__(self, status=503, code='chat_unavailable'):
        self.status, self.code = status, code


class ChatBridge:
    def __init__(self, url, *, max_active=2, gateway_token=None):
        if type(max_active) is not int or not 1 <= max_active <= 25:
            raise ValueError('chat bridge supports 1..25 active requests')
        target = urlsplit(url)
        if (target.scheme != 'http' or target.hostname not in
                {'host.docker.internal', '127.0.0.1', 'localhost', '::1', 'luna'} or
                target.username or target.password or target.path not in {'', '/'} or
                target.query or target.fragment):
            raise ValueError('chat gateway must be an explicit local HTTP endpoint')
        self.host, self.port = target.hostname, target.port or 80
        if gateway_token is not None and (len(gateway_token)!=64 or any(c not in '0123456789abcdef' for c in gateway_token)):
            raise ValueError('invalid gateway token')
        self.gateway_token=gateway_token
        self.active = 0
        self.queued_active = 0
        self.reserved_jobs = 0
        self.max_queued = 0
        self.max_active = max_active
        self._health = None
        self._health_at = 0
        self._checking = False
        self._connect_slots = asyncio.Semaphore(16)
        self._connecting = 0
        self._connect_waiters = 0
        self._connection_stats = Counter()
        self._upstream_slots = asyncio.Semaphore(128)
        self._upstream_active = 0
        self._upstream_waiters = 0

    def connection_status(self):
        return {'connecting': self._connecting, 'waiting': self._connect_waiters,
                'limit': 16, 'upstream_active': self._upstream_active,
                'upstream_waiting': self._upstream_waiters, 'upstream_limit': 128,
                'counters': dict(self._connection_stats)}

    def _connection_error(self, phase, error):
        # Never retain exception text: it can contain request paths or credentials.
        self._connection_stats[f'{phase}_{type(error).__name__}_{error.errno}'] += 1

    async def _connect(self, queued):
        # Only DNS/TCP establishment is gated. Open conversations remain independent.
        # Retry here only: after this returns, no request bytes may be retransmitted.
        attempts = 8 if queued else 1
        for attempt in range(attempts):
            waiting = time.monotonic()
            self._connect_waiters += 1
            try:
                await self._connect_slots.acquire()
            finally:
                self._connect_waiters -= 1
                self._connection_stats['wait_ms'] += round((time.monotonic()-waiting)*1000)
            started = time.monotonic()
            self._connecting += 1
            self._connection_stats['peak_connecting'] = max(self._connection_stats['peak_connecting'], self._connecting)
            try:
                self._connection_stats['attempts'] += 1
                async with asyncio.timeout(2):
                    result = await asyncio.open_connection(self.host, self.port, limit=8192)
                self._connection_stats['connected'] += 1
                return result
            except OSError as error:
                self._connection_error('connect', error)
                if (attempt+1 == attempts or not isinstance(error, TimeoutError) and error.errno not in
                        {errno.ECONNREFUSED, errno.ECONNRESET, errno.ETIMEDOUT}):
                    raise
            finally:
                self._connection_stats['execute_ms'] += round((time.monotonic()-started)*1000)
                self._connecting -= 1
                self._connect_slots.release()
            self._connection_stats['retries'] += 1
            await asyncio.sleep(min(2, .1*2**attempt))

    async def health(self):
        if self._health is not None and time.monotonic() - self._health_at < 2:
            return dict(self._health)
        if self._checking:
            return {'ready': False, 'state': 'checking'}
        self._checking = True
        checked_at = time.monotonic()
        writer = None
        value = {'ready': False, 'state': 'offline'}
        try:
            async with asyncio.timeout(2):
                reader, writer = await asyncio.open_connection(self.host, self.port, limit=4096)
                writer.write(b'GET /health/ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n')
                await writer.drain()
                head = (await reader.readuntil(b'\r\n\r\n')).decode('ascii').split('\r\n')
                status = int(head[0].split()[1])
                headers = {k.lower(): v.strip() for k, v in
                           (line.split(':', 1) for line in head[1:] if ':' in line)}
                size = int(headers['content-length'])
                if not 0 <= size <= 4096 or 'transfer-encoding' in headers:
                    raise ValueError('unbounded health response')
                data = json.loads(await reader.readexactly(size))
                if isinstance(data, dict) and data.get('service') == 'dataieum-luna':
                    ready = status == 200 and data.get('ready') is True
                    state = data.get('state')
                    value = {'ready': ready, 'state': 'busy' if ready and data.get('busy') else
                             'ready' if ready else state if state in {'embedding_not_configured', 'vector_preparing', 'vector_unavailable', 'authentication_required'} else 'unavailable'}
        except (OSError, TimeoutError, ValueError, KeyError, UnicodeError,
                asyncio.IncompleteReadError, asyncio.LimitOverrunError):
            pass
        finally:
            try:
                if writer:
                    writer.close()
                    try: await asyncio.wait_for(writer.wait_closed(), .5)
                    except (OSError, TimeoutError): pass
            finally:
                self._checking = False
        if self._health_at > checked_at:
            return dict(self._health)
        self._health, self._health_at = value, time.monotonic()
        return dict(value)

    async def query(self, query, check, context=None):
        params = {'q': query}
        if context is not None: params['context'] = json.dumps(context,ensure_ascii=False,separators=(',',':'))
        return await self._request('/api/discover?' + urlencode(params), check)

    async def explore(self, indicator, countries, check, context=None):
        params = {'indicator': indicator}
        if countries is not None: params['countries'] = json.dumps(countries, ensure_ascii=False)
        if context is not None: params['context'] = json.dumps(context, ensure_ascii=False, separators=(',',':'))
        return await self._request('/api/explore?' + urlencode(params), check)

    async def queued(self, payload, check, *, response_check=None):
        params={'indicator':payload['indicator']} if payload.get('indicator') else {'q':payload['query']}
        if payload.get('context') is not None:params['context']=json.dumps(payload['context'],ensure_ascii=False,separators=(',',':'))
        route='/api/jobs/explore?' if payload.get('indicator') else '/api/jobs/discover?'
        return await self._request(route+urlencode(params),check,timeout_seconds=7262 if self.max_queued else 62,
                                   response_check=response_check)

    async def introductions(self, ids, check):
        from .dataset_intro import validate_ids, validate_rows
        validate_ids(ids)
        return await self._request('/api/dataset-intros?' + urlencode({'ids': json.dumps(ids)}),
            check, timeout_seconds=42, result_validator=lambda value: validate_rows(value, ids))

    async def related(self,payload,check):
        from .related_suggestions import validate_result,url_hash
        params={'context':json.dumps(payload['context'],ensure_ascii=False,separators=(',',':')),
                'exclude_ids':json.dumps(payload['exclude_ids'],separators=(',',':')),
                'exclude_urls':json.dumps([url_hash(url) for url in payload['exclude_urls']],separators=(',',':'))}
        return await self._request('/api/related?'+urlencode(params),check,timeout_seconds=23,
                                   result_validator=validate_result)

    async def _request(self, path, check, *, timeout_seconds=19.5, response_check=None, result_validator=None):
        queued=path.startswith('/api/jobs/')
        separate=self.max_queued>0
        if ((separate and (self.queued_active>=self.max_queued if queued else self.active-self.queued_active>=self.max_active)) or
                (not separate and (self.active >= self.max_active or
                (not queued and self.active-self.queued_active>=self.max_active-self.reserved_jobs) or
                (queued and self.reserved_jobs and self.queued_active>=self.reserved_jobs)))):
            raise ChatUnavailable(503, 'chat_busy')
        if not queued:check()
        self.active += 1
        if queued:self.queued_active+=1
        writer = None
        phase = 'connect'
        upstream_acquired = False
        try:
            async with asyncio.timeout(timeout_seconds):
                waiting = time.monotonic()
                self._upstream_waiters += 1
                try:
                    await self._upstream_slots.acquire()
                    upstream_acquired = True
                    self._upstream_active += 1
                    self._connection_stats['peak_upstream'] = max(self._connection_stats['peak_upstream'], self._upstream_active)
                finally:
                    self._upstream_waiters -= 1
                    self._connection_stats['upstream_wait_ms'] += round((time.monotonic()-waiting)*1000)
                if queued:await self._wait_for_capacity(check)
                reader, writer = await self._connect(queued)
                auth='X-Dataieum-Gateway-Token: '+self.gateway_token+'\r\n' if self.gateway_token else ''
                phase = 'write'
                writer.write(f'GET {path} HTTP/1.1\r\nHost: localhost\r\n{auth}Connection: close\r\n\r\n'.encode())
                await writer.drain()
                phase = 'header'
                head = await reader.readuntil(b'\r\n\r\n')
                lines = head.decode('ascii').split('\r\n')
                status = int(lines[0].split()[1])
                headers = {k.lower(): v.strip() for k, v in
                           (line.split(':', 1) for line in lines[1:] if ':' in line)}
                size = int(headers['content-length'])
                if not 0 <= size <= 1048576 or 'transfer-encoding' in headers:
                    raise ValueError('unbounded gateway response')
                body = bytearray()
                phase = 'body'
                while len(body) < size:
                    # This response has already passed the byte cap and holds
                    # an upstream slot. Do not wait for new-work CPU admission
                    # to drain it; retain telemetry and other resource guards.
                    if queued:await self._wait_for_capacity(response_check if response_check is not None else check)
                    else:check()
                    chunk = await reader.read(min(65536, size - len(body)))
                    if not chunk:
                        raise ValueError('incomplete gateway response')
                    body.extend(chunk)
                value = json.loads(body)
                if status != 200:
                    if isinstance(value,dict) and value.get('code') == 'authentication_required':
                        self._health = {'ready':False,'state':'authentication_required'}
                        self._health_at = time.monotonic()
                        raise ChatUnavailable(503, 'authentication_required')
                    raise ChatUnavailable(status if status in {400, 422, 503, 504} else 502,
                                          value['code'] if isinstance(value,dict) and value.get('code') in {'vector_unavailable','dependency_busy','dependency_timeout','model_initialization_timeout'} else
                                          'chat_busy' if status == 503 else 'chat_failed')
                if result_validator is not None:
                    result_validator(value)
                    return bytes(body)
                if isinstance(value,dict) and value.get('version') == 2:
                    validate_site_result(value)
                    return bytes(body)
                if (not isinstance(value, dict) or type(value.get('total')) is not int or
                        value['total'] < 0 or not isinstance(value.get('concept_id'), str) or
                        not isinstance(value.get('datasets'), list) or len(value['datasets']) > 30 or
                        any(not isinstance(row, dict) for row in value['datasets'])):
                    raise ValueError('invalid gateway result')
                applied = value.get('scope')
                if not isinstance(applied, dict) or applied.get('basis') != 'provider_country':
                    raise ValueError('missing applied country scope')
                country_list(applied.get('countries'))
                country_list(applied.get('unavailable_countries'))
                return bytes(body)
        except TimeoutError:
            raise ChatUnavailable(504, 'chat_timeout') from None
        except (ValueError, KeyError, UnicodeError, asyncio.IncompleteReadError, asyncio.LimitOverrunError):
            raise ChatUnavailable(502, 'chat_invalid_response') from None
        except OSError as error:
            if phase != 'connect':self._connection_error(phase, error)
            raise ChatUnavailable() from None
        finally:
            try:
                if writer is not None:
                    # Closing the upstream socket also cancels the owned CLI turn.
                    writer.close()
                    try: await asyncio.wait_for(writer.wait_closed(), .25)
                    except (OSError, TimeoutError): pass
            finally:
                if upstream_acquired:
                    self._upstream_active -= 1
                    self._upstream_slots.release()
                self.active -= 1
                if queued:self.queued_active-=1

    @staticmethod
    async def _wait_for_capacity(check):
        # A queued request can wait for fresh, healthy telemetry within its
        # existing deadline. Do not discard a completed upstream answer or
        # repeat a model call merely because one resource sample is unavailable.
        while True:
            try:
                check()
                return
            except ServerOverloaded as error:
                if error.reason not in {'telemetry_unavailable','telemetry_stale','cpu_pressure',
                                        'memory_pressure','disk_pressure','loop_lag','disk_headroom'}:
                    raise
                await asyncio.sleep(.25)
