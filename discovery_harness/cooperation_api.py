"""Account/cooperation HTTP boundary. Mail availability never controls search readiness."""
from __future__ import annotations

import asyncio
import hmac
import html
from http.cookies import SimpleCookie
import json
import logging
import os
from pathlib import Path
import re
import sqlite3
from urllib.parse import parse_qs, urlsplit

from .cooperation import CooperationError, contact_options, fail, prepare_request, sender, text
from .cooperation_store import CooperationStore
from .mail_delivery import MailWorker, ResendDelivery, WebhookProcessor

COOKIE = '__Host-dataieum-session'
LOG = logging.getLogger(__name__)


def read_config(path):
    with Path(path).open('rb') as stream:
        body = stream.read(65537)
    if len(body) > 65536: raise ValueError('configuration too large')
    return json.loads(body)


class CooperationAPI:
    def __init__(self, store, catalogue, *, contacts, template, from_address='', public_origin='', enabled=False,
                 provider=None, webhook=None, identity=None, google=None, auth_method='email'):
        self.store, self.catalogue = store, catalogue
        self.contacts, self.template = contacts, template
        self.from_address, self.public_origin, self.enabled = from_address, public_origin, enabled
        self.webhook = webhook
        self.identity = identity or (lambda scope: str(scope.get('client', ('unknown',))[0]))
        self.worker = MailWorker(store, provider, authorize=self.authorize) if provider else None
        self.google, self.auth_method, self.cleanup_task = google, auth_method, None

    @classmethod
    def from_env(cls, catalogue, control, identity):
        contacts_path = os.environ.get('DATAIEUM_COOPERATION_CONTACTS', '/harness-config/cooperation-contacts.json')
        template_path = os.environ.get('DATAIEUM_COOPERATION_TEMPLATE', '/harness-config/cooperation-template.json')
        mode = os.environ.get('DATAIEUM_MAIL_MODE', 'google')
        kwargs = dict(contacts=lambda: read_config(contacts_path), template=lambda: read_config(template_path),
                      identity=identity, auth_method='google' if mode == 'google' else 'email')
        if os.environ.get('DATAIEUM_COOPERATION_ENABLED') != '1': return cls(None, catalogue, **kwargs)
        try:
            origin = os.environ['DATAIEUM_PUBLIC_ORIGIN']
            parsed = urlsplit(origin)
            if parsed.scheme != 'https' or not parsed.hostname or parsed.path or parsed.query or parsed.fragment or parsed.username:
                raise ValueError('public origin must be an HTTPS origin')
            def secret(name): return Path(os.environ[name]).read_text(encoding='utf-8').strip()
            session_secret = secret('DATAIEUM_ACCOUNT_SECRET_FILE')
            if mode == 'google':
                from .google_mail import GoogleMail, CALLBACK
                from .google_mail_store import GoogleMailStore
                from google.oauth2 import id_token  # Fail closed when the official verifier is missing.
                client = read_config(os.environ['DATAIEUM_GOOGLE_CLIENT_FILE'])['web']
                if origin + CALLBACK not in client['redirect_uris']: raise ValueError('callback not registered in client file')
                store = GoogleMailStore(Path(control) / 'google-cooperation.sqlite3', session_secret,
                                        secret('DATAIEUM_GOOGLE_TOKEN_KEY_FILE'))
                return cls(store, catalogue, public_origin=origin, enabled=True,
                           google=GoogleMail(client['client_id'], client['client_secret'], origin), **kwargs)
            if mode not in {'resend','smtp'}: raise ValueError('unknown mail mode')
            if os.environ.get('DATAIEUM_MAIL_DOMAIN_VERIFIED') != '1': raise ValueError('verified mail domain required')
            from_address = sender(os.environ['DATAIEUM_MAIL_FROM'])
            if mode == 'smtp':
                from .smtp_delivery import SMTPDelivery
                provider = SMTPDelivery(read_config(os.environ['DATAIEUM_SMTP_CONFIG_FILE']),
                                        secret('DATAIEUM_SMTP_PASSWORD_FILE'), from_address)
                store = CooperationStore(Path(control) / 'smtp-cooperation.sqlite3', session_secret,
                                         idempotent_delivery=False)
                return cls(store, catalogue, from_address=from_address, public_origin=origin,
                           enabled=True, provider=provider, **kwargs)
            api_key = secret('DATAIEUM_RESEND_API_KEY_FILE')
            webhook_secret = secret('DATAIEUM_RESEND_WEBHOOK_SECRET_FILE')
            if not api_key or not webhook_secret: raise ValueError('missing secret')
            # Fail closed before creating the transport if official verification is unavailable.
            from svix.webhooks import Webhook
            Webhook(webhook_secret)
            store = CooperationStore(Path(control) / 'cooperation.sqlite3', session_secret)
            webhook = WebhookProcessor(store, webhook_secret)
            return cls(store, catalogue, from_address=from_address, public_origin=origin, enabled=True,
                       provider=ResendDelivery(api_key), webhook=webhook, **kwargs)
        except (KeyError, ValueError, TypeError, OSError, ImportError, sqlite3.Error, CooperationError):
            LOG.error('cooperation configuration unavailable; mail disabled')
            return cls(None, catalogue, **kwargs)

    async def start(self):
        if self.enabled and self.worker: await self.worker.start()
        if self.enabled and self.google and self.cleanup_task is None:
            try: await asyncio.to_thread(self.store.cleanup)
            except sqlite3.Error: LOG.error('Google mail cleanup deferred')
            self.cleanup_task = asyncio.create_task(self._cleanup())

    async def _cleanup(self):
        while True:
            await asyncio.sleep(60)
            try: await asyncio.to_thread(self.store.cleanup)
            except Exception: LOG.error('Google mail cleanup deferred')

    async def aclose(self):
        if self.worker: await self.worker.aclose()
        if self.cleanup_task:
            self.cleanup_task.cancel()
            try: await self.cleanup_task
            except asyncio.CancelledError: pass
            self.cleanup_task = None
        if self.google: await self.google.aclose()

    async def metrics(self):
        return {'enabled': self.enabled, 'queue': await asyncio.to_thread(self.store.metrics) if self.store else {}}

    async def configs(self):
        try:
            return await asyncio.to_thread(lambda: (self.contacts(), self.template()))
        except (OSError, ValueError, TypeError):
            fail(503, 'configuration_unavailable', '메일 양식과 연락처를 준비 중입니다.')

    async def dataset(self, identifier):
        identifier = text(identifier, 2048, line=True)
        try: dataset = await self.catalogue(identifier)
        except Exception: fail(503, 'catalogue_unavailable', '자료 정보를 일시적으로 확인할 수 없습니다. 잠시 후 다시 시도해 주세요.')
        if not dataset: fail(404, 'dataset_not_found', '선택한 자료를 찾을 수 없습니다.')
        return dataset

    async def current_preview(self, member, data):
        contacts, template = await self.configs()
        return prepare_request(data, member, await self.dataset(data.get('dataset_id')), contacts, template,
                               member['email'] if self.google else self.from_address)

    async def authorize(self, message):
        if not self.enabled: fail(503, 'disabled', '메일 발송을 준비 중입니다.')
        if message['request_id']:
            def lookup():
                with self.store.connection() as db:
                    row = db.execute('SELECT * FROM requests WHERE request_id=?', (message['request_id'],)).fetchone()
                    if not row: fail(409, 'request_unavailable', '요청을 확인할 수 없습니다.')
                    member = db.execute("SELECT * FROM members WHERE member_id=? AND status='active'", (row['member_id'],)).fetchone()
                    if not member or row['redacted']: fail(409, 'request_unavailable', '요청을 확인할 수 없습니다.')
                    return self.store.member(member), self.store._view(row)
            member, saved = await asyncio.to_thread(lookup)
            current = await self.current_preview(member, saved['input'])
            if not current['sendable'] or current['fingerprint'] != saved['fingerprint']:
                fail(409, 'preview_changed', '양식 또는 수신처가 변경되었습니다.')

    @staticmethod
    def headers(scope):
        result = {}
        for key, value in scope.get('headers', []):
            key = key.decode('ascii').lower()
            if key in result: fail(400, 'duplicate_header', '요청 헤더가 중복되었습니다.')
            result[key] = value.decode('latin-1')
        return result

    @staticmethod
    def cookie(token, *, clear=False):
        return (b'set-cookie', f'{COOKIE}={token}; Path=/; Secure; HttpOnly; SameSite=Lax; Max-Age={0 if clear else 86400}'.encode())

    @staticmethod
    async def body(receive, *, limit=16384):
        body = bytearray()
        async with asyncio.timeout(5):
            while True:
                item = await receive()
                if item['type'] == 'http.disconnect': fail(400, 'disconnected', '연결이 종료되었습니다.')
                body.extend(item.get('body', b''))
                if len(body) > limit: fail(413, 'body_too_large', '입력 내용이 너무 깁니다.')
                if not item.get('more_body'): return bytes(body)

    @staticmethod
    def fields(data, allowed, required=()):
        if not isinstance(data, dict) or set(data) - set(allowed) or not set(required) <= set(data):
            fail(400, 'invalid_fields', '요청 항목을 확인해 주세요.')

    async def __call__(self, scope, receive, send):
        try:
            status, value, extra = await self.route(scope, receive)
        except CooperationError as error:
            status, value, extra = error.status, {'code': error.code, 'error': error.message}, []
        except TimeoutError:
            status, value, extra = 408, {'code': 'body_timeout', 'error': '입력 시간이 초과되었습니다.'}, []
        except (ValueError, UnicodeError, TypeError, KeyError, RecursionError):
            status, value, extra = 400, {'code': 'invalid_request', 'error': '요청 내용을 확인해 주세요.'}, []
        except sqlite3.Error:
            status, value, extra = 503, {'code': 'account_store_unavailable', 'error': '잠시 후 다시 시도해 주세요.'}, []
        if scope['path'] == '/api/account/google/callback':
            message = value.get('error') or 'Gmail 계정을 연결했습니다. 이 창을 닫고 원래 검색 화면으로 돌아가 주세요.'
            body = ('<!doctype html><html lang="ko"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
                    '<title>데이터이음 Gmail 연결</title><h1>Gmail 연결</h1><p>' + html.escape(message) + '</p></html>').encode()
            mime = b'text/html; charset=utf-8'
        else:
            body = json.dumps(value, ensure_ascii=False).encode()
            mime = b'application/json; charset=utf-8'
        await send({'type': 'http.response.start', 'status': status,
                    'headers': [(b'content-type', mime), (b'cache-control', b'no-store'), (b'referrer-policy', b'no-referrer'),
                                (b'content-length', str(len(body)).encode()), *extra]})
        await send({'type': 'http.response.body', 'body': body})

    async def route(self, scope, receive):
        path, method = scope['path'], scope['method']; headers = self.headers(scope)
        raw_query = scope.get('query_string', b'')
        if len(raw_query) > 16384: fail(400, 'invalid_query', '요청 주소가 너무 깁니다.')
        query = parse_qs(raw_query.decode('utf-8'), keep_blank_values=True, max_num_fields=12)
        if path == '/api/account/google/callback' and method == 'GET':
            if not self.enabled or not self.google: fail(503, 'google_disabled', 'Google 연결을 준비 중입니다.')
            if (set(query) - {'state', 'code', 'error', 'scope', 'authuser', 'prompt', 'hd'}
                    or any(len(v) != 1 for v in query.values()) or 'state' not in query
                    or ('code' in query) == ('error' in query)):
                fail(400, 'invalid_query', 'Google 연결 요청을 다시 시작해 주세요.')
            cookie = SimpleCookie(); cookie.load(headers.get('cookie', ''))
            token = cookie[COOKIE].value if COOKIE in cookie else ''
            state = text(query['state'][0], 128, line=True)
            flow = await asyncio.to_thread(self.store.consume_google, token, state)
            if 'error' in query: fail(400, 'google_denied', 'Google 연결을 취소했습니다. 원래 화면에서 다시 연결할 수 있습니다.')
            identity = await self.google.exchange(query['code'][0], flow)
            session = await asyncio.to_thread(self.store.finish_google, token, state, identity)
            return 200, {'connected': True}, [self.cookie(session['token'])]
        if path == '/api/cooperation/webhooks/resend' and method == 'POST':
            if query: fail(400, 'invalid_query', '잘못된 요청입니다.')
            if not self.webhook: fail(503, 'webhook_unavailable', '웹훅이 비활성화되어 있습니다.')
            raw = await self.body(receive, limit=65536)
            await asyncio.to_thread(self.webhook.process, raw, headers)
            return 200, {'ok': True}, []
        if headers.get('sec-fetch-site') not in {None, 'same-origin', 'none'}:
            fail(403, 'origin_invalid', '현재 사이트에서 다시 시도해 주세요.')
        if path == '/api/cooperation/options' and method == 'GET':
            if set(query) != {'dataset_id'} or len(query['dataset_id']) != 1: fail(400, 'invalid_query', '자료를 선택해 주세요.')
            dataset = await self.dataset(query['dataset_id'][0])
            try:
                contacts, template = await self.configs()
            except CooperationError:
                if self.enabled: raise
                contacts, template = {'contacts': []}, {'types': {}, 'status': 'draft'}
            routes = {kind: contact_options(dataset, contacts, kind) for kind in ('usage_inquiry', 'data_provision')}
            allowed = [kind for kind, route in routes.items() if route['available']]
            options = routes[allowed[0]] if allowed else routes['usage_inquiry']
            return 200, {key: value for key, value in options.items() if key != 'contact'} | {
                'enabled': self.enabled, 'template_status': template.get('status'),
                'types': {key: value['label'] for key, value in template.get('types', {}).items() if key in allowed},
                'available_types': allowed}, []
        if not self.enabled or not self.store:
            if path in {'/api/account/me', '/api/account/google/status'} and method == 'GET':
                return 200, {'enabled': False, 'member': None, 'csrf': None, 'auth_method': self.auth_method, 'gmail_connected': False}, []
            fail(503, 'cooperation_disabled', '메일 자동 발송을 준비 중입니다. 공식 문의 경로를 이용해 주세요.')
        cookie = SimpleCookie(); cookie.load(headers.get('cookie', ''))
        token = cookie[COOKIE].value if COOKIE in cookie else ''
        session = await asyncio.to_thread(self.store.session, token)
        if method not in {'GET', 'HEAD'}:
            if headers.get('origin') != self.public_origin or not session or not hmac.compare_digest(headers.get('x-dataieum-csrf', ''), session['csrf']):
                fail(403, 'csrf_invalid', '화면을 다시 열고 시도해 주세요.')
            if headers.get('content-type', '').split(';')[0].strip() != 'application/json':
                fail(415, 'json_required', 'JSON 요청이 필요합니다.')
            raw = await self.body(receive)
            def unique(pairs):
                value = dict(pairs)
                if len(value) != len(pairs): raise ValueError('duplicate field')
                return value
            data = json.loads(raw, object_pairs_hook=unique)
            if not isinstance(data, dict): fail(400, 'invalid_fields', '요청 항목을 확인해 주세요.')
        else: data = {}
        if query and not (path == '/api/cooperation/requests' and method == 'GET'):
            fail(400, 'invalid_query', '요청 항목을 확인해 주세요.')
        if path in {'/api/account/me', '/api/account/google/status'} and method == 'GET':
            extra = []
            if not session and path == '/api/account/me':
                session = await asyncio.to_thread(self.store.new_session); extra = [self.cookie(session['token'])]
            connected = await asyncio.to_thread(self.store.connected, token) if self.google and token else False
            return 200, {'enabled': True, 'member': session['member'] if session else None,
                         'csrf': session['csrf'] if session else None,
                         'auth_method': self.auth_method, 'gmail_connected': connected}, extra
        if path == '/api/account/google/start' and method == 'POST' and self.google:
            self.fields(data, set())
            flow = await asyncio.to_thread(self.store.start_google, token)
            return 200, {'url': self.google.authorization_url(flow)}, []
        if self.google and path.startswith('/api/account/code/'):
            fail(404, 'not_found', 'Google 계정으로 연결해 주세요.')
        if path == '/api/account/code/start' and method == 'POST':
            self.fields(data, {'email', 'name', 'organization'}, {'email', 'name'})
            value = await asyncio.to_thread(self.store.start_code, token, data['email'], data['name'], data.get('organization', '개인'), self.identity(scope), self.from_address)
            return 202, value, []
        if path == '/api/account/code/verify' and method == 'POST':
            self.fields(data, {'challenge_id', 'code'}, {'challenge_id', 'code'})
            value = await asyncio.to_thread(self.store.verify_code, token, data['challenge_id'], data['code'])
            return 200, {'member': value['member'], 'csrf': value['csrf'], 'enabled': True}, [self.cookie(value['token'])]
        if not session or not session['member']: fail(401, 'account_required', '이메일 인증으로 가입하거나 로그인해 주세요.')
        member = session['member']; mid = member['member_id']
        if path == '/api/account/me' and method == 'PATCH':
            self.fields(data, {'name', 'organization'}, {'name', 'organization'})
            return 200, {'member': await asyncio.to_thread(self.store.update_member, mid, data['name'], data['organization'])}, []
        if path == '/api/account/me' and method == 'DELETE':
            self.fields(data, {'consent'}, {'consent'})
            if data['consent'] is not True: fail(400, 'consent_required', '회원 탈퇴에 동의해 주세요.')
            await asyncio.to_thread(self.store.delete_member, token)
            return 200, {'ok': True}, [self.cookie('', clear=True)]
        if path == '/api/account/logout' and method == 'POST':
            self.fields(data, set()); await asyncio.to_thread(self.store.logout, token)
            return 200, {'ok': True}, [self.cookie('', clear=True)]
        if path == '/api/cooperation/previews' and method == 'POST':
            request_id = data.pop('request_id', None)
            value = await self.current_preview(member, data)
            return 201, await asyncio.to_thread(self.store.save_preview, mid, value, request_id), []
        if path == '/api/cooperation/requests' and method == 'GET':
            if set(query) - {'page'} or any(len(v) != 1 for v in query.values()): fail(400, 'invalid_query', '페이지를 확인해 주세요.')
            page = int(query.get('page', ['1'])[0])
            if not 1 <= page <= 10000: fail(400, 'invalid_query', '페이지를 확인해 주세요.')
            return 200, await asyncio.to_thread(self.store.list_requests, mid, page), []
        match = re.fullmatch(r'/api/cooperation/requests/([a-f0-9-]{36})(/send)?', path)
        if match:
            request_id, action = match.groups()
            saved = await asyncio.to_thread(self.store.get_request, mid, request_id)
            if method == 'GET' and not action: return 200, saved, []
            if method == 'POST' and action:
                self.fields(data, {'version', 'consent'}, {'version', 'consent'})
                if data['consent'] is not True: fail(400, 'consent_required', '메일 내용과 개인정보 전달에 동의해 주세요.')
                current = await self.current_preview(member, saved['input']) if saved['consent_at'] is None else saved
                if self.google:
                    value, access = await asyncio.to_thread(self.store.claim_google, token, mid, request_id,
                                                          data['version'], current['fingerprint'])
                    if access is not None:
                        try: outcome, provider_id = await self.google.deliver(access, value['payload'])
                        except BaseException:
                            await asyncio.shield(asyncio.to_thread(self.store.finish_send, mid, request_id, 'unknown', None))
                            raise
                        value = await asyncio.to_thread(self.store.finish_send, mid, request_id, outcome, provider_id)
                    return 200, value, []
                value = await asyncio.to_thread(self.store.confirm, mid, request_id, data['version'], current['fingerprint'])
                return 202, value, []
        fail(404, 'not_found', '요청 경로를 찾을 수 없습니다.')
