"""Google authorization and one-shot Gmail transport for the website.

No mailbox reads, refresh tokens, model-generated headers, or automatic send retries.
"""
from __future__ import annotations

import asyncio
import base64
from email.message import EmailMessage
from email.policy import SMTP
import hashlib
import hmac
from urllib.parse import urlencode

import httpx

from .cooperation import email, fail, text

SEND_SCOPE = 'https://www.googleapis.com/auth/gmail.send'
SCOPES = 'openid email profile ' + SEND_SCOPE
CALLBACK = '/api/account/google/callback'


def verify_identity(token, client_id):
    """Official signature, issuer, audience and lifetime verification; bounded I/O."""
    from google.auth.transport.requests import Request
    from google.oauth2.id_token import verify_oauth2_token
    import requests
    with requests.Session() as session:
        session.trust_env = False
        request = Request(session=session)
        return verify_oauth2_token(token, lambda *a, **kw: request(*a, **{**kw, 'timeout': 10}), client_id)


class GoogleMail:
    def __init__(self, client_id, client_secret, public_origin, *, transport=None, verifier=verify_identity):
        self.client_id = text(client_id, 512, line=True)
        self.client_secret = text(client_secret, 512, line=True)
        self.redirect_uri = public_origin + CALLBACK
        self.verify = verifier
        self.client = httpx.AsyncClient(timeout=10, follow_redirects=False, trust_env=False, transport=transport)

    def authorization_url(self, flow):
        challenge = base64.urlsafe_b64encode(hashlib.sha256(flow['verifier'].encode()).digest()).decode().rstrip('=')
        return 'https://accounts.google.com/o/oauth2/v2/auth?' + urlencode({
            'client_id': self.client_id, 'redirect_uri': self.redirect_uri, 'response_type': 'code',
            'scope': SCOPES, 'state': flow['state'], 'nonce': flow['nonce'],
            'code_challenge': challenge, 'code_challenge_method': 'S256',
            'access_type': 'online', 'prompt': 'select_account',
        })

    async def exchange(self, code, flow):
        try:
            response = await self.client.post('https://oauth2.googleapis.com/token', data={
                'client_id': self.client_id, 'client_secret': self.client_secret,
                'code': text(code, 4096, line=True), 'code_verifier': flow['verifier'],
                'redirect_uri': self.redirect_uri, 'grant_type': 'authorization_code',
            })
            if response.status_code != 200: raise ValueError('exchange rejected')
            value = response.json()
            if value.get('token_type', '').lower() != 'bearer': raise ValueError('token type')
            if SEND_SCOPE not in value.get('scope', '').split():
                fail(403, 'google_scope_required', 'Gmail 발송 권한을 허용해야 메일을 보낼 수 있습니다.')
            claims = await asyncio.to_thread(self.verify, value['id_token'], self.client_id)
            if (claims.get('email_verified') is not True
                    or not isinstance(claims.get('nonce'), str)
                    or not hmac.compare_digest(claims['nonce'], flow['nonce'])
                    or claims.get('azp', self.client_id) != self.client_id):
                raise ValueError('identity mismatch')
            lifetime = value['expires_in']
            if type(lifetime) is not int or lifetime <= 60: raise ValueError('token lifetime')
            return {'sub': text(claims.get('sub'), 255, line=True), 'email': email(claims.get('email')),
                    'name': text(claims.get('name') or claims.get('email'), 100, line=True),
                    'access_token': text(value.get('access_token'), 8192, line=True),
                    'expires_in': min(lifetime - 30, 3570)}
        except (httpx.HTTPError, ValueError, KeyError, TypeError):
            fail(502, 'google_connection_failed', 'Google 연결을 완료하지 못했습니다. 창을 닫고 다시 연결해 주세요.')
        except Exception as exc:
            # google-auth has multiple verification/transport exception types. Never expose tokens.
            from .cooperation import CooperationError
            if isinstance(exc, CooperationError): raise
            fail(502, 'google_connection_failed', 'Google 계정을 확인하지 못했습니다. 다시 연결해 주세요.')

    async def deliver(self, access_token, payload):
        message = EmailMessage(policy=SMTP)
        message['From'] = email(payload['from'])
        message['To'] = email(payload['to'][0])
        message['Subject'] = text(payload['subject'], 998, line=True)
        message.set_content(payload['text'])
        raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
        try:
            response = await self.client.post('https://gmail.googleapis.com/gmail/v1/users/me/messages/send',
                headers={'Authorization': 'Bearer ' + access_token}, json={'raw': raw})
            if response.status_code == 200:
                identifier = response.json().get('id')
                if isinstance(identifier, str) and 0 < len(identifier) <= 200:
                    return 'accepted', identifier
                return 'unknown', None
            # A definitive rejection may be displayed, but is never retried by this transport.
            if 400 <= response.status_code < 500 and response.status_code != 408:
                return 'failed', None
            return 'unknown', None
        except (httpx.HTTPError, ValueError, TypeError):
            return 'unknown', None

    async def aclose(self):
        await self.client.aclose()
