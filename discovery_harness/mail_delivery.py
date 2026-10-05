"""Resend transport and one bounded worker; raw-body verification uses official Svix."""
from __future__ import annotations

import asyncio
from datetime import datetime
import logging
import json

from .cooperation import CooperationError, fail, text

LOG = logging.getLogger(__name__)


class DeliveryError(Exception):
    def __init__(self, code, *, retryable=False, ambiguous=False):
        self.code, self.retryable, self.ambiguous = code, retryable, ambiguous


class ResendDelivery:
    def __init__(self, api_key, *, transport=None):
        import httpx
        self.client = httpx.AsyncClient(timeout=httpx.Timeout(10), follow_redirects=False, transport=transport,
                                        headers={'Authorization': 'Bearer ' + api_key})

    async def deliver(self, message):
        import httpx
        try:
            response = await self.client.post('https://api.resend.com/emails', json=message['payload'],
                                               headers={'Idempotency-Key': 'mail/' + message['message_id']})
        except httpx.RequestError:
            raise DeliveryError('provider_connection', retryable=True, ambiguous=True) from None
        if response.status_code == 429 or response.status_code >= 500:
            raise DeliveryError('provider_retry', retryable=True, ambiguous=response.status_code >= 500)
        if not 200 <= response.status_code < 300:
            raise DeliveryError('provider_rejected')
        try:
            provider_id = response.json()['id']
            if not isinstance(provider_id, str) or not 1 <= len(provider_id) <= 200: raise ValueError('invalid id')
        except (ValueError, KeyError, TypeError):
            raise DeliveryError('provider_response_invalid', retryable=True, ambiguous=True) from None
        return provider_id

    async def aclose(self):
        await self.client.aclose()


class MailWorker:
    def __init__(self, store, provider, *, authorize=None):
        self.store, self.provider, self.authorize = store, provider, authorize
        self.task = None

    async def once(self):
        message = await asyncio.to_thread(self.store.claim)
        if not message: return False
        try:
            if self.authorize: await self.authorize(message)
            provider_id = await self.provider.deliver(message)
        except CooperationError as error:
            await asyncio.to_thread(self.store.delivery_failed, message['message_id'],
                                    'dependency_unavailable' if error.status >= 500 else 'configuration_changed',
                                    retryable=error.status >= 500)
        except DeliveryError as error:
            await asyncio.to_thread(self.store.delivery_failed, message['message_id'], error.code,
                                    retryable=error.retryable, ambiguous=error.ambiguous)
        else:
            await asyncio.to_thread(self.store.accepted, message['message_id'], provider_id)
        return True

    async def _run(self):
        cycles = 0
        while True:
            try:
                if cycles % 60 == 0: await asyncio.to_thread(self.store.cleanup)
                worked = await self.once(); cycles += 1
                await asyncio.sleep(0.6 if worked else 2)
            except asyncio.CancelledError: raise
            except Exception:
                # Never print exceptions/payloads, which may contain addresses or provider bodies.
                LOG.error('cooperation worker deferred after internal failure')
                await asyncio.sleep(5)

    async def start(self):
        if self.task is None: self.task = asyncio.create_task(self._run())

    async def aclose(self):
        if self.task:
            self.task.cancel()
            try: await self.task
            except asyncio.CancelledError: pass
            self.task = None
        await self.provider.aclose()


class WebhookProcessor:
    def __init__(self, store, secret, *, verifier=None):
        self.store = store
        if verifier is None:
            from svix.webhooks import Webhook
            verifier = Webhook(secret).verify
        self.verify = verifier

    def process(self, body, headers):
        try:
            # Svix 2.x returns None on success. Parse exactly the verified bytes afterwards.
            self.verify(body.decode('utf-8'), headers)
            event = json.loads(body)
        except Exception:
            fail(400, 'webhook_signature_invalid', '웹훅 서명이 유효하지 않습니다.')
        try:
            event_id = text(headers.get('svix-id'), 200, line=True)
            provider_id = text(event['data']['email_id'], 200, line=True)
            timestamp = datetime.fromisoformat(event['created_at'].replace('Z', '+00:00'))
            if timestamp.tzinfo is None: raise ValueError('timezone missing')
            kind = {'email.sent': 'accepted', 'email.delivered': 'delivered', 'email.delivery_delayed': 'delayed',
                    'email.bounced': 'bounced', 'email.complained': 'complained', 'email.failed': 'failed'}.get(event['type'], 'ignored')
            if kind == 'bounced' and event['data'].get('bounce', {}).get('type') == 'Transient': kind = 'delayed'
        except (KeyError, TypeError, ValueError, AttributeError):
            fail(400, 'webhook_invalid', '웹훅 형식이 유효하지 않습니다.')
        self.store.event(event_id, provider_id, kind, timestamp.timestamp())
