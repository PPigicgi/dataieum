"""Outermost ASGI admission: reserve capacity before reading/parsing request bodies."""
import asyncio
import json

from .capacity import ServerOverloaded
from .errors import (AdapterContractError, BudgetExceeded, CapacityExceeded, CleanupFailed,
                     DeadlineExceeded, HarnessError)


class _BodyTooLarge(Exception):
    pass


class CapacityMiddleware:
    """Endpoints use scope['harness_session']; do not nest Harness.run.

    The application owns harness.start/aclose in its lifespan. Wrap the whole app,
    outside routing and body parsers. Upstream also needs bounded connections.
    """
    def __init__(self, app, harness):
        if not harness.policy.server.enabled:
            raise ValueError('capacity middleware requires server policy')
        self.app, self.harness = app, harness

    async def __call__(self, scope, receive, send):
        if scope['type'] == 'lifespan':
            return await self.app(scope, receive, send)
        if scope['type'] != 'http':
            if scope['type'] == 'websocket':
                await send({'type': 'websocket.close', 'code': 1008})
            return
        if scope.get('path') == '/health/live' and scope.get('method') == 'GET':
            return await self._reply(send, 200, 'alive')
        if scope.get('path') == '/health/ready' and scope.get('method') == 'GET':
            ready = self.harness.healthy and self.harness.capacity.status()['ready']
            return await self._reply(send, 200 if ready else 503, 'ready' if ready else 'unavailable')
        response_started = False
        response_bytes = 0

        async def workflow(session):
            async def tracked_send(message):
                nonlocal response_started, response_bytes
                # Each send, including empty events, consumes shared work.
                session._reserve(stream_items=1)
                # Work is already admitted and the response is size-bounded.
                # A CPU spike must not discard its completed result; retain
                # telemetry, memory, disk and poisoned-state protection.
                self.harness.capacity.check_response()
                if message['type'] == 'http.response.body':
                    chunk = message.get('body', b'')
                    if type(chunk) is not bytes:
                        raise AdapterContractError('ASGI response body must be bytes')
                    ceiling = self.harness.policy.server.max_response_bytes
                    if len(chunk) > ceiling - response_bytes:
                        raise BudgetExceeded('response_bytes', ceiling)
                    response_bytes += len(chunk)
                if message['type'] == 'http.response.start':
                    response_started = True
                await send(message)
                await asyncio.sleep(0)

            limit = self.harness.policy.server.max_request_body_bytes
            for key, value in scope.get('headers', []):
                if key.lower() == b'content-length':
                    if len(value) > 10 or not value.isdigit() or int(value) > limit:
                        raise _BodyTooLarge
            body = bytearray()
            while True:
                # Empty transport events still consume bounded receive work.
                session._reserve(stream_items=1)
                message = await receive()
                session._check()
                self.harness.capacity.check()
                if message['type'] == 'http.disconnect':
                    raise asyncio.CancelledError
                if message['type'] != 'http.request':
                    raise ValueError('invalid ASGI request message')
                chunk = message.get('body', b'')
                if len(body) + len(chunk) > limit:
                    raise _BodyTooLarge
                body.extend(chunk)
                if not message.get('more_body', False):
                    break
                await asyncio.sleep(0)
            delivered = False

            async def bounded_receive():
                nonlocal delivered
                session._check()
                self.harness.capacity.check()
                if not delivered:
                    delivered = True
                    return {'type': 'http.request', 'body': bytes(body), 'more_body': False}
                # Disconnect listeners keep the same work and pressure bounds
                # after the buffered request body has been delivered once.
                session._reserve(stream_items=1)
                message = await receive()
                await asyncio.sleep(0)
                session._check()
                self.harness.capacity.check()
                return message

            self.harness.capacity.check()
            await self.app({**scope, 'harness_session': session}, bounded_receive, tracked_send)

        try:
            await self.harness.run(workflow)
        except (ServerOverloaded, CapacityExceeded, CleanupFailed) as exc:
            if response_started:
                raise
            self.harness.capacity.record_rejection(getattr(exc, 'reason', exc.code))
            await self._reply(send, 503, exc.code, retry=True)
        except _BodyTooLarge:
            if response_started:
                raise
            await self._reply(send, 413, 'request_body_too_large')
        except DeadlineExceeded as exc:
            if response_started:
                raise
            await self._reply(send, 504, exc.code)
        except HarnessError as exc:
            if response_started:
                raise
            await self._reply(send, 422, exc.code)

    @staticmethod
    async def _reply(send, status, code, retry=False):
        body = json.dumps({'code': code}).encode()
        headers = [(b'content-type', b'application/json'), (b'content-length', str(len(body)).encode())]
        if retry:
            headers.append((b'retry-after', b'1'))
        await send({'type': 'http.response.start', 'status': status, 'headers': headers})
        await send({'type': 'http.response.body', 'body': body})
