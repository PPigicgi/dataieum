"""Bound connections at acceptance; keep admitted connections making progress.

Uvicorn's global connection-count check can reject every request in a burst.
This H11 adapter marks only excess connections for rejection. Heavy work still
passes the application's independent DB/LLM/request admission limits.
"""
import asyncio

import uvicorn
from uvicorn.protocols.http.h11_impl import H11Protocol


class BoundedH11Protocol(H11Protocol):
    max_connections = 1200

    def connection_made(self, transport):
        super().connection_made(transport)
        self._overflow = len(self.connections) > self.max_connections
        self._overflow_head = b''
        self._header_timer = self.loop.call_later(3, transport.close)

    def connection_lost(self, exc):
        self._header_timer.cancel()
        super().connection_lost(exc)

    def data_received(self, data):
        if self._overflow:
            self._overflow_head = (self._overflow_head + data)[:16385]
            if b'\r\n\r\n' in self._overflow_head or len(self._overflow_head) > 16384:
                body = b'{"code":"connection_slots","error":"Too many connections. Retry shortly."}'
                self.transport.write((f'HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\n'
                    f'Content-Length: {len(body)}\r\nRetry-After: 2\r\nConnection: close\r\n\r\n').encode()+body)
                self.transport.close()
                self._header_timer.cancel()
            return
        super().data_received(data)
        if self.cycle is not None:
            self._header_timer.cancel()


def main():
    uvicorn.run('discovery_harness.dataieum:create_app', factory=True,
                host='0.0.0.0', port=8000, workers=1, http=BoundedH11Protocol, ws='none',
                backlog=2048, timeout_keep_alive=3, timeout_graceful_shutdown=70,
                h11_max_incomplete_event_size=32768, access_log=False, proxy_headers=False)


if __name__ == '__main__':
    main()
