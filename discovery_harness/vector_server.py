"""Authenticated, bounded local HTTP transport for vector retrieval only."""
from __future__ import annotations

import argparse
import hmac
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path
import socket
from socketserver import ThreadingMixIn
import sqlite3
import threading
import time
from collections import Counter

from .vector_index import IndexUnavailable, MODEL, VectorIndex


MAX_BODY = 64 * 1024


def _strict_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate JSON field')
        result[key] = value
    return result


def _reject_constant(_value):
    raise ValueError('non-finite JSON number')


class VectorHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True
    block_on_close = True
    request_queue_size = 16
    allow_reuse_address = True

    def __init__(self, address, index, token, *, workers=2, connections=8, wait_seconds=1.):
        if not isinstance(token, str) or not token.isascii() or not 32 <= len(token) <= 512 or any(c.isspace() for c in token):
            raise ValueError('gateway token must contain 32 to 512 non-whitespace characters')
        self.index, self.token = index, token
        if type(workers) is not int or not 1<=workers<=2 or type(connections) is not int or not workers<=connections<=16 or not 0<wait_seconds<=3:
            raise ValueError('invalid vector service capacity')
        # Parsing/health connections must not consume the expensive search slots.
        self.slots = threading.BoundedSemaphore(connections)
        self.search_slots = threading.BoundedSemaphore(workers)
        self.workers, self.connections, self.wait_seconds = workers, connections, wait_seconds
        self.stats = Counter()
        self.stats_lock = threading.Lock()
        super().__init__(address, VectorHandler)

    def status(self):
        with self.stats_lock:
            return {**self.stats,'search_workers':self.workers,'connection_slots':self.connections,
                    'queue_wait_seconds':self.wait_seconds}

    def process_request(self, request, client_address):
        # No executor queue: excess connections get an immediate bounded reply.
        if not self.slots.acquire(blocking=False):
            with self.stats_lock:self.stats['connection_rejected']+=1
            try:
                request.settimeout(.25)
                request.sendall(b'HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: 23\r\nRetry-After: 1\r\n\r\n{"error":"vector_busy"}')
            except OSError:
                pass
            finally:
                self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self.slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.slots.release()


class VectorHandler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    server_version = 'DataieumVector/1'
    sys_version = ''

    def setup(self):
        self.request.settimeout(3)
        super().setup()

    def log_message(self, _format, *_args):
        # Never log vectors, credentials, query strings, or metadata.
        pass

    def reply(self, status, value):
        body = json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Connection', 'close')
        self.send_header('Cache-Control', 'no-store')
        self.send_header('X-Content-Type-Options', 'nosniff')
        if status == 503:
            self.send_header('Retry-After', '1')
        self.end_headers()
        self.close_connection = True
        self.wfile.write(body)

    def do_GET(self):
        if self.path not in {'/health/ready','/health/metrics'}:
            self.reply(404, {'error': 'not_found'})
            return
        try:
            self.reply(200, {**self.server.index.health(),'scheduler':self.server.status()})
        except (IndexUnavailable, OSError, sqlite3.DatabaseError):
            self.reply(503, {'ready': False, 'error': 'vector_index_unavailable'})

    def do_POST(self):
        received = time.monotonic()
        if self.path != '/search':
            self.reply(404, {'error': 'not_found'})
            return
        tokens = self.headers.get_all('X-Dataieum-Gateway-Token') or []
        if len(tokens) != 1 or not tokens[0].isascii() or not hmac.compare_digest(tokens[0], self.server.token):
            self.reply(403, {'error': 'forbidden'})
            return
        lengths = self.headers.get_all('Content-Length') or []
        if self.headers.get('Transfer-Encoding') or len(lengths) != 1 or not lengths[0].isascii() or not lengths[0].isdigit():
            self.reply(400, {'error': 'invalid_content_length'})
            return
        length = int(lengths[0])
        if not 1 <= length <= MAX_BODY:
            self.reply(413, {'error': 'request_too_large'})
            return
        if self.headers.get_content_type() != 'application/json':
            self.reply(415, {'error': 'json_required'})
            return
        try:
            body = self.rfile.read(length)
            if len(body) != length:
                raise ValueError('incomplete request')
            request = json.loads(body.decode('utf-8'), object_pairs_hook=_strict_object,
                                 parse_constant=_reject_constant)
            if not isinstance(request, dict) or set(request) - {'model', 'vector', 'limit', 'source_ids','timeout_ms','explore','include_related','filter_groups','lexical_query','collapse_time_series'}:
                raise ValueError('unsupported request fields')
            if request.get('model') != MODEL or 'vector' not in request:
                raise ValueError('embedding model and vector required')
            if type(request.get('explore', False)) is not bool or type(request.get('include_related', True)) is not bool:
                raise ValueError('explore must be boolean')
            if type(request.get('collapse_time_series', False)) is not bool:
                raise ValueError('collapse_time_series must be boolean')
            budget = request.get('timeout_ms',7000)
            if type(budget) is not int or not 1<=budget<=7000:raise ValueError('invalid search deadline')
            deadline = received + budget/1000
            with self.server.stats_lock:self.server.stats['waiting_searches']+=1
            try:
                acquired=self.server.search_slots.acquire(timeout=min(self.server.wait_seconds,max(0,deadline-time.monotonic())))
            finally:
                with self.server.stats_lock:self.server.stats['waiting_searches']-=1
            if not acquired:
                with self.server.stats_lock:self.server.stats['search_rejected']+=1
                self.reply(503,{'error':'vector_busy'})
                return
            with self.server.stats_lock:
                self.server.stats['active_searches']+=1
                self.server.stats['searches']+=1
                self.server.stats['peak_searches']=max(self.server.stats['peak_searches'],self.server.stats['active_searches'])
            try:
                remaining=deadline-time.monotonic()
                if remaining<=0:raise TimeoutError('search deadline expired in queue')
                result = self.server.index.search(request['vector'], limit=request.get('limit', 40),
                                                  source_ids=request.get('source_ids'), timeout=remaining,
                                                  explore=request.get('explore', False),lexical_query=request.get('lexical_query'),
                                                  **({'filter_groups':request['filter_groups']} if 'filter_groups' in request else {}),
                                                  **({'collapse_time_series':request['collapse_time_series']} if 'collapse_time_series' in request else {}),
                                                  **({'include_related':request['include_related']} if 'include_related' in request else {}))
                if time.monotonic()>=deadline:raise TimeoutError('search deadline expired')
            finally:
                with self.server.stats_lock:self.server.stats['active_searches']-=1
                self.server.search_slots.release()
            self.reply(200, result)
        except (ValueError, UnicodeError, RecursionError, OverflowError):
            self.reply(400, {'error': 'invalid_vector_request'})
        except (TimeoutError, socket.timeout):
            with self.server.stats_lock:self.server.stats['search_timeout']+=1
            self.reply(504, {'error': 'vector_search_timeout'})
        except (IndexUnavailable, sqlite3.DatabaseError, OSError):
            self.reply(503, {'error': 'vector_index_unavailable'})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=8093)
    parser.add_argument('--embeddings', default='/data/embeddings-small-v1/embeddings.sqlite3')
    parser.add_argument('--catalog', default='/data/catalog.sqlite3')
    parser.add_argument('--index', default='/vector-index')
    parser.add_argument('--token-file', default='/run/dataieum-gateway.token')
    parser.add_argument('--threads', type=int, default=2)
    parser.add_argument('--topic-vectors', default=os.environ.get('DATAIEUM_TOPIC_VECTORS'))
    parser.add_argument('--topic-graph', default=os.environ.get('DATAIEUM_TOPIC_GRAPH'))
    parser.add_argument('--coverage-index', default=os.environ.get('DATAIEUM_COVERAGE_INDEX'))
    args = parser.parse_args()
    token = Path(args.token_file).read_text(encoding='utf-8').strip()
    index = VectorIndex(args.embeddings, args.catalog, args.index, threads=args.threads,
                        topic_vectors=args.topic_vectors, topic_graph=args.topic_graph,coverage_index=args.coverage_index)
    with VectorHTTPServer((args.host, args.port), index, token) as server:
        server.serve_forever(poll_interval=.25)


if __name__ == '__main__':
    main()
