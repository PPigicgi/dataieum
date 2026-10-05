"""Bounded async adapter for the immutable dataset/topic classification snapshot."""
import asyncio
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import json
import re
from urllib.parse import parse_qs

from .resources import bounded_json
from .errors import BudgetExceeded


class TopicAPI:
    def __init__(self, path=None, catalog_path=None, *, graph=None):
        if graph is None:
            from .topic_graph import TopicGraph
            graph = TopicGraph(path, catalog_path)
        self.graph = graph
        self._executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix='topic-read')
        self._slots = asyncio.Semaphore(2)
        self._tasks = set()
        self._active = 0
        self._closed = False
        self._stats = Counter()

    def status(self):
        return {'active': self._active, 'requests': len(self._tasks), 'workers': 2,
                'max_requests': 18, 'closed': self._closed, 'counters': dict(self._stats)}

    def _parse(self, path, raw):
        if not isinstance(raw, bytes) or len(raw) > 24576: raise ValueError('query size')
        values = parse_qs(raw.decode('utf-8'), strict_parsing=True, keep_blank_values=True,
                          errors='strict', max_num_fields=3)
        if any(len(v) != 1 for v in values.values()): raise ValueError('duplicate query')
        q = {k: v[0] for k, v in values.items()}
        if path == '/api/ontology/topics':
            if q: raise ValueError('overview query')
            return self.graph.overview, ()
        if path == '/api/ontology/dataset-topics':
            if set(q) != {'ids'}: raise ValueError('dataset ids required')
            ids = json.loads(q['ids'])
            if (not isinstance(ids, list) or len(ids) > 20 or
                    any(not isinstance(v, str) or not 1 <= len(v) <= 2048 or
                        any(ord(c) < 32 for c in v) for v in ids)):
                raise ValueError('invalid dataset ids')
            return self.graph.dataset_topics, (ids,)
        if set(q) - {'limit', 'band'}: raise ValueError('unknown fields')
        limit = int(q.get('limit', '20'))
        if not 1 <= limit <= 20: raise ValueError('invalid limit')
        band = q.get('band')
        if band is not None and band not in {'high', 'low'}: raise ValueError('invalid band')
        if path == '/api/ontology/unclassified':
            if band is not None: raise ValueError('unclassified band')
            return self.graph.unclassified, (limit,)
        prefix = '/api/ontology/topics/'
        if not path.startswith(prefix): raise KeyError('route')
        identifier = path[len(prefix):]
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,80}', identifier): raise ValueError('invalid topic')
        return self.graph.topic, (identifier, limit, band)

    async def query(self, path, raw=b''):
        try:
            operation, args = self._parse(path, raw)
        except (ValueError, UnicodeError, TypeError, RecursionError):
            return 400, {'code': 'invalid_query'}
        except KeyError:
            return 404, {'code': 'not_found'}
        if self._closed or len(self._tasks) >= 18:
            return 503, {'code': 'topic_busy'}
        task = asyncio.current_task()
        self._tasks.add(task)
        acquired = False
        future = None
        try:
            async with asyncio.timeout(.5): await self._slots.acquire()
            acquired = True
            self._active += 1
            self._stats['peak_active'] = max(self._stats['peak_active'], self._active)
            future = asyncio.get_running_loop().run_in_executor(self._executor, operation, *args)
            try:
                value = await asyncio.shield(future)
            except asyncio.CancelledError:
                # SQLite has its own cooperative deadline. Retain the permit
                # until that work ends so cancelled readers cannot overbook it.
                while not future.done():
                    try: await asyncio.shield(future)
                    except asyncio.CancelledError: continue
                    except Exception: break
                if future.done() and not future.cancelled(): future.exception()
                raise
            bounded_json(value, 262144)
            self._stats['completed'] += 1
            return 200, value
        except KeyError:
            return 404, {'code': 'topic_not_found'}
        except ValueError:
            return 400, {'code': 'invalid_query'}
        except (OSError, RuntimeError, TimeoutError, BudgetExceeded):
            self._stats['unavailable'] += 1
            return 503, {'code': 'topic_unavailable'}
        finally:
            if acquired:
                self._active -= 1
                self._slots.release()
            self._tasks.discard(task)

    async def aclose(self):
        if self._closed: return
        self._closed = True
        tasks = tuple(self._tasks)
        for task in tasks: task.cancel()
        if tasks: await asyncio.gather(*tasks, return_exceptions=True)
        self._executor.shutdown(wait=True, cancel_futures=True)
