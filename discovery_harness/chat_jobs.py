"""Durable, bounded chat admission for a single application owner.

SQLite owns the FIFO and admission counters; one executor owns its connection.
An execution slot is released only after the handler has finished cancellation.
Callers authenticate jobs with a 256-bit browser-generated capability token.
"""
import asyncio
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import time


class JobError(Exception):
    def __init__(self, status, code):
        self.status, self.code = status, code
        super().__init__(code)


@dataclass
class _Running:
    task: asyncio.Task
    reason: str | None = None


class ChatJobs:
    """An async job store. The application must hold its process owner lock.

    ``start``, ``submit``, ``get``, ``cancel``, ``status`` and ``aclose`` are async.
    No callback or model request is retried. Queued jobs survive shutdown; a job
    found running after a process crash fails explicitly on the next start.
    """

    def __init__(self, path, handler_async, *, workers=4, max_pending=1000,
                 max_owner_pending=2, max_finished=1000, max_result_bytes=1048576,
                 max_stored_result_bytes=67108864, result_ttl=600, queue_ttl=7200,
                 execution_timeout=65, ready_async=None, dispatcher_lock=None, dispatch_pause=None):
        integers = ((workers, 1, 1000), (max_pending, 1, 1000),
                    (max_owner_pending, 1, max_pending), (max_finished, 1, 1000),
                    (max_result_bytes, 1, 1048576),
                    (max_stored_result_bytes, max_result_bytes, 67108864))
        durations = ((result_ttl, 600), (queue_ttl, 7200), (execution_timeout, 7265))
        if (any(type(v) is not int or not lo <= v <= hi for v, lo, hi in integers)
                or any(type(v) not in (int, float) or not math.isfinite(v)
                       or not 0 < v <= hi for v, hi in durations)
                or not callable(handler_async)
                or (ready_async is not None and not callable(ready_async))):
            raise ValueError('invalid chat job bounds')
        self.path, self.handler = Path(path), handler_async
        self.dispatcher_lock = Path(dispatcher_lock) if dispatcher_lock else None
        self.dispatch_pause = Path(dispatch_pause) if dispatch_pause else None
        if bool(self.dispatcher_lock) != bool(self.dispatch_pause):
            raise ValueError('Dispatcher lock and pause path must be configured together')
        self._dispatcher_fd = None
        self.workers, self.max_pending = workers, max_pending
        self.max_owner_pending, self.max_finished = max_owner_pending, max_finished
        self.max_result_bytes = max_result_bytes
        self.max_stored_result_bytes = max_stored_result_bytes
        self.result_ttl, self.queue_ttl = result_ttl, queue_ttl
        self.receipt_ttl = queue_ttl + min(execution_timeout,65) + result_ttl
        self.max_receipts = 10000
        self.execution_timeout, self.ready_async = execution_timeout, ready_async
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='chat-jobs-db')
        self._connection = None
        self._db_lock, self._control_lock = asyncio.Lock(), asyncio.Lock()
        self._lifecycle_lock, self._ready_lock = asyncio.Lock(), asyncio.Lock()
        self._wake, self._stop = asyncio.Event(), asyncio.Event()
        self._tasks, self._running = [], {}
        self._completions = set()
        self._close_task = None
        self._started = self._closing = self._closed = self._fatal = False
        self._ready_value, self._ready_checked = True, float('-inf')
        self._blocked_reason = None
        self._stats = Counter()
        self._listeners = {}
        self._listener_count = 0

    @staticmethod
    def _token(token, *, lookup=False):
        if not isinstance(token, str) or re.fullmatch(r'[0-9a-fA-F]{64}', token) is None:
            raise JobError(404 if lookup else 400,
                           'job_not_found' if lookup else 'invalid_job_token')
        digest = hashlib.sha256(token.encode('ascii')).hexdigest()
        return digest[:32], digest

    @staticmethod
    def _encode(value, maximum, code):
        if not isinstance(value, dict):
            raise JobError(400, code)
        try:
            encoded = json.dumps(value, sort_keys=True, separators=(',', ':'),
                                 ensure_ascii=False, allow_nan=False)
            size = len(encoded.encode('utf-8'))
        except (TypeError, ValueError, OverflowError, RecursionError, UnicodeError):
            raise JobError(400, code) from None
        if size > maximum:
            raise JobError(413, code)
        return encoded, size

    async def _db(self, operation, *args):
        # Shield the transaction, including on HTTP disconnect, before releasing
        # the DB lock. A retry of an interrupted submit uses the same capability.
        async with self._db_lock:
            future = asyncio.get_running_loop().run_in_executor(self._executor, operation, *args)
            try:
                return await asyncio.shield(future)
            except asyncio.CancelledError:
                while not future.done():
                    try:
                        await asyncio.shield(future)
                    except asyncio.CancelledError:
                        pass
                    except Exception:
                        break
                if future.done() and not future.cancelled():
                    future.exception()
                raise
            except (sqlite3.Error, OSError):
                raise JobError(503, 'job_store_unavailable') from None

    def _transaction(self, operation, *args):
        self._connection.execute('BEGIN IMMEDIATE')
        try:
            result = operation(*args)
            self._connection.execute('COMMIT')
            return result
        except BaseException:
            self._connection.execute('ROLLBACK')
            raise

    def _open(self):
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        try:
            descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
        except FileExistsError:
            pass
        else:
            os.close(descriptor)
        os.chmod(self.path, 0o600)
        connection = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        connection.row_factory = sqlite3.Row
        self._connection = connection
        connection.executescript('''
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            PRAGMA secure_delete=ON;
            PRAGMA busy_timeout=5000;
            PRAGMA journal_size_limit=1048576;
            PRAGMA wal_autocheckpoint=256;
            CREATE TABLE IF NOT EXISTS chat_jobs (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL UNIQUE,
                token_hash TEXT NOT NULL UNIQUE,
                owner TEXT NOT NULL,
                payload TEXT,
                payload_hash TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN
                    ('queued','running','completed','failed','cancelled','expired')),
                created_at REAL NOT NULL,
                started_at REAL,
                finished_at REAL,
                queue_expires_at REAL NOT NULL,
                result TEXT,
                result_bytes INTEGER NOT NULL DEFAULT 0,
                error TEXT,
                cancel_requested INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS chat_jobs_fifo ON chat_jobs(status,seq);
            CREATE INDEX IF NOT EXISTS chat_jobs_owner ON chat_jobs(owner,status);
            CREATE INDEX IF NOT EXISTS chat_jobs_retention ON chat_jobs(finished_at,seq);
            CREATE TABLE IF NOT EXISTS chat_job_receipts (
                job_id TEXT PRIMARY KEY,
                token_hash TEXT NOT NULL UNIQUE,
                owner TEXT NOT NULL,
                payload_hash TEXT NOT NULL,
                expires_at REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS chat_job_receipts_expiry ON chat_job_receipts(expires_at);
        ''')
        def recover():
            now = time.time()
            connection.execute('DELETE FROM chat_job_receipts WHERE expires_at<=?', (now,))
            # Preserve idempotency when upgrading a store created before receipts
            # existed. Neither source prompts nor results enter this compact table.
            connection.execute('''INSERT OR IGNORE INTO chat_job_receipts
                (job_id,token_hash,owner,payload_hash,expires_at)
                SELECT job_id,token_hash,owner,payload_hash,created_at+? FROM chat_jobs
                WHERE created_at+?>?''', (self.receipt_ttl, self.receipt_ttl, now))
            if connection.execute('SELECT count(*) FROM chat_job_receipts').fetchone()[0] > self.max_receipts:
                raise JobError(503, 'job_receipt_capacity')
            count = 0 if self.dispatcher_lock else connection.execute('''UPDATE chat_jobs SET status='failed',
                error='worker_restarted',payload=NULL,result=NULL,result_bytes=0,finished_at=?
                WHERE status='running' ''', (now,)).rowcount
            self._prune(now)
            return count
        return self._transaction(recover)

    def _recover_running(self):
        # Only the exclusive dispatcher may recover work abandoned by a crash.
        # Never resubmit uncertain model requests.
        return self._connection.execute("UPDATE chat_jobs SET status='failed',"
            "error='worker_restarted',payload=NULL,result=NULL,result_bytes=0,finished_at=? "
            "WHERE status='running'", (time.time(),)).rowcount

    def _release_dispatcher(self):
        if self._dispatcher_fd is not None:
            self._dispatcher_fd.close()
            self._dispatcher_fd = None

    async def _dispatch_allowed(self):
        if self.dispatcher_lock is None:
            return True
        if self.dispatch_pause.exists():
            if not self._running and not self._completions:
                self._release_dispatcher()
            return False
        if self._dispatcher_fd is None:
            import fcntl
            self.dispatcher_lock.parent.mkdir(parents=True, exist_ok=True)
            fd = self.dispatcher_lock.open('a')
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                fd.close()
                return False
            self._dispatcher_fd = fd
            if self.dispatch_pause.exists():
                self._release_dispatcher()
                return False
            self._stats['restarted_failed'] += await self._db(self._transaction, self._recover_running)
        return True

    async def _remote_cancellations(self):
        if self.dispatcher_lock is None or not self._running:
            return
        def cancelled():
            return [r[0] for r in self._connection.execute(
                "SELECT job_id FROM chat_jobs WHERE status='running' AND cancel_requested=1")]
        for job_id in await self._db(cancelled):
            active = self._running.get(job_id)
            if active is not None and active.reason is None:
                active.reason = 'cancelled'
                active.task.cancel()

    def _prune(self, now):
        """All callers run inside a transaction on the dedicated DB thread."""
        self._connection.execute('DELETE FROM chat_job_receipts WHERE expires_at<=?', (now,))
        self._connection.execute('''UPDATE chat_jobs SET status='expired',error='queue_expired',
            payload=NULL,finished_at=? WHERE status='queued' AND queue_expires_at<=?''', (now, now))
        self._connection.execute('DELETE FROM chat_jobs WHERE finished_at IS NOT NULL AND finished_at<=?',
                                 (now - self.result_ttl,))
        rows = self._connection.execute('''SELECT seq,result_bytes FROM chat_jobs
            WHERE finished_at IS NOT NULL ORDER BY finished_at,seq''').fetchall()
        total_bytes, count = sum(row['result_bytes'] for row in rows), len(rows)
        remove = []
        for row in rows:
            if count <= self.max_finished and total_bytes <= self.max_stored_result_bytes:
                break
            remove.append((row['seq'],))
            count -= 1
            total_bytes -= row['result_bytes']
        if remove:
            self._connection.executemany('DELETE FROM chat_jobs WHERE seq=?', remove)

    def _public(self, row):
        position = 0
        if row['status'] == 'queued':
            position = self._connection.execute(
                "SELECT count(*) FROM chat_jobs WHERE status='queued' AND seq<=?",
                (row['seq'],)).fetchone()[0]
        output = {'job_id': row['job_id'], 'status': row['status'], 'position': position,
                  'poll_after_ms': 3000 + min(7000, position * 10), 'created_at': row['created_at'],
                  'started_at': row['started_at'], 'finished_at': row['finished_at'],
                  'queue_expires_at': row['queue_expires_at'],
                  'cancel_requested': bool(row['cancel_requested'])}
        if row['status'] == 'queued' and self._blocked_reason:
            output['blocked_reason'] = self._blocked_reason
        if row['status'] == 'completed':
            output['result'] = json.loads(row['result'])
        elif row['error']:
            output['error'] = row['error']
        return output

    def _authenticated(self, job_id, digest):
        row = self._connection.execute('SELECT * FROM chat_jobs WHERE job_id=?', (job_id,)).fetchone()
        if row is None or not hmac.compare_digest(row['token_hash'], digest):
            raise JobError(404, 'job_not_found')
        return row

    def _submit(self, payload, payload_hash, owner, job_id, digest, blocked_reason=None, *, expires_at=None):
        now = time.time()
        self._prune(now)
        previous = self._connection.execute('SELECT * FROM chat_jobs WHERE job_id=?', (job_id,)).fetchone()
        if previous is not None:
            if (not hmac.compare_digest(previous['token_hash'], digest)
                    or previous['owner'] != owner or previous['payload_hash'] != payload_hash):
                raise JobError(409, 'job_conflict')
            return self._public(previous), False
        receipt = self._connection.execute('SELECT * FROM chat_job_receipts WHERE job_id=?', (job_id,)).fetchone()
        if receipt is not None:
            if (not hmac.compare_digest(receipt['token_hash'], digest)
                    or receipt['owner'] != owner or receipt['payload_hash'] != payload_hash):
                raise JobError(409, 'job_conflict')
            # A lost response or refreshed page must never silently execute the
            # same expensive model request after its result was evicted.
            raise JobError(410, 'job_expired')
        if expires_at is not None and expires_at<=now:raise JobError(410,'job_expired')
        if blocked_reason == 'authentication_required' or self._blocked_reason == 'authentication_required':
            raise JobError(503, 'authentication_required')
        pending = self._connection.execute(
            "SELECT count(*) FROM chat_jobs WHERE status IN ('queued','running')").fetchone()[0]
        if pending >= self.max_pending:
            raise JobError(429, 'job_queue_full')
        owned = self._connection.execute('''SELECT count(*) FROM chat_jobs
            WHERE owner=? AND status IN ('queued','running')''', (owner,)).fetchone()[0]
        if owned >= self.max_owner_pending:
            raise JobError(429, 'job_owner_limit')
        receipts = self._connection.execute('SELECT count(*) FROM chat_job_receipts').fetchone()[0]
        if receipts >= self.max_receipts:
            raise JobError(429, 'job_receipt_capacity')
        self._connection.execute('''INSERT INTO chat_job_receipts
            (job_id,token_hash,owner,payload_hash,expires_at) VALUES (?,?,?,?,?)''',
            (job_id, digest, owner, payload_hash, now + self.receipt_ttl))
        self._connection.execute('''INSERT INTO chat_jobs
            (job_id,token_hash,owner,payload,payload_hash,status,created_at,queue_expires_at)
            VALUES (?,?,?,?,?,'queued',?,?)''',
            (job_id, digest, owner, payload, payload_hash, now, min(now+self.queue_ttl,expires_at) if expires_at is not None else now+self.queue_ttl))
        row = self._connection.execute('SELECT * FROM chat_jobs WHERE job_id=?', (job_id,)).fetchone()
        return self._public(row), True

    def _get(self, job_id, digest):
        # A status read must not scan/trim every other user's result. Enforce
        # this row's exact expiry here; writes and the dispatcher prune globally.
        row=self._authenticated(job_id,digest)
        now=time.time()
        if row['finished_at'] is not None and row['finished_at']<=now-self.result_ttl:
            raise JobError(404,'job_not_found')
        if row['status']=='queued' and row['queue_expires_at']<=now:
            self._connection.execute("UPDATE chat_jobs SET status='expired',error='queue_expired',payload=NULL,finished_at=? WHERE job_id=?",(now,job_id))
            row=self._authenticated(job_id,digest)
        return self._public(row)

    def _cancel(self, job_id, digest):
        now = time.time()
        self._prune(now)
        row = self._authenticated(job_id, digest)
        signal = row['status'] == 'running' and not row['cancel_requested']
        if row['status'] == 'queued':
            self._connection.execute('''UPDATE chat_jobs SET status='cancelled',error='cancelled',
                cancel_requested=1,payload=NULL,finished_at=? WHERE job_id=?''', (now, job_id))
        elif signal:
            self._connection.execute('UPDATE chat_jobs SET cancel_requested=1 WHERE job_id=?', (job_id,))
        row = self._connection.execute('SELECT * FROM chat_jobs WHERE job_id=?', (job_id,)).fetchone()
        self._prune(now)
        return self._public(row), signal

    def _claim(self):
        rows=self._claim_batch(1)
        return rows[0] if rows else None

    def _claim_batch(self,limit):
        if type(limit) is not int or not 1<=limit<=64:raise ValueError('invalid dispatch batch')
        now = time.time()
        self._prune(now)
        if self._blocked_reason == 'authentication_required':
            return []
        rows = self._connection.execute(
            "SELECT * FROM chat_jobs WHERE status='queued' ORDER BY seq LIMIT ?",(limit,)).fetchall()
        self._connection.executemany("UPDATE chat_jobs SET status='running',started_at=? WHERE job_id=?",
                                     [(now,row['job_id']) for row in rows])
        # Stage waiting uses the original acceptance lifetime, not another two
        # hours added after a job finally leaves the durable queue.
        return [(row['job_id'],json.loads(row['payload']),
                 min(self.execution_timeout,max(0,row['queue_expires_at']+65-now))) for row in rows]

    def _finish(self, job_id, state, result, size, error):
        now = time.time()
        row = self._connection.execute('SELECT cancel_requested,status FROM chat_jobs WHERE job_id=?',
                                       (job_id,)).fetchone()
        if row is None or row['status'] != 'running':
            return
        if row['cancel_requested']:
            state, result, size, error = 'cancelled', None, 0, 'cancelled'
        self._connection.execute('''UPDATE chat_jobs SET status=?,payload=NULL,result=?,result_bytes=?,
            error=?,finished_at=? WHERE job_id=? AND status='running' ''',
            (state, result, size, error, now, job_id))
        self._prune(now)

    def _snapshot(self):
        now = time.time()
        self._prune(now)
        counts = {row[0]: row[1] for row in self._connection.execute(
            'SELECT status,count(*) FROM chat_jobs GROUP BY status')}
        total = self._connection.execute('SELECT coalesce(sum(result_bytes),0) FROM chat_jobs').fetchone()[0]
        queued, running = counts.get('queued', 0), counts.get('running', 0)
        oldest = self._connection.execute("SELECT min(created_at) FROM chat_jobs WHERE status='queued'").fetchone()[0]
        receipts = self._connection.execute('SELECT count(*) FROM chat_job_receipts').fetchone()[0]
        return {**{state: counts.get(state, 0) for state in
                   ('queued', 'running', 'completed', 'failed', 'cancelled', 'expired')},
                'pending': queued + running, 'stored_result_bytes': total, 'receipts': receipts,
                'oldest_queue_seconds': max(0, now - oldest) if oldest is not None else 0}

    @property
    def healthy(self):
        return (self._started and not self._closing and not self._closed and not self._fatal
                and any(not task.done() for task in self._tasks))

    def _ensure_open(self):
        if not self.healthy:
            raise JobError(503, 'job_queue_unavailable')

    async def start(self):
        async with self._lifecycle_lock:
            if self._started and not self._closed:
                return
            if self._closed:
                raise JobError(503, 'job_queue_unavailable')
            try:
                if self.dispatcher_lock:
                    self.dispatcher_lock.parent.mkdir(parents=True,exist_ok=True)
                    # Verify permissions even while paused/standing by. A worker
                    # that cannot later claim the queue must fail before cutover.
                    with self.dispatcher_lock.open('a'):pass
                self._stats['restarted_failed'] += await self._db(self._open)
            except BaseException:
                if self._connection is not None:
                    await self._db(self._close_db)
                self._executor.shutdown(wait=False)
                self._closed = True
                raise
            self._started = True
            self._tasks = [asyncio.create_task(self._worker(), name='chat-session-dispatcher')]

    async def submit(self, payload, owner, token):
        self._ensure_open()
        job_id, digest = self._token(token)
        if not isinstance(owner, str) or not 1 <= len(owner) <= 256:
            raise JobError(400, 'invalid_job_owner')
        encoded, _ = self._encode(payload, 32768, 'invalid_job_payload')
        payload_hash = hashlib.sha256(encoded.encode('utf-8')).hexdigest()
        await self._backend_ready()
        try:
            output, created = await self._db(self._transaction, self._submit,
                                             encoded, payload_hash, owner, job_id, digest, self._blocked_reason)
        except JobError as exc:
            self._stats[exc.code] += 1
            raise
        self._stats['submitted' if created else 'idempotent'] += 1
        self._wake.set()
        return output

    async def submit_related(self, parent_id, parent_token, owner, child_token):
        """Authenticate parent and create its deterministic child in one transaction."""
        from .related_suggestions import parent_payload
        self._ensure_open()
        _,parent_digest=self._token(parent_token,lookup=True)
        job_id,digest=self._token(child_token)
        if not isinstance(parent_id,str) or re.fullmatch(r'[0-9a-f]{32}',parent_id) is None:
            raise JobError(404,'job_not_found')
        await self._backend_ready()
        blocked_reason = self._blocked_reason
        def create():
            self._prune(time.time())
            parent=self._authenticated(parent_id,parent_digest)
            if parent['owner']!=owner:raise JobError(404,'job_not_found')
            if parent['status']!='completed':raise JobError(409,'parent_not_completed')
            try:payload=parent_payload(json.loads(parent['result']))
            except (ValueError,KeyError,TypeError):raise JobError(400,'invalid_related_parent') from None
            payload['expires_at']=parent['finished_at']+30
            encoded,_=self._encode(payload,32768,'invalid_job_payload')
            return self._submit(encoded,hashlib.sha256(encoded.encode()).hexdigest(),owner,job_id,digest,blocked_reason,expires_at=payload['expires_at'])
        output,created=await self._db(self._transaction,create)
        self._stats['submitted' if created else 'idempotent']+=1
        self._wake.set()
        return {**output,'request_token':child_token}

    async def get(self, job_id, token):
        _, digest = self._token(token, lookup=True)
        if not isinstance(job_id, str) or re.fullmatch(r'[0-9a-f]{32}', job_id) is None:
            raise JobError(404, 'job_not_found')
        self._ensure_open()
        return await self._db(self._transaction, self._get, job_id, digest)

    def _notify(self, job_id):
        for event in self._listeners.get(job_id, ()):
            event.set()

    async def wait(self, job_id, token, previous_state, *, timeout=10.):
        # Authenticate before allocating a listener. Register before the second
        # read so completion between authentication and registration is not lost.
        if previous_state not in {'queued','running'} or not 0<timeout<=10:
            raise JobError(400,'invalid_job_wait')
        deadline=time.monotonic()+timeout
        value=await self.get(job_id,token)
        if value['status']!=previous_state or time.monotonic()>=deadline:return value
        if self._listener_count>=1000 or len(self._listeners.get(job_id,()))>=2:
            raise JobError(429,'job_waiters_full')
        event=asyncio.Event()
        listeners=self._listeners.setdefault(job_id,set())
        listeners.add(event);self._listener_count+=1
        self._stats['peak_status_waiters']=max(self._stats['peak_status_waiters'],self._listener_count)
        try:
            value=await self.get(job_id,token)
            if value['status']!=previous_state or time.monotonic()>=deadline:return value
            try:
                async with asyncio.timeout(max(0,deadline-time.monotonic())):await event.wait()
            except TimeoutError:pass
            return await self.get(job_id,token)
        finally:
            listeners.discard(event);self._listener_count-=1
            if not listeners and self._listeners.get(job_id) is listeners:
                self._listeners.pop(job_id,None)

    @staticmethod
    async def _drain_on_cancel(task):
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            while not task.done():
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    pass
                except Exception:
                    break
            if task.done() and not task.cancelled():
                task.exception()
            raise

    async def cancel(self, job_id, token):
        _, digest = self._token(token, lookup=True)
        if not isinstance(job_id, str) or re.fullmatch(r'[0-9a-f]{32}', job_id) is None:
            raise JobError(404, 'job_not_found')
        self._ensure_open()
        # Disconnecting the DELETE request must not leave a persisted cancellation
        # flag without actually signalling its running handler.
        return await self._drain_on_cancel(asyncio.create_task(self._cancel_and_signal(job_id, digest)))

    async def _cancel_and_signal(self, job_id, digest):
        async with self._control_lock:
            self._ensure_open()
            output, signal = await self._db(self._transaction, self._cancel, job_id, digest)
            active = self._running.get(job_id)
            if signal and active is not None and active.reason is None:
                active.reason = 'cancelled'
                active.task.cancel()
            self._notify(job_id)
        self._wake.set()
        return output

    async def status(self):
        self._ensure_open()
        output = await self._db(self._transaction, self._snapshot)
        return {**output, 'workers': self.workers, 'max_pending': self.max_pending,
                'max_sessions':self.workers,'dispatcher_tasks':len(self._tasks),
                'max_owner_pending': self.max_owner_pending, 'max_finished': self.max_finished,
                'max_result_bytes': self.max_result_bytes,
                'max_stored_result_bytes': self.max_stored_result_bytes,
                'max_receipts': self.max_receipts, 'receipt_ttl': self.receipt_ttl,
                'result_ttl': self.result_ttl, 'queue_ttl': self.queue_ttl,
                'execution_timeout': self.execution_timeout, 'backend_ready': self._ready_value,
                'blocked_reason': self._blocked_reason,
                'status_waiters':self._listener_count,'max_status_waiters':1000,
                'dispatcher_owner':self._dispatcher_fd is not None if self.dispatcher_lock else True,
                'dispatch_paused':bool(self.dispatch_pause and self.dispatch_pause.exists()),
                'counters': dict(self._stats)}

    async def _backend_ready(self):
        if self.ready_async is None:
            return self._blocked_reason is None
        async with self._ready_lock:
            if time.monotonic() - self._ready_checked < 1:
                return self._ready_value
            try:
                checked_at = time.monotonic()
                async with asyncio.timeout(2):
                    value = await self.ready_async()
                # A turn can fail while this older health request is in flight.
                if self._ready_checked > checked_at:
                    return self._ready_value
                self._ready_value = value.get('ready') is True if isinstance(value,dict) else bool(value)
                if self._ready_value:
                    self._blocked_reason = None
                elif isinstance(value,dict) and value.get('state') == 'authentication_required':
                    self._blocked_reason = 'authentication_required'
            except Exception:
                self._ready_value = False
            self._ready_checked = time.monotonic()
            self._stats['readiness_checks'] += 1
            if not self._ready_value:
                self._stats['readiness_unavailable'] += 1
            return self._ready_value

    async def _invoke(self, payload, remaining):
        async with asyncio.timeout(remaining):
            return await self.handler(payload)

    async def _worker(self):
        try:
            await self._work_loop()
        except Exception:
            # An unavailable store cannot safely acknowledge further executions.
            # Already persisted running rows recover as explicit restart failures.
            self._fatal = True
            self._stats['worker_internal_error'] += 1
            self._stop.set()
            self._wake.set()

    async def _work_loop(self):
        while not self._closing and not self._fatal:
            await self._remote_cancellations()
            if not await self._dispatch_allowed():
                try: await asyncio.wait_for(self._stop.wait(), .25)
                except TimeoutError: pass
                continue
            if len(self._running)>=self.workers:
                self._wake.clear()
                # Completion can occur after this snapshot: it sets _wake.
                if len(self._running)>=self.workers:
                    try: await asyncio.wait_for(self._wake.wait(), 1)
                    except TimeoutError: pass
                continue
            if not await self._backend_ready():
                await self._db(self._transaction, self._prune, time.time())
                try:
                    await asyncio.wait_for(self._stop.wait(), 1)
                except TimeoutError:
                    pass
                continue
            async with self._control_lock:
                if self._closing or self._fatal:
                    return
                self._wake.clear()
                claimed = await self._db(self._transaction, self._claim_batch,min(64,self.workers-len(self._running)))
                for job_id, payload, remaining in claimed:
                    self._notify(job_id)
                    active = _Running(asyncio.create_task(self._invoke(payload,remaining)))
                    self._running[job_id] = active
                    self._stats['executed'] += 1
                    self._stats['peak_running'] = max(self._stats['peak_running'], len(self._running))
                    completion=asyncio.create_task(self._complete(job_id,active))
                    self._completions.add(completion)
                    completion.add_done_callback(self._completions.discard)
            if not claimed:
                try:
                    await asyncio.wait_for(self._wake.wait(), 1)
                except TimeoutError:
                    pass
                continue
            # Only admission is serialized. Sessions suspend independently at
            # dependency queues; no whole-question worker is held until reply.

    async def _complete(self,job_id,active):
        try:
            await self._finish_active(job_id,active)
        except Exception:
            self._fatal=True
            self._stats['worker_internal_error']+=1
            self._stop.set()
            self._wake.set()

    async def _finish_active(self,job_id,active):
        state, encoded, size, error = 'completed', None, 0, None
        try:
            result = await active.task
            try:
                encoded, size = self._encode(result, self.max_result_bytes, 'invalid_job_result')
            except JobError:
                state, error = 'failed', 'invalid_job_result'
        except asyncio.CancelledError:
            state, error = ('cancelled', 'cancelled') if active.reason == 'cancelled' else (
                'failed', 'stopped' if active.reason == 'stopped' else 'job_failed')
        except TimeoutError:
            state, error = 'failed', 'execution_timeout'
        except Exception as failure:
            safe_codes={'chat_busy','dependency_busy','dependency_timeout','vector_unavailable',
                        'chat_timeout','chat_failed','chat_unavailable','chat_invalid_response',
                        'capacity_exceeded','deadline_exceeded','server_overloaded','model_initialization_timeout','authentication_required'}
            code=getattr(failure,'code',None)
            state,error='failed',code if isinstance(code,str) and code in safe_codes else 'job_failed'
            if error == 'authentication_required':
                self._ready_value, self._blocked_reason = False, error
                self._ready_checked = time.monotonic()
        async with self._control_lock:
            # Cancellation can arrive after the handler returns but before
            # persistence. Never publish a result for an accepted cancellation.
            if active.reason is not None:
                state, error = ('cancelled', 'cancelled') if active.reason == 'cancelled' else ('failed', 'stopped')
            if state != 'completed':
                encoded, size = None, 0
            try:
                await self._db(self._transaction, self._finish, job_id, state, encoded, size, error)
                self._notify(job_id)
            finally:
                self._running.pop(job_id, None)
            self._stats[state] += 1
            if error is not None:
                self._stats['error_' + error] += 1
            if state == 'failed' and error != 'authentication_required':
                # A cached healthy response must not turn a sudden backend
                # outage into a tight loop that fails the entire durable queue.
                self._ready_checked = float('-inf')
        self._wake.set()

    def _close_db(self):
        if self._connection is not None:
            try:
                self._connection.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            finally:
                self._connection.close()
                self._connection = None

    async def aclose(self):
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        await self._drain_on_cancel(self._close_task)

    async def _close(self):
        async with self._lifecycle_lock:
            if self._closed:
                return
            async with self._control_lock:
                self._closing = True
                self._stop.set()
                self._wake.set()
                for job_id in tuple(self._listeners):self._notify(job_id)
                for active in self._running.values():
                    if active.reason is None:
                        active.reason = 'stopped'
                        active.task.cancel()
            if self._tasks:
                await asyncio.gather(*self._tasks)
            if self._completions:
                await asyncio.gather(*tuple(self._completions))
            self._release_dispatcher()
            if self._started:
                await self._db(self._close_db)
            self._executor.shutdown(wait=False)
            self._closed = True
