"""Persistent account and mail state, separate from the read-only catalogue."""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import hmac
import json
from pathlib import Path
import secrets
import sqlite3
import time
import uuid

from .cooperation import fail, email, text, fingerprint


SCHEMA = '''
CREATE TABLE IF NOT EXISTS members (
 member_id TEXT PRIMARY KEY, email TEXT UNIQUE, name TEXT, organization TEXT,
 verified_at REAL NOT NULL, status TEXT NOT NULL DEFAULT 'active');
CREATE TABLE IF NOT EXISTS sessions (
 token_hash TEXT PRIMARY KEY, csrf TEXT NOT NULL, member_id TEXT REFERENCES members,
 created_at REAL NOT NULL, expires_at REAL NOT NULL, authenticated_at REAL);
CREATE TABLE IF NOT EXISTS email_challenges (
 challenge_id TEXT PRIMARY KEY, browser_hash TEXT NOT NULL, email TEXT NOT NULL,
 address_hash TEXT NOT NULL, ip_hash TEXT NOT NULL, name TEXT NOT NULL, organization TEXT NOT NULL,
 code_hash TEXT NOT NULL, created_at REAL NOT NULL, expires_at REAL NOT NULL,
 attempts INTEGER NOT NULL DEFAULT 0, consumed INTEGER NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS challenge_address ON email_challenges(address_hash, created_at);
CREATE INDEX IF NOT EXISTS challenge_ip ON email_challenges(ip_hash, created_at);
CREATE TABLE IF NOT EXISTS requests (
 request_id TEXT PRIMARY KEY, member_id TEXT NOT NULL REFERENCES members, version INTEGER NOT NULL,
 snapshot TEXT NOT NULL, fingerprint TEXT NOT NULL, member_fingerprint TEXT NOT NULL,
 dataset_id TEXT NOT NULL, source_id TEXT NOT NULL, recipient_hash TEXT NOT NULL,
 address_hash TEXT NOT NULL, status TEXT NOT NULL, created_at REAL NOT NULL, expires_at REAL NOT NULL,
 consent_at REAL, event_at REAL NOT NULL DEFAULT 0, redacted INTEGER NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS requests_owner ON requests(member_id, created_at);
CREATE INDEX IF NOT EXISTS requests_limit ON requests(address_hash, consent_at);
CREATE TABLE IF NOT EXISTS outbox (
 message_id TEXT PRIMARY KEY, kind TEXT NOT NULL, challenge_id TEXT UNIQUE, request_id TEXT UNIQUE,
 payload TEXT NOT NULL, recipient_hash TEXT NOT NULL, status TEXT NOT NULL,
 created_at REAL NOT NULL, expires_at REAL NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
 first_attempt REAL, next_attempt REAL NOT NULL, lease_until REAL,
 provider_id TEXT UNIQUE, last_error TEXT, ambiguous INTEGER NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS outbox_due ON outbox(status, next_attempt);
CREATE TABLE IF NOT EXISTS webhook_events (
 event_id TEXT PRIMARY KEY, provider_id TEXT NOT NULL, event_type TEXT NOT NULL, event_at REAL NOT NULL,
 received_at REAL NOT NULL, applied INTEGER NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS webhook_provider ON webhook_events(provider_id, event_at);
CREATE TABLE IF NOT EXISTS suppressions (
 recipient_hash TEXT PRIMARY KEY, reason TEXT NOT NULL, created_at REAL NOT NULL);
'''


class CooperationStore:
    def __init__(self, path, secret, *, clock=time.time, max_queue=100, idempotent_delivery=True):
        if len(secret) < 32: raise ValueError('cooperation secret needs at least 32 characters')
        self.path, self.secret, self.clock, self.max_queue = Path(path), secret.encode(), clock, max_queue
        self.idempotent_delivery = idempotent_delivery
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as db:
            db.execute('PRAGMA journal_mode=WAL')
            db.executescript(SCHEMA)

    @contextmanager
    def connection(self):
        db = sqlite3.connect(self.path, timeout=3, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA foreign_keys=ON')
        try: yield db
        finally: db.close()

    @contextmanager
    def transaction(self):
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            try: yield db
            except BaseException:
                db.rollback(); raise
            else: db.commit()

    def digest(self, value):
        return hmac.new(self.secret, value.encode(), hashlib.sha256).hexdigest()

    @staticmethod
    def member(row):
        return {key: row[key] for key in ('member_id', 'email', 'name', 'organization')} | {'email_verified_at': row['verified_at']}

    def _new_session(self, db, member_id=None):
        now = self.clock(); token = secrets.token_urlsafe(32); csrf = secrets.token_urlsafe(32)
        db.execute('INSERT INTO sessions VALUES (?,?,?,?,?,?)',
                   (self.digest(token), csrf, member_id, now, now + 86400 if member_id else now + 1800, now if member_id else None))
        value = {'token': token, 'csrf': csrf, 'member': None}
        if member_id:
            value['member'] = self.member(db.execute('SELECT * FROM members WHERE member_id=?', (member_id,)).fetchone())
        return value

    def new_session(self):
        with self.transaction() as db:
            db.execute('DELETE FROM sessions WHERE expires_at<=?', (self.clock(),))
            if db.execute('SELECT COUNT(*) FROM sessions').fetchone()[0] >= 10000:
                fail(503, 'session_capacity', '잠시 후 다시 시도해 주세요.')
            return self._new_session(db)

    def session(self, token):
        with self.connection() as db: return self._session(db, token)

    def _session(self, db, token):
        if not token or not isinstance(token, str): return None
        row = db.execute('SELECT * FROM sessions WHERE token_hash=? AND expires_at>?', (self.digest(token), self.clock())).fetchone()
        if row is None: return None
        value = dict(row); value['member'] = None
        if row['member_id']:
            member = db.execute("SELECT * FROM members WHERE member_id=? AND status='active'", (row['member_id'],)).fetchone()
            if member is None: return None
            value['member'] = self.member(member)
        return value

    def _queue_capacity(self, db):
        if db.execute("SELECT COUNT(*) FROM outbox WHERE status IN ('queued','retrying','sending')").fetchone()[0] >= self.max_queue:
            fail(503, 'mail_queue_full', '발송 요청이 많습니다. 잠시 후 다시 시도해 주세요.')

    def _enqueue(self, db, kind, payload, expires, *, challenge_id=None, request_id=None):
        self._queue_capacity(db)
        recipient = self.digest('email:' + email(payload['to'][0]))
        if db.execute('SELECT 1 FROM suppressions WHERE recipient_hash=?', (recipient,)).fetchone():
            fail(409, 'recipient_suppressed', '해당 주소로는 발송할 수 없습니다. 공식 문의 경로를 이용해 주세요.')
        message_id = str(uuid.uuid4()); now = self.clock()
        db.execute('INSERT INTO outbox(message_id,kind,challenge_id,request_id,payload,recipient_hash,status,created_at,expires_at,next_attempt) VALUES(?,?,?,?,?,?,?,?,?,?)',
                   (message_id, kind, challenge_id, request_id, json.dumps(payload, ensure_ascii=False), recipient, 'queued', now, expires, now))
        return message_id

    def start_code(self, token, address, name, organization, ip, from_address):
        address = email(address); name = text(name, 100, line=True, required=False)
        organization = text(organization or '개인', 200, line=True)
        now = self.clock(); address_hash = self.digest('email:' + address); ip_hash = self.digest('ip:' + ip)
        with self.transaction() as db:
            if not self._session(db, token): fail(401, 'session_required', '화면을 다시 열어 주세요.')
            recent = db.execute('SELECT MAX(created_at),COUNT(*) FROM email_challenges WHERE address_hash=? AND created_at>?', (address_hash, now - 3600)).fetchone()
            ip_count = db.execute('SELECT COUNT(*) FROM email_challenges WHERE ip_hash=? AND created_at>?', (ip_hash, now - 3600)).fetchone()[0]
            if recent[1] >= 5 or ip_count >= 5 or (recent[0] is not None and now - recent[0] < 60):
                fail(429, 'code_rate_limit', '인증번호는 60초 후 재전송할 수 있으며 시간당 5회까지 요청할 수 있습니다.')
            browser = self.digest(token)
            db.execute("UPDATE outbox SET status='cancelled',payload='{}' WHERE challenge_id IN (SELECT challenge_id FROM email_challenges WHERE address_hash=? OR browser_hash=?) AND status IN ('queued','retrying')", (address_hash, browser))
            db.execute('UPDATE email_challenges SET consumed=1 WHERE address_hash=? OR browser_hash=?', (address_hash, browser))
            challenge_id = str(uuid.uuid4()); code = f'{secrets.randbelow(1000000):06d}'
            db.execute('INSERT INTO email_challenges(challenge_id,browser_hash,email,address_hash,ip_hash,name,organization,code_hash,created_at,expires_at) VALUES(?,?,?,?,?,?,?,?,?,?)',
                       (challenge_id, browser, address, address_hash, ip_hash, name, organization, self.digest(challenge_id + ':' + code), now, now + 600))
            payload = {'from': from_address, 'to': [address], 'subject': '[데이터이음] 이메일 인증번호',
                       'text': f'인증번호: {code}\n\n10분 안에 데이터이음에서 입력해 주세요. 직접 요청하지 않았다면 이 메일을 무시해 주세요.'}
            self._enqueue(db, 'email_verification', payload, now + 600, challenge_id=challenge_id)
            return {'challenge_id': challenge_id, 'expires_at': now + 600, 'retry_after': 60}

    def verify_code(self, token, challenge_id, code):
        now = self.clock(); error = False; result = None
        with self.transaction() as db:
            session = self._session(db, token)
            row = db.execute('SELECT * FROM email_challenges WHERE challenge_id=? AND browser_hash=?', (challenge_id, self.digest(token))).fetchone()
            if not session or not row or row['consumed'] or row['expires_at'] <= now or row['attempts'] >= 5:
                error = True
            else:
                db.execute('UPDATE email_challenges SET attempts=attempts+1 WHERE challenge_id=?', (challenge_id,))
                if not isinstance(code, str) or not hmac.compare_digest(row['code_hash'], self.digest(challenge_id + ':' + code)):
                    error = True
                else:
                    member = db.execute("SELECT * FROM members WHERE email=? AND status='active'", (row['email'],)).fetchone()
                    if not member and not row['name']:
                        fail(400, 'signup_profile_required', '처음 가입하는 이메일입니다. 가입을 선택하고 이름을 입력한 뒤 인증번호를 다시 받아 주세요.')
                    db.execute('UPDATE email_challenges SET consumed=1,code_hash=? WHERE challenge_id=?', ('', challenge_id))
                    mid = member['member_id'] if member else str(uuid.uuid4())
                    if not member:
                        db.execute('INSERT INTO members(member_id,email,name,organization,verified_at) VALUES(?,?,?,?,?)', (mid, row['email'], row['name'], row['organization'], now))
                    db.execute('DELETE FROM sessions WHERE token_hash=?', (self.digest(token),))
                    result = self._new_session(db, mid)
        if error: fail(400, 'code_invalid', '인증번호가 맞지 않거나 만료되었습니다. 최대 5회까지 확인할 수 있습니다.')
        return result

    def logout(self, token):
        with self.transaction() as db: db.execute('DELETE FROM sessions WHERE token_hash=?', (self.digest(token),))

    def update_member(self, member_id, name, organization):
        name = text(name, 100, line=True); organization = text(organization or '개인', 200, line=True)
        with self.transaction() as db:
            db.execute("UPDATE members SET name=?,organization=? WHERE member_id=? AND status='active'", (name, organization, member_id))
            row = db.execute("SELECT * FROM members WHERE member_id=? AND status='active'", (member_id,)).fetchone()
            if not row: fail(401, 'account_required', '로그인이 필요합니다.')
            return self.member(row)

    def delete_member(self, token):
        with self.transaction() as db:
            session = self._session(db, token)
            if not session or not session['member']: fail(401, 'account_required', '로그인이 필요합니다.')
            if self.clock() - session['authenticated_at'] > 600: fail(403, 'recent_login_required', '탈퇴하려면 이메일 인증으로 다시 로그인해 주세요.')
            mid = session['member']['member_id']; address_hash = self.digest('email:' + session['member']['email'])
            db.execute("UPDATE outbox SET status=CASE WHEN status='sending' THEN 'unknown' WHEN status IN ('queued','retrying') THEN 'cancelled' ELSE status END,payload='{}' WHERE request_id IN (SELECT request_id FROM requests WHERE member_id=?) OR challenge_id IN (SELECT challenge_id FROM email_challenges WHERE address_hash=?)", (mid, address_hash))
            db.execute("UPDATE requests SET status=CASE WHEN status IN ('draft','queued','retrying') THEN 'cancelled' WHEN status='sending' THEN 'unknown' ELSE status END,snapshot='{}',redacted=1 WHERE member_id=?", (mid,))
            # Preserve only hashed quota evidence until daily cleanup; deletion cannot reset issuance limits.
            db.execute("UPDATE email_challenges SET email='',name='',organization='',code_hash='',consumed=1 WHERE address_hash=?", (address_hash,))
            db.execute('DELETE FROM sessions WHERE member_id=?', (mid,))
            db.execute("UPDATE members SET email=NULL,name=NULL,organization=NULL,status='deleted' WHERE member_id=?", (mid,))

    def save_preview(self, member_id, value, request_id=None):
        now = self.clock()
        with self.transaction() as db:
            member = db.execute("SELECT * FROM members WHERE member_id=? AND status='active'", (member_id,)).fetchone()
            if not member: fail(401, 'account_required', '로그인이 필요합니다.')
            if value['member_fingerprint'] != fingerprint({key: member[key] for key in ('email', 'name', 'organization')}):
                fail(409, 'profile_changed', '회원정보가 바뀌었습니다. 다시 미리보기를 확인해 주세요.')
            if request_id:
                row = self._request(db, member_id, request_id)
                if row['status'] != 'draft': fail(409, 'request_already_sent', '이미 확인한 요청은 수정할 수 없습니다.')
                version = row['version'] + 1
                db.execute('DELETE FROM requests WHERE request_id=?', (request_id,))
            else:
                if db.execute("SELECT COUNT(*) FROM requests WHERE member_id=? AND status='draft' AND expires_at>?", (member_id, now)).fetchone()[0] >= 20:
                    fail(429, 'preview_limit', '열린 미리보기가 많습니다. 기존 요청을 수정해 주세요.')
                request_id = str(uuid.uuid4()); version = 1
            db.execute('INSERT INTO requests(request_id,member_id,version,snapshot,fingerprint,member_fingerprint,dataset_id,source_id,recipient_hash,address_hash,status,created_at,expires_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                       (request_id, member_id, version, json.dumps(value, ensure_ascii=False), value['fingerprint'], value['member_fingerprint'],
                        value['input']['dataset_id'], value['source_id'], self.digest('email:' + value['payload']['to'][0]),
                        self.digest('email:' + member['email']), 'draft', now, now + 1800))
            return self._view(self._request(db, member_id, request_id))

    @staticmethod
    def _request(db, member_id, request_id):
        row = db.execute('SELECT * FROM requests WHERE request_id=? AND member_id=?', (request_id, member_id)).fetchone()
        if row is None: fail(404, 'not_found', '요청을 찾을 수 없습니다.')
        return row

    @staticmethod
    def _view(row):
        return {**json.loads(row['snapshot']), **{key: row[key] for key in ('request_id', 'version', 'status', 'created_at', 'expires_at', 'consent_at', 'redacted')}}

    def get_request(self, member_id, request_id):
        with self.connection() as db: return self._view(self._request(db, member_id, request_id))

    def list_requests(self, member_id, page=1):
        with self.connection() as db:
            rows = db.execute('SELECT * FROM requests WHERE member_id=? ORDER BY created_at DESC,request_id DESC LIMIT 21 OFFSET ?', (member_id, (page - 1) * 20)).fetchall()
            return {'requests': [self._view(row) for row in rows[:20]], 'has_more': len(rows) > 20, 'page': page, 'retention_days': 30}

    def confirm(self, member_id, request_id, version, expected_fingerprint):
        now = self.clock()
        with self.transaction() as db:
            row, snapshot = self._confirmation(db, member_id, request_id, version, expected_fingerprint)
            if snapshot is None: return self._view(row)
            self._enqueue(db, 'cooperation_request', snapshot['payload'], now + 86400, request_id=request_id)
            db.execute("UPDATE requests SET status='queued',consent_at=? WHERE request_id=?", (now, request_id))
            return self._view(self._request(db, member_id, request_id))

    def _confirmation(self, db, member_id, request_id, version, expected_fingerprint):
        """Shared confirmation rules; caller owns the transaction and dispatch mechanism."""
        row = self._request(db, member_id, request_id); now = self.clock()
        if type(version) is not int or row['version'] != version:
            fail(409, 'preview_changed', '새 미리보기를 확인해 주세요.')
        if row['consent_at'] is not None: return row, None
        if row['expires_at'] <= now: fail(410, 'preview_expired', '미리보기가 만료되었습니다. 다시 확인해 주세요.')
        if row['status'] != 'draft': fail(409, 'request_unavailable', '요청을 다시 확인해 주세요.')
        snapshot = json.loads(row['snapshot'])
        if row['fingerprint'] != expected_fingerprint or not snapshot['sendable']:
            fail(409, 'preview_changed', '메일 양식 또는 수신처가 바뀌었습니다. 새 미리보기를 확인해 주세요.')
        member = db.execute("SELECT * FROM members WHERE member_id=? AND status='active'", (member_id,)).fetchone()
        if not member or row['member_fingerprint'] != fingerprint({key: member[key] for key in ('email', 'name', 'organization')}):
            fail(409, 'profile_changed', '회원정보가 바뀌었습니다. 새 미리보기를 확인해 주세요.')
        if db.execute('SELECT COUNT(*) FROM requests WHERE address_hash=? AND consent_at>?', (row['address_hash'], now - 86400)).fetchone()[0] >= 5:
            fail(429, 'daily_limit', '24시간 동안 최대 5건까지 보낼 수 있습니다.')
        if db.execute('SELECT 1 FROM requests WHERE address_hash=? AND dataset_id=? AND source_id=? AND consent_at>?', (row['address_hash'], row['dataset_id'], row['source_id'], now - 86400)).fetchone():
            fail(429, 'duplicate_request', '같은 자료와 기관에는 24시간에 한 번 보낼 수 있습니다.')
        if db.execute('SELECT 1 FROM suppressions WHERE recipient_hash=?', (row['recipient_hash'],)).fetchone():
            fail(409, 'recipient_suppressed', '해당 주소로는 발송할 수 없습니다. 공식 문의 경로를 이용해 주세요.')
        return row, snapshot

    def _set_status(self, db, row, status, error=None):
        db.execute('UPDATE outbox SET status=?,last_error=?,lease_until=NULL WHERE message_id=?', (status, error, row['message_id']))
        if row['request_id']:
            db.execute('UPDATE requests SET status=? WHERE request_id=? AND status NOT IN (\'delivered\',\'bounced\',\'complained\')', (status, row['request_id']))

    def claim(self):
        now = self.clock()
        with self.transaction() as db:
            # Leases survive process restarts. Only the single mail worker claims messages.
            if self.idempotent_delivery:
                db.execute("UPDATE outbox SET status='retrying',ambiguous=1 WHERE status='sending' AND lease_until<=?", (now,))
            else:
                for row in db.execute("SELECT * FROM outbox WHERE status='sending' AND lease_until<=?", (now,)).fetchall():
                    self._set_status(db, row, 'unknown', 'delivery_interrupted')
                    db.execute('UPDATE outbox SET ambiguous=1 WHERE message_id=?', (row['message_id'],))
            rows = db.execute("SELECT * FROM outbox WHERE status IN ('queued','retrying') AND next_attempt<=? ORDER BY created_at LIMIT ?", (now, self.max_queue)).fetchall()
            for row in rows:
                if row['ambiguous'] and not self.idempotent_delivery:
                    self._set_status(db, row, 'unknown', 'delivery_uncertain'); continue
                if row['first_attempt'] is not None and now - row['first_attempt'] >= 86400:
                    self._set_status(db, row, 'unknown', 'idempotency_expired'); continue
                if row['expires_at'] <= now:
                    self._set_status(db, row, 'unknown' if row['ambiguous'] else 'cancelled', 'expired'); continue
                if row['attempts'] >= 5 or (row['first_attempt'] is not None and now - row['first_attempt'] >= 3600):
                    self._set_status(db, row, 'unknown' if row['ambiguous'] else 'failed', 'retry_exhausted'); continue
                if db.execute('SELECT 1 FROM suppressions WHERE recipient_hash=?', (row['recipient_hash'],)).fetchone():
                    self._set_status(db, row, 'failed', 'recipient_suppressed'); continue
                if row['challenge_id']:
                    challenge = db.execute('SELECT * FROM email_challenges WHERE challenge_id=?', (row['challenge_id'],)).fetchone()
                    if not challenge or challenge['consumed'] or challenge['expires_at'] <= now:
                        self._set_status(db, row, 'cancelled', 'challenge_expired'); continue
                db.execute("UPDATE outbox SET status='sending',attempts=attempts+1,first_attempt=COALESCE(first_attempt,?),lease_until=? WHERE message_id=?", (now, now + 30, row['message_id']))
                if row['request_id']: db.execute("UPDATE requests SET status='sending' WHERE request_id=?", (row['request_id'],))
                value = dict(db.execute('SELECT * FROM outbox WHERE message_id=?', (row['message_id'],)).fetchone())
                value['payload'] = json.loads(value['payload'])
                return value
        return None

    def metrics(self):
        with self.connection() as db:
            return {row['status']: row['n'] for row in db.execute('SELECT status,COUNT(*) AS n FROM outbox GROUP BY status')}

    def accepted(self, message_id, provider_id):
        with self.transaction() as db:
            row = db.execute('SELECT * FROM outbox WHERE message_id=?', (message_id,)).fetchone()
            if not row: return
            db.execute('UPDATE outbox SET provider_id=? WHERE message_id=?', (provider_id, message_id))
            if row['status'] == 'sending': self._set_status(db, row, 'accepted')
            if row['kind'] == 'email_verification':
                db.execute("UPDATE outbox SET payload='{}' WHERE message_id=?", (message_id,))
            self._apply_events(db, provider_id)

    def delivery_failed(self, message_id, code, *, retryable=False, ambiguous=False):
        now = self.clock()
        with self.transaction() as db:
            row = db.execute('SELECT * FROM outbox WHERE message_id=?', (message_id,)).fetchone()
            if not row or row['status'] != 'sending': return
            ambiguous = ambiguous or bool(row['ambiguous'])
            retry = retryable and (self.idempotent_delivery or not ambiguous) and row['attempts'] < 5 and now - row['first_attempt'] < 3600 and row['expires_at'] > now
            status = 'retrying' if retry else ('unknown' if ambiguous else 'failed')
            self._set_status(db, row, status, code)
            db.execute('UPDATE outbox SET next_attempt=?,ambiguous=? WHERE message_id=?',
                       (now + min(600, 30 * 2 ** (row['attempts'] - 1)), int(ambiguous), message_id))

    def event(self, event_id, provider_id, event_type, event_at):
        with self.transaction() as db:
            if db.execute('SELECT 1 FROM webhook_events WHERE event_id=?', (event_id,)).fetchone(): return
            if db.execute('SELECT COUNT(*) FROM webhook_events').fetchone()[0] >= 100000:
                fail(503, 'event_capacity', '이벤트 보관 한도입니다.')
            db.execute('INSERT INTO webhook_events(event_id,provider_id,event_type,event_at,received_at) VALUES(?,?,?,?,?)',
                       (event_id, provider_id, event_type, event_at, self.clock()))
            self._apply_events(db, provider_id)

    def _apply_events(self, db, provider_id):
        message = db.execute('SELECT * FROM outbox WHERE provider_id=?', (provider_id,)).fetchone()
        if not message: return
        ranks = {'queued': 0, 'sending': 1, 'retrying': 1, 'unknown': 1, 'cancelled': 1,
                 'accepted': 2, 'delayed': 3, 'delivered': 4, 'failed': 4, 'bounced': 5, 'complained': 6}
        for event in db.execute('SELECT * FROM webhook_events WHERE provider_id=? AND applied=0 ORDER BY event_at,event_id', (provider_id,)).fetchall():
            status = event['event_type']
            if status in {'bounced', 'complained'}:
                db.execute('INSERT OR IGNORE INTO suppressions VALUES(?,?,?)', (message['recipient_hash'], status, self.clock()))
                pending = db.execute("SELECT * FROM outbox WHERE recipient_hash=? AND status IN ('queued','retrying')", (message['recipient_hash'],)).fetchall()
                for row in pending: self._set_status(db, row, 'failed', 'recipient_suppressed')
            current = db.execute('SELECT status FROM outbox WHERE message_id=?', (message['message_id'],)).fetchone()[0]
            # Delivery and final failure are terminal peers. Their provider event time,
            # not arrival order, decides between them; sent/delayed cannot undo either.
            current_event_at = db.execute('SELECT MAX(event_at) FROM webhook_events WHERE provider_id=? AND event_type=? AND applied=1',
                                         (provider_id, current)).fetchone()[0]
            same_rank_older = (ranks.get(status, -1) == ranks.get(current, 0)
                               and current_event_at is not None and event['event_at'] < current_event_at)
            if ranks.get(status, -1) >= ranks.get(current, 0) and not same_rank_older:
                db.execute('UPDATE outbox SET status=? WHERE message_id=?', (status, message['message_id']))
                if message['request_id']:
                    db.execute('UPDATE requests SET status=?,event_at=MAX(event_at,?) WHERE request_id=?', (status, event['event_at'], message['request_id']))
            db.execute('UPDATE webhook_events SET applied=1 WHERE event_id=?', (event['event_id'],))

    def cleanup(self):
        now = self.clock()
        with self.transaction() as db:
            db.execute('DELETE FROM sessions WHERE expires_at<=?', (now,))
            db.execute("UPDATE outbox SET status='cancelled',payload='{}' WHERE kind='email_verification' AND expires_at<=? AND status IN ('queued','retrying')", (now,))
            db.execute("UPDATE outbox SET payload='{}' WHERE kind='email_verification' AND expires_at<=?", (now,))
            db.execute('DELETE FROM email_challenges WHERE created_at<?', (now - 86400,))
            db.execute("UPDATE requests SET snapshot='{}',redacted=1 WHERE created_at<?", (now - 30 * 86400,))
            db.execute("UPDATE outbox SET payload='{}' WHERE created_at<?", (now - 30 * 86400,))
            db.execute('DELETE FROM webhook_events WHERE received_at<?', (now - 30 * 86400,))
            db.execute('PRAGMA incremental_vacuum(100)')
