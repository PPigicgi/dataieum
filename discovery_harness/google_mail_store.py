"""Website Google identity/session credentials and atomic Gmail dispatch state."""
from __future__ import annotations

import json
import secrets
import uuid

from cryptography.fernet import Fernet, InvalidToken

from .cooperation import fail
from .cooperation_store import CooperationStore

GOOGLE_SCHEMA = '''
CREATE TABLE IF NOT EXISTS google_identities (
 subject TEXT PRIMARY KEY, member_id TEXT UNIQUE NOT NULL REFERENCES members);
CREATE TABLE IF NOT EXISTS google_flows (
 state_hash TEXT PRIMARY KEY, browser_hash TEXT UNIQUE NOT NULL REFERENCES sessions(token_hash) ON DELETE CASCADE,
 secret BLOB NOT NULL, created_at REAL NOT NULL, expires_at REAL NOT NULL, consumed INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS google_credentials (
 token_hash TEXT PRIMARY KEY REFERENCES sessions(token_hash) ON DELETE CASCADE,
 subject TEXT NOT NULL REFERENCES google_identities, secret BLOB NOT NULL, expires_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS google_sends (
 request_id TEXT PRIMARY KEY REFERENCES requests, started_at REAL NOT NULL, provider_id TEXT);
'''


class GoogleMailStore(CooperationStore):
    def __init__(self, path, secret, encryption_key, **kwargs):
        self.cipher = Fernet(encryption_key)
        super().__init__(path, secret, **kwargs)
        with self.connection() as db: db.executescript(GOOGLE_SCHEMA)

    def start_google(self, token):
        now = self.clock()
        flow = {key: secrets.token_urlsafe(32) for key in ('state', 'verifier', 'nonce')}
        with self.transaction() as db:
            if not self._session(db, token): fail(401, 'session_required', '화면을 다시 열어 주세요.')
            browser = self.digest(token)
            old = db.execute('SELECT created_at FROM google_flows WHERE browser_hash=?', (browser,)).fetchone()
            if old and now - old['created_at'] < 5:
                fail(429, 'google_connection_busy', '잠시 후 다시 연결해 주세요.')
            db.execute('DELETE FROM google_flows WHERE browser_hash=?', (browser,))
            db.execute('INSERT INTO google_flows(state_hash,browser_hash,secret,created_at,expires_at) VALUES(?,?,?,?,?)',
                (self.digest(flow['state']), browser, self.cipher.encrypt(json.dumps(flow).encode()), now, now + 600))
        return flow

    def consume_google(self, token, state):
        with self.transaction() as db:
            row = db.execute('SELECT * FROM google_flows WHERE state_hash=? AND browser_hash=?',
                             (self.digest(state), self.digest(token))).fetchone()
            if not self._session(db, token) or not row or row['consumed'] or row['expires_at'] <= self.clock():
                fail(400, 'google_state_invalid', '연결 요청이 만료되었거나 다른 창에서 시작되었습니다. 다시 연결해 주세요.')
            db.execute('UPDATE google_flows SET consumed=1 WHERE state_hash=?', (row['state_hash'],))
            return json.loads(self.cipher.decrypt(row['secret']))

    def finish_google(self, token, state, identity):
        now = self.clock()
        with self.transaction() as db:
            flow = db.execute('SELECT * FROM google_flows WHERE state_hash=? AND browser_hash=? AND consumed=1',
                              (self.digest(state), self.digest(token))).fetchone()
            if not self._session(db, token) or not flow or flow['expires_at'] <= now:
                fail(400, 'google_state_invalid', '연결 요청이 종료되었습니다. 다시 연결해 주세요.')
            # Never attach an existing email-only account to a new Google subject.
            member = db.execute('SELECT m.* FROM google_identities g JOIN members m USING(member_id) WHERE g.subject=?',
                                (identity['sub'],)).fetchone()
            if member and member['status'] == 'active' and member['email'] != identity['email']:
                fail(409, 'google_identity_changed', '계정 이메일이 변경되었습니다. 계정 연결 확인이 필요합니다.')
            if not member or member['status'] != 'active':
                if db.execute('SELECT 1 FROM members WHERE email=?', (identity['email'],)).fetchone():
                    fail(409, 'google_identity_conflict', '같은 이메일의 다른 계정이 있습니다. 계정 연결 확인이 필요합니다.')
                mid = str(uuid.uuid4())
                db.execute('INSERT INTO members(member_id,email,name,organization,verified_at) VALUES(?,?,?,?,?)',
                           (mid, identity['email'], identity['name'], '개인', now))
                db.execute('INSERT OR REPLACE INTO google_identities VALUES(?,?)', (identity['sub'], mid))
            else: mid = member['member_id']
            db.execute('DELETE FROM sessions WHERE token_hash=?', (self.digest(token),))
            session = self._new_session(db, mid)
            db.execute('INSERT INTO google_credentials VALUES(?,?,?,?)',
                (self.digest(session['token']), identity['sub'], self.cipher.encrypt(identity['access_token'].encode()),
                 now + identity['expires_in']))
            return session

    def connected(self, token):
        with self.connection() as db:
            return db.execute('SELECT 1 FROM google_credentials WHERE token_hash=? AND expires_at>?',
                              (self.digest(token), self.clock() + 15)).fetchone() is not None

    def claim_google(self, token, member_id, request_id, version, expected_fingerprint):
        with self.transaction() as db:
            session = self._session(db, token)
            if not session or not session['member'] or session['member']['member_id'] != member_id:
                fail(401, 'account_required', 'Google 계정을 다시 연결해 주세요.')
            row, snapshot = self._confirmation(db, member_id, request_id, version, expected_fingerprint)
            if snapshot is None: return self._view(row), None
            credential = db.execute('SELECT * FROM google_credentials WHERE token_hash=? AND expires_at>?',
                                    (self.digest(token), self.clock() + 15)).fetchone()
            if not credential: fail(401, 'google_reconnect_required', 'Gmail 연결이 만료되었습니다. 다시 연결해 주세요.')
            try: access = self.cipher.decrypt(credential['secret']).decode()
            except InvalidToken: fail(401, 'google_reconnect_required', 'Gmail 계정을 다시 연결해 주세요.')
            db.execute('INSERT INTO google_sends(request_id,started_at) VALUES(?,?)', (request_id, self.clock()))
            db.execute("UPDATE requests SET status='sending',consent_at=? WHERE request_id=?", (self.clock(), request_id))
            return self._view(self._request(db, member_id, request_id)), access

    def finish_send(self, member_id, request_id, status, provider_id):
        if status not in {'accepted', 'failed', 'unknown'}: raise ValueError('invalid Gmail outcome')
        with self.transaction() as db:
            row = self._request(db, member_id, request_id)
            # A completed provider response can resolve a stale/crashed or deleted-account unknown record.
            if row['status'] in {'sending', 'unknown'}:
                db.execute('UPDATE requests SET status=? WHERE request_id=?', (status, request_id))
                db.execute('UPDATE google_sends SET provider_id=? WHERE request_id=?', (provider_id, request_id))
            return self._view(self._request(db, member_id, request_id))

    def cleanup(self):
        super().cleanup()
        with self.transaction() as db:
            db.execute('DELETE FROM google_flows WHERE expires_at<=?', (self.clock(),))
            db.execute('DELETE FROM google_credentials WHERE expires_at<=?', (self.clock(),))
            db.execute("UPDATE requests SET status='unknown' WHERE status='sending' AND request_id IN (SELECT request_id FROM google_sends WHERE started_at<?)",
                       (self.clock() - 60,))
