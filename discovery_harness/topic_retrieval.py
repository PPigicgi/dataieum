"""Read-only, snapshot-bound topic grounding from original stored embeddings.

Topic similarity is an exploration heuristic, never equivalence or causality.
Only confidence.links may establish a dataset's membership in a shown topic.
"""
from contextlib import contextmanager
import base64
import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3
import time


VERSION = 'public-data-topics-v6'
MODEL = 'text-embedding-3-small'
DIMENSIONS = 1536
MAX_BYTES = 2 * 1024 * 1024
QUERY_FLOOR = .15
RELATED_FLOOR = .25


class TopicRetrievalUnavailable(RuntimeError):
    pass


def unavailable():
    return {'status': 'unavailable', 'version': None, 'query_topics': [],
            'related_topics': [], 'relations': []}


def _check(deadline):
    if time.monotonic() >= deadline:
        raise TimeoutError('topic retrieval deadline exceeded')


def _identity(path):
    if path.is_symlink() or not path.is_file():
        raise ValueError('topic snapshot must be a regular file')
    wal = path.with_name(path.name + '-wal')
    if wal.exists() and wal.stat().st_size:
        raise ValueError('topic snapshot has a live WAL')
    stat = path.stat()
    return stat.st_ino, stat.st_size, stat.st_mtime_ns


@contextmanager
def _connect(path, deadline):
    _check(deadline)
    db = sqlite3.connect(path.as_uri() + '?mode=ro&immutable=1', uri=True, timeout=.05)
    try:
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON')
        db.execute('PRAGMA cache_size=-2048')
        db.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
        yield db
    except sqlite3.OperationalError:
        _check(deadline)
        raise
    finally:
        db.close()


def _text(value, maximum):
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ValueError('invalid topic text')
    return value


class TopicRetrieval:
    def __init__(self, vectors_path=None, confidence_path=None):
        self.ready = False
        self.vector_path = Path(vectors_path).absolute() if vectors_path else None
        self.path = Path(confidence_path).absolute() if confidence_path else None
        if self.vector_path is None or self.path is None:
            return
        try:
            self._load()
            self.ready = True
        except (ValueError, OSError, TypeError, KeyError, sqlite3.Error, TimeoutError):
            # Optional exploration must not fabricate substitutes or disable
            # the existing dataset retrieval when its snapshot is unavailable.
            self.ready = False

    def _load(self):
        import numpy as np
        self.identities = (_identity(self.vector_path), _identity(self.path))
        with self.vector_path.open('rb') as stream:
            raw = stream.read(MAX_BYTES + 1)
        if len(raw) > MAX_BYTES:
            raise ValueError('topic export exceeds its byte budget')
        value = json.loads(raw)
        if (not isinstance(value, dict) or value.get('version') != VERSION
                or value.get('model') != MODEL or value.get('dimensions') != DIMENSIONS
                or not isinstance(value.get('topics'), list) or len(value['topics']) != 101):
            raise ValueError('incompatible topic export')
        self.topics = {}
        vectors = []
        with _connect(self.path, time.monotonic() + 2.) as db:
            state = db.execute("SELECT value FROM state WHERE key='complete'").fetchone()
            rows = {row['id']: row for row in db.execute('SELECT * FROM topics LIMIT 102')}
        if state is None or state[0] != '1' or len(rows) != 101:
            raise ValueError('incomplete classification snapshot')
        for item in sorted(value['topics'], key=lambda row: row['id']):
            ident = item['id']
            if (not isinstance(ident, str) or not re.fullmatch(r'M\d{2}-S\d{2}', ident)
                    or ident in self.topics or ident not in rows
                    or item.get('model') != MODEL or item.get('dimensions') != DIMENSIONS):
                raise ValueError('invalid topic identity or model')
            row = rows[ident]
            label = _text(item['subtopic_ko'], 200)
            definition = _text(item['definition_ko'], 2048)
            main = _text(item['main_topic'], 200)
            label_en = _text(item['subtopic_en'], 200)
            definition_en = _text(item['definition_en'], 2048)
            if (row['main_id'] != ident[:3] or row['main_name'] != main or row['name'] != label
                    or row['definition'] != definition
                    or item.get('embedding_text') != '\n'.join((label, definition, label_en, definition_en))):
                raise ValueError('topic definitions differ from their snapshot')
            blob = base64.b64decode(item['vector'], validate=True)
            if len(blob) != DIMENSIONS * 4:
                raise ValueError('invalid topic vector size')
            vector = np.frombuffer(blob, dtype='<f4').astype(np.float64)
            norm = float(np.linalg.norm(vector))
            if not bool(np.isfinite(vector).all()) or not math.isfinite(norm) or norm <= 1e-12:
                raise ValueError('invalid topic vector')
            vectors.append(vector / norm)
            self.topics[ident] = {'id': ident, 'label': label, 'definition': definition,
                                  'parent_id': ident[:3]}
        self.ids = list(self.topics)
        self.matrix = np.stack(vectors)
        self.sha256 = hashlib.sha256(raw).hexdigest()
        self._current()

    def _current(self):
        if self.identities != (_identity(self.vector_path), _identity(self.path)):
            raise ValueError('topic snapshots changed')

    def select(self, query, *, deadline, include_related=True):
        import numpy as np
        _check(deadline)
        if not self.ready:
            return unavailable(), None
        try:
            self._current()
        except (ValueError, OSError):
            return unavailable(), None
        vector = np.asarray(query, dtype=np.float64)
        norm = float(np.linalg.norm(vector))
        if vector.shape != (DIMENSIONS,) or not bool(np.isfinite(vector).all()) or norm <= 1e-12:
            raise ValueError('invalid query vector')
        scores = self.matrix @ (vector / norm)
        order = sorted(range(len(self.ids)), key=lambda i: (-float(scores[i]), self.ids[i]))
        result = {'status': 'no_match', 'version': VERSION, 'topic_vector_sha256': self.sha256,
                  'query_topics': [], 'related_topics': [], 'relations': [],
                  'thresholds': {'query_cosine_min': QUERY_FLOOR, 'topic_cosine_min': RELATED_FLOOR,
                                 'basis': 'retrieval_heuristic'},
                  'membership_basis': 'stored_dataset_topic_memberships'}
        if scores[order[0]] < QUERY_FLOOR:
            return result, None
        first = order[0]
        root = self.ids[first]
        result['status'] = 'ready'
        result['query_topics'] = [{**self.topics[root], 'query_cosine': float(np.clip(scores[first], -1., 1.))}]
        neighbors = self.matrix @ self.matrix[first]
        selected = [first]
        for index in sorted(range(len(self.ids)), key=lambda i: (-float(neighbors[i]), self.ids[i])):
            if not include_related:break
            if index == first or neighbors[index] < RELATED_FLOOR:
                continue
            ident = self.ids[index]
            cosine = float(np.clip(neighbors[index], -1., 1.))
            result['related_topics'].append({**self.topics[ident], 'via_topic_id': root, 'topic_cosine': cosine})
            result['relations'].append({'source': root, 'target': ident, 'relation': 'semantic_similarity',
                                        'basis': 'stored_topic_vector_cosine', 'cosine': cosine})
            selected.append(index)
            if len(selected) == 3:
                break
        _check(deadline)
        return result, np.ascontiguousarray(self.matrix[selected], dtype=np.float32)

    def memberships(self, dataset_ids, exploration, *, deadline):
        _check(deadline)
        if exploration['status'] != 'ready' or not dataset_ids:
            return {}
        try:
            self._current()
            if len(dataset_ids) > 40:
                raise ValueError('too many topic membership candidates')
            selected = {row['id']: {key: row[key] for key in ('id', 'label', 'definition')}
                        | {'origin': 'query_match'} for row in exploration['query_topics']}
            selected.update({row['id']: {key: row[key] for key in ('id', 'label', 'definition', 'via_topic_id')}
                             | {'origin': 'related'} for row in exploration['related_topics']})
            matches = {}
            with _connect(self.path, deadline) as db:
                sql = ('SELECT dataset_id,topic,band,score FROM links WHERE dataset_id IN ('
                       + ','.join('?' for _ in dataset_ids) + ') AND topic IN ('
                       + ','.join('?' for _ in selected) + ') LIMIT 121')
                rows = db.execute(sql, [*dataset_ids, *selected]).fetchall()
                if len(rows) > 120:
                    raise ValueError('invalid membership count')
                for row in rows:
                    score = row['score']
                    if (type(score) not in (float, int) or not math.isfinite(score) or not -1 <= score <= 1
                            or row['band'] not in ('high', 'low')
                            or score < (.25 if row['band'] == 'high' else .15)):
                        raise ValueError('invalid stored topic membership')
                    matches.setdefault(row['dataset_id'], []).append(
                        {**selected[row['topic']], 'dataset_topic_cosine': float(score)})
            self._current()
            _check(deadline)
            return matches
        except (ValueError, OSError, sqlite3.Error) as error:
            raise TopicRetrievalUnavailable('topic memberships unavailable') from error
