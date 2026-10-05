"""Read-only embedding retrieval with a disposable, source-bound FAISS index.

IVF-PQ finds a bounded candidate set; scores returned to the agent are exact
cosines of the original stored vectors. They are not relevance probabilities,
and the approximate candidate set does not establish global top-k recall.
"""
from __future__ import annotations

import argparse
from collections import Counter, OrderedDict
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import shutil
import sqlite3
import threading
import time
from urllib.parse import urlsplit
import uuid


MODEL = 'text-embedding-3-small'
DIMENSIONS = 1536
INPUT_VERSION = 'four-fields-v1'
FORMAT_VERSION = 1
MAX_SOURCES = 256
MAX_LIMIT = 40
RERANK_CANDIDATES = 4096
MAX_METADATA_LOOKUPS = 160
NPROBE = 128
MAX_CODES = 2000000
MAX_SOURCE_FILTERS = 64
MAX_METADATA_CHARS = 2000
MAX_RAW_METADATA_BYTES = 256 * 1024
HASH = re.compile(r'^[a-f0-9]{64}$')


class IndexUnavailable(RuntimeError):
    """Index is missing, corrupt, or no longer matches the source snapshots."""


class _ByteLRU:
    """Immutable snapshot data only; bounded keys, payload and entry overhead."""
    def __init__(self, max_entries, max_bytes):
        self.max_entries,self.max_bytes=max_entries,max_bytes
        self.items=OrderedDict();self.bytes=0;self.lock=threading.Lock()

    def get(self,key):
        with self.lock:
            value=self.items.get(key)
            if value is not None:self.items.move_to_end(key)
            return value

    def put(self,key,value):
        if not isinstance(key,bytes) or not isinstance(value,bytes):raise TypeError('immutable cache bytes required')
        size=len(key)+len(value)+256
        if size>self.max_bytes or not self.max_entries:return
        with self.lock:
            previous=self.items.pop(key,None)
            if previous is not None:self.bytes-=len(key)+len(previous)+256
            while self.items and (len(self.items)>=self.max_entries or self.bytes+size>self.max_bytes):
                old_key,old_value=self.items.popitem(last=False)
                self.bytes-=len(old_key)+len(old_value)+256
            self.items[key]=value;self.bytes+=size

    def status(self):
        with self.lock:
            return {'entries':len(self.items),'accounted_bytes':self.bytes,
                    'max_entries':self.max_entries,'max_bytes':self.max_bytes}


def _deps():
    import faiss
    import numpy as np
    return faiss, np


def _json(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def _now():
    return datetime.now(timezone.utc).isoformat()


def _sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _identity(path):
    path = Path(path).resolve(strict=True)
    stat = path.stat()
    # Inodes and mount paths differ between builder/service containers. Content
    # header, size, modification time, and WAL identity survive that handoff.
    with path.open('rb') as stream:
        header = hashlib.sha256(stream.read(65536)).hexdigest()
    wal = path.with_name(path.name + '-wal')
    journal = None
    if wal.exists() and wal.stat().st_size:
        ws = wal.stat()
        journal = [ws.st_size, ws.st_mtime_ns]
    return {'bytes': stat.st_size, 'mtime_ns': stat.st_mtime_ns,
            'header_sha256': header, 'wal': journal}


@contextmanager
def _connect(path, *, deadline=None):
    path=Path(path).resolve(strict=True)
    wal=path.with_name(path.name+'-wal')
    # Published snapshots may keep WAL journal mode in their header without
    # live WAL data. Immutable opens avoid creating sidecars on read-only mounts.
    # Search checks the complete source identity again before returning results.
    immutable=not wal.exists() or wal.stat().st_size==0
    db = sqlite3.connect(path.as_uri() + '?mode=ro' + ('&immutable=1' if immutable else ''),
                         uri=True, timeout=.25)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA query_only=ON')
    db.execute('PRAGMA cache_size=-8192')
    db.execute('PRAGMA temp_store=FILE')
    if deadline is not None:
        db.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
    try:
        yield db
    except sqlite3.OperationalError as error:
        if (deadline is not None and time.monotonic()>=deadline and
                getattr(error,'sqlite_errorcode',None)==sqlite3.SQLITE_INTERRUPT):
            raise TimeoutError('vector database deadline exceeded') from None
        raise
    finally:
        db.close()


def _source_contract(db):
    try:
        state = dict(db.execute("SELECT key,value FROM state WHERE key IN ('config','source_signature')"))
        config = json.loads(state['config'])
        signature = json.loads(state['source_signature'])
    except (sqlite3.DatabaseError, ValueError, KeyError, TypeError) as exc:
        raise IndexUnavailable('embedding source contract is missing or invalid') from exc
    if config != {'model': MODEL, 'dimensions': DIMENSIONS, 'version': INPUT_VERSION}:
        raise IndexUnavailable('embedding model, dimensions, or input version do not match')
    if not isinstance(signature, list) or len(signature) != 3:
        raise IndexUnavailable('invalid embedding catalog signature')
    return config, signature


def normalized(vector, *, dimensions=DIMENSIONS, from_blob=False):
    """Validate finite nonzero float vectors and explicitly normalize cosine."""
    _, np = _deps()
    if from_blob:
        if not isinstance(vector, bytes) or len(vector) != dimensions * 4:
            raise ValueError('invalid stored vector size')
        array = np.frombuffer(vector, dtype='<f4')
    else:
        if not isinstance(vector, (list, tuple, np.ndarray)) or len(vector) != dimensions:
            raise ValueError('query vector dimensions do not match')
        if not isinstance(vector, np.ndarray) and any(type(v) not in (int, float) for v in vector):
            raise ValueError('query vector must contain numbers')
        try:
            with np.errstate(over='ignore', invalid='ignore'):
                array = np.asarray(vector, dtype=np.float32)
        except (ValueError, OverflowError, TypeError) as exc:
            raise ValueError('query vector is invalid') from exc
    if array.shape != (dimensions,) or not bool(np.isfinite(array).all()):
        raise ValueError('vector must be finite')
    # Float64 norm prevents overflow/underflow when validating finite float32.
    norm = float(np.linalg.norm(array.astype(np.float64)))
    if not math.isfinite(norm) or norm <= 1e-12:
        raise ValueError('vector must have nonzero finite norm')
    return np.ascontiguousarray(array / norm, dtype=np.float32)


def _batch_cosines(blobs, query64):
    """Score one bounded DB batch from original vectors; None marks invalid rows."""
    _, np = _deps()
    if len(blobs) > 256:
        raise ValueError('reranking batch exceeds database batch size')
    scores = [None] * len(blobs)
    positions = [i for i, blob in enumerate(blobs)
                 if isinstance(blob, bytes) and len(blob) == DIMENSIONS * 4]
    if not positions:
        return scores
    matrix = np.frombuffer(b''.join(blobs[i] for i in positions), dtype='<f4').reshape(-1, DIMENSIONS)
    matrix64 = matrix.astype(np.float64)
    # Compute norms and dots in native array operations, outside the per-row
    # Python loop. Float64 prevents overflow for finite float32 inputs.
    with np.errstate(over='ignore', invalid='ignore', divide='ignore'):
        norms = np.sqrt(np.einsum('ij,ij->i', matrix64, matrix64))
        valid = np.isfinite(matrix).all(axis=1) & np.isfinite(norms) & (norms > 1e-12)
        # Match normalized(): division rounds to float32 before the float64 dot.
        unit = matrix / norms.astype(np.float32)[:, None]
        values = unit.astype(np.float64) @ query64
    for index, position in enumerate(positions):
        if not valid[index]:
            continue
        cosine = float(values[index])
        if math.isfinite(cosine) and abs(cosine) <= 1.00001:
            scores[position] = max(-1.0, min(1.0, cosine))
    return scores


def _safe_url(value):
    if not isinstance(value, str) or len(value) > 1000:
        return ''
    try:
        parts = urlsplit(value)
        host = parts.hostname or ''
        if parts.scheme not in ('http', 'https') or not host or parts.username or parts.password:
            return ''
        if any(ord(c) < 33 for c in value) or '\\' in value:
            return ''
        if host.lower() == 'localhost' or host.lower().endswith(('.localhost', '.local', '.internal')):
            return ''
        try:
            if not ipaddress.ip_address(host).is_global:
                return ''
        except ValueError:
            if '.' not in host:
                return ''
        _ = parts.port
    except ValueError:
        return ''
    return value


def _bounded_value(value, budget=140, depth=0):
    """Copy small JSON values without inventing coverage from other fields."""
    if isinstance(value, str):
        clipped = value.strip()[:budget]
        while clipped and len(_json(clipped)) > budget:
            clipped = clipped[:-8]
        return clipped or None
    if type(value) in (bool, int):
        return value if len(str(value)) <= budget else None
    if type(value) is float:
        return value if math.isfinite(value) else None
    if depth >= 2:
        return None
    if isinstance(value, list):
        result = []
        for item in value[:8]:
            projected = _bounded_value(item, min(80, budget), depth + 1)
            if projected is None:
                continue
            if len(_json([*result, projected])) <= budget:
                result.append(projected)
        return result or None
    if isinstance(value, dict):
        result = {}
        for key, item in list(value.items())[:8]:
            if not isinstance(key, str) or len(key) > 48:
                continue
            projected = _bounded_value(item, min(80, budget), depth + 1)
            if projected is not None and len(_json({**result, key: projected})) <= budget:
                result[key] = projected
        return result or None
    return None


def project_metadata(raw, *, title='', description=''):
    """Allowlisted data only; never interpret metadata text as instructions."""
    if not isinstance(raw, str) or len(raw) > MAX_RAW_METADATA_BYTES:
        return None
    try:
        original = json.loads(raw)
    except (ValueError, RecursionError):
        return None
    if not isinstance(original, dict):
        return None
    url = _safe_url(original.get('url'))
    title = title if isinstance(title, str) else original.get('title')
    if not isinstance(title, str) or not title.strip() or not url:
        return None
    out = {'title': title.strip()[:280], 'url': url}
    # Exact filters and usable destination take precedence over prose length.
    # These match the judge's evidence fields. Coverage is copied only from
    # the record itself: publisher nationality is never substituted for it.
    fields = ('coverage_countries', 'countries', 'region', 'regions', 'coverage_regions',
              'period', 'coverage_years', 'temporal_coverage', 'dates', 'format', 'formats',
              'frequency', 'fields', 'method', 'delivery', 'license', 'access', 'free',
              'commercial', 'geography_level', 'unit', 'measurement_basis')
    for key in fields:
        value = _bounded_value(original.get(key))
        if value is not None:
            candidate = {**out, key: value}
            if len(_json(candidate)) <= MAX_METADATA_CHARS:
                out = candidate
    for key in ('native_catalog_path', 'survey_name', 'survey_title', 'native_survey_name', 'statistical_survey_name'):
        value = original.get(key)
        if isinstance(value, str) and value.strip():
            candidate = {**out, key: value.strip()[:140]}
            if len(_json(candidate)) <= MAX_METADATA_CHARS:
                out = candidate
    paths = original.get('native_catalog_paths')
    if isinstance(paths, list):
        paths = [p.strip()[:120] for p in paths[:2] if isinstance(p, str) and p.strip()]
        if paths and len(_json({**out, 'native_catalog_paths': paths})) <= MAX_METADATA_CHARS:
            out['native_catalog_paths'] = paths
    subjects = original.get('subjects')
    if isinstance(subjects, list):
        labels = []
        for item in subjects[:8]:
            value = item.get('label') if isinstance(item, dict) else item
            if isinstance(value, str) and value.strip():
                labels.append(value.strip()[:60])
        if labels and len(_json({**out, 'subjects': labels})) <= MAX_METADATA_CHARS:
            out['subjects'] = labels
    description = description if isinstance(description, str) else ''
    description = description.strip()[:700]
    if description:
        # JSON escaping can use more characters than the source string.
        while description and len(_json({**out, 'description': description})) > MAX_METADATA_CHARS:
            description = description[:-32]
        if description:
            out['description'] = description
    # Provider display text is lowest priority and is never coverage evidence.
    publisher = _bounded_value(original.get('publisher'), 100)
    if publisher is not None and len(_json({**out, 'publisher': publisher})) <= MAX_METADATA_CHARS:
        out['publisher'] = publisher
    return out


@contextmanager
def _build_lock(directory):
    lock = directory / '.build.lock'
    if lock.is_symlink():
        raise ValueError('index lock cannot be a symlink')
    stream = lock.open('a+b')
    try:
        if os.name == 'posix':
            import fcntl
            try:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise IndexUnavailable('another index build is active') from exc
        else:
            import msvcrt
            stream.seek(0)
            stream.write(b'0')
            stream.flush()
            stream.seek(0)
            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        yield
    finally:
        stream.close()


def build_index(embeddings, catalog, output, *, training_size=32768, nlist=256,
                pq_m=96, iterations=12, batch_size=1024, threads=2):
    """Build with bounded batches and deterministic source-aware sampling.

    At production size, the 1536d training sample is about 192 MiB plus a small
    per-source reservoir. Raw corpus vectors are never retained in memory.
    """
    faiss, np = _deps()
    if not 1024 <= training_size <= 65536 or not 1 <= nlist <= 1024:
        raise ValueError('invalid training budget')
    if not 1 <= pq_m <= 192 or DIMENSIONS % pq_m or not 1 <= iterations <= 30:
        raise ValueError('invalid product quantizer')
    if not 32 <= batch_size <= 2048 or not 1 <= threads <= 4:
        raise ValueError('invalid build memory/CPU budget')
    faiss.omp_set_num_threads(threads)
    embeddings, catalog, output = Path(embeddings).resolve(), Path(catalog).resolve(), Path(output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    with _build_lock(output):
        generation = 'generation-' + uuid.uuid4().hex
        temporary = output / ('.' + generation)
        temporary.mkdir(mode=0o700)
        try:
            manifest = _build(embeddings, catalog, temporary, training_size, nlist,
                              pq_m, iterations, batch_size, faiss, np)
            final = output / generation
            temporary.replace(final)
            pointer = output / ('.CURRENT-' + uuid.uuid4().hex)
            with pointer.open('x', encoding='ascii') as stream:
                stream.write(generation)
                stream.flush()
                os.fsync(stream.fileno())
            pointer.replace(output / 'CURRENT')
            if os.name == 'posix':
                fd = os.open(output, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
            print(_json({'stage': 'published', **manifest}), flush=True)
            return manifest
        finally:
            if temporary.exists():
                shutil.rmtree(temporary)


def _build(embeddings, catalog, directory, training_size, nlist, pq_m, iterations, batch_size, faiss, np):
    started = time.monotonic()
    identities = {'embeddings': _identity(embeddings), 'catalog': _identity(catalog)}
    with _connect(embeddings) as db, _connect(catalog) as catalog_db:
        config, catalog_signature = _source_contract(db)
        actual_signature = list(catalog_db.execute('SELECT count(*),max(rowid),max(checked_at) FROM datasets').fetchone())
        if actual_signature != catalog_signature:
            raise IndexUnavailable('catalog does not match embedding input snapshot')
        rng = np.random.default_rng(20260918)
        training = np.empty((training_size, DIMENSIONS), dtype=np.float32)
        source_training, source_seen = {}, Counter()
        seen, valid, invalid = 0, 0, 0
        last = time.monotonic()
        query = 'SELECT rowid,dataset_id,source_id,input_hash,vector FROM embeddings WHERE vector IS NOT NULL AND exclusion IS NULL ORDER BY rowid'
        cursor = db.execute(query)
        # Deterministic global reservoir plus up to 128 samples from each
        # source keeps small catalogues represented alongside large sources.
        while rows := cursor.fetchmany(batch_size):
            for row in rows:
                seen += 1
                sid = row['source_id']
                if not isinstance(sid, str) or not sid or len(sid) > 128 or not isinstance(row['dataset_id'], str) or not 1 <= len(row['dataset_id']) <= 512 or not HASH.fullmatch(row['input_hash'] or ''):
                    invalid += 1
                    continue
                try:
                    vector = normalized(row['vector'], from_blob=True)
                except ValueError:
                    invalid += 1
                    continue
                valid += 1
                slot = valid - 1 if valid <= training_size else int(rng.integers(valid))
                if slot < training_size:
                    training[slot] = vector
                if sid not in source_training:
                    if len(source_training) >= MAX_SOURCES:
                        raise IndexUnavailable('source count exceeds index budget')
                    source_training[sid] = np.empty((128, DIMENSIONS), dtype=np.float32)
                source_seen[sid] += 1
                slot = source_seen[sid] - 1 if source_seen[sid] <= 128 else int(rng.integers(source_seen[sid]))
                if slot < 128:
                    source_training[sid][slot] = vector
            if time.monotonic() - last > 10:
                print(_json({'stage': 'sample', 'visited': seen, 'valid': valid, 'invalid': invalid}), flush=True)
                last = time.monotonic()
        if not valid:
            raise IndexUnavailable('no valid completed embeddings')
        sources = sorted(source_training)
        source_codes = {sid: n for n, sid in enumerate(sources)}
        approximate = valid >= 10000
        if approximate:
            samples = np.concatenate([training[:min(valid, training_size)]] +
                                     [source_training[sid][:min(source_seen[sid], 128)] for sid in sources])
            del training, source_training
            nlist = min(nlist, max(1, len(samples) // 40))
            index = faiss.IndexIVFPQ(faiss.IndexFlatIP(DIMENSIONS), DIMENSIONS, nlist,
                                     pq_m, 8, faiss.METRIC_INNER_PRODUCT)
            index.cp.seed = 20260918
            index.cp.niter = iterations
            index.pq.cp.seed = 20260918
            index.pq.cp.niter = iterations
            index.use_precomputed_table = -1
            print(_json({'stage': 'train', 'samples': len(samples), 'nlist': nlist, 'pq_m': pq_m}), flush=True)
            index.train(samples)
            del samples
        else:
            del training, source_training
            index = faiss.IndexFlatIP(DIMENSIONS)
        mapping = sqlite3.connect(directory / 'mapping.sqlite3')
        mapping.execute('PRAGMA journal_mode=OFF')
        mapping.execute('PRAGMA synchronous=OFF')
        mapping.execute('PRAGMA cache_size=-16384')
        mapping.execute('CREATE TABLE records(label INTEGER PRIMARY KEY,embedding_rowid INTEGER NOT NULL,dataset_id TEXT NOT NULL,source_id TEXT NOT NULL,input_hash TEXT NOT NULL)')
        codes = np.lib.format.open_memmap(directory / 'sources.npy', mode='w+', dtype=np.uint16, shape=(valid,))
        vectors = np.empty((batch_size, DIMENSIONS), dtype=np.float32)
        count, pending = 0, []
        cursor = db.execute(query)
        try:
            while rows := cursor.fetchmany(batch_size):
                used = 0
                for row in rows:
                    if row['source_id'] not in source_codes or not isinstance(row['dataset_id'], str) or not 1 <= len(row['dataset_id']) <= 512 or not HASH.fullmatch(row['input_hash'] or ''):
                        continue
                    try:
                        vectors[used] = normalized(row['vector'], from_blob=True)
                    except ValueError:
                        continue
                    label = count + used
                    codes[label] = source_codes[row['source_id']]
                    pending.append((label, row['rowid'], row['dataset_id'], row['source_id'], row['input_hash']))
                    used += 1
                if used:
                    index.add(vectors[:used])
                    mapping.executemany('INSERT INTO records VALUES(?,?,?,?,?)', pending)
                    mapping.commit()
                    pending.clear()
                    count += used
                if time.monotonic() - last > 10:
                    print(_json({'stage': 'add', 'indexed': count, 'expected': valid}), flush=True)
                    last = time.monotonic()
            if count == valid:
                mapping.execute('CREATE INDEX records_dataset_id ON records(dataset_id)')
                mapping.commit()
        finally:
            mapping.close()
            codes.flush()
            del codes
        if count != valid:
            raise IndexUnavailable('embedding source changed while indexing')
        faiss.write_index(index, str(directory / 'vectors.faiss'))
    if identities != {'embeddings': _identity(embeddings), 'catalog': _identity(catalog)}:
        raise IndexUnavailable('source snapshots changed during index build')
    files = {name: {'bytes': (directory / name).stat().st_size, 'sha256': _sha(directory / name)}
             for name in ('vectors.faiss', 'mapping.sqlite3', 'sources.npy')}
    manifest = {'format_version': FORMAT_VERSION, **config, 'catalog_signature': catalog_signature,
                'source_identity': identities, 'indexed_vectors': count, 'rejected_vectors': invalid,
                'completed_rows_seen': seen, 'source_counts': dict(source_seen), 'sources': sources,
                'algorithm': 'IVF-PQ' if approximate else 'FlatIP', 'approximate': approximate,
                'nlist': nlist if approximate else 0, 'pq_m': pq_m if approximate else 0,
                'training_seed': 20260918, 'built_at': _now(),
                'build_seconds': round(time.monotonic() - started, 3), 'files': files}
    (directory / 'manifest.json').write_text(_json(manifest), encoding='utf-8')
    for path in directory.iterdir():
        with path.open('rb') as stream:
            os.fsync(stream.fileno())
    return manifest


class VectorIndex:
    def __init__(self, embeddings, catalog, output, *, threads=2, verify_hashes=True,
                 topic_vectors=None, topic_graph=None, coverage_index=None):
        self.embeddings, self.catalog, self.output = map(lambda p: Path(p).resolve(), (embeddings, catalog, output))
        self.faiss, self.np = _deps()
        self.threads = max(1, min(int(threads), 4))
        self.faiss.omp_set_num_threads(self.threads)
        try:
            current = (self.output / 'CURRENT').read_text(encoding='ascii').strip()
            if not re.fullmatch(r'generation-[a-f0-9]{32}', current):
                raise ValueError('invalid generation')
            self.directory = self.output / current
            if self.directory.is_symlink():
                raise ValueError('generation cannot be a symlink')
            self.manifest = json.loads((self.directory / 'manifest.json').read_text(encoding='utf-8'))
            self.updates = self.directory / 'updates.sqlite3' if 'updates' in self.manifest.get('source_identity', {}) else None
            if any(self.manifest.get(k) != v for k, v in
                   {'format_version': FORMAT_VERSION, 'model': MODEL, 'dimensions': DIMENSIONS, 'version': INPUT_VERSION}.items()):
                raise ValueError('index contract differs')
            self.check_current()
            artifacts = ['vectors.faiss', 'mapping.sqlite3', 'sources.npy']
            if 'active_vectors' in self.manifest:
                artifacts.append('active.npy')
            elif 'active.npy' in self.manifest['files']:
                raise ValueError('active-label contract is incomplete')
            for name in artifacts:
                path = self.directory / name
                expected = self.manifest['files'][name]
                if path.is_symlink() or path.stat().st_size != expected['bytes']:
                    raise ValueError('index artifact size differs')
                if verify_hashes and _sha(path) != expected['sha256']:
                    raise ValueError('index artifact checksum differs')
            self.index = self.faiss.read_index(str(self.directory / 'vectors.faiss'))
            self.sources = self.np.load(self.directory / 'sources.npy', mmap_mode='r', allow_pickle=False)
            if self.index.d != DIMENSIONS or self.index.ntotal != self.manifest['indexed_vectors'] or self.sources.shape != (self.index.ntotal,):
                raise ValueError('index dimensions or row count differ')
            if self.index.metric_type != self.faiss.METRIC_INNER_PRODUCT:
                raise ValueError('index metric differs')
            self.active = None
            if 'active_vectors' in self.manifest:
                count = self.manifest['active_vectors']
                self.active = self.np.load(self.directory / 'active.npy', mmap_mode='r', allow_pickle=False)
                if (type(count) is not int or not 0 <= count <= self.index.ntotal or
                        self.active.dtype != self.np.dtype(bool) or self.active.shape != (self.index.ntotal,) or
                        int(self.np.count_nonzero(self.active)) != count):
                    raise ValueError('active-label mask differs')
            self.source_codes = {sid: n for n, sid in enumerate(self.manifest['sources'])}
        except (OSError, ValueError, KeyError, RuntimeError, sqlite3.DatabaseError) as exc:
            raise IndexUnavailable('vector index is unavailable or stale') from exc
        from .topic_retrieval import TopicRetrieval
        self.topics = TopicRetrieval(topic_vectors, topic_graph)
        # These caches belong to this verified immutable snapshot. Every search
        # still checks source identity before reading and before returning.
        self._topic_ann_cache=_ByteLRU(128,4*1024**2)
        self._coverage_cache=_ByteLRU(16,8*1024**2)
        self.coverage_index=None
        path=Path(coverage_index) if coverage_index else self.directory/'coverage.sqlite3'
        if path.exists():
            from .coverage_index import CoverageIndex
            self.coverage_index=CoverageIndex(self.catalog,self.directory/'mapping.sqlite3',path)

    def check_current(self):
        try:
            identities = {'embeddings': _identity(self.embeddings), 'catalog': _identity(self.catalog)}
            if self.updates is not None:identities['updates'] = _identity(self.updates)
            if self.manifest['source_identity'] != identities:
                raise IndexUnavailable('source snapshots changed; rebuild the vector index')
        except OSError as exc:
            raise IndexUnavailable('source snapshots unavailable') from exc

    def health(self):
        self.check_current()
        return {'ready': True, 'model': MODEL, 'dimensions': DIMENSIONS,
                'indexed_vectors': self.manifest['indexed_vectors'], 'approximate': self.manifest['approximate'],
                'built_at': self.manifest['built_at'], 'source_count': len(self.source_codes),
                'coverage_generation': self._coverage_generation(),
                'snapshot_cache':{'topic_ann':self._topic_ann_cache.status()}}

    def _coverage_generation(self):
        if self.coverage_index is None:return None
        try:self.coverage_index.check_current()
        except (ValueError,OSError,sqlite3.DatabaseError) as exc:
            raise IndexUnavailable('coverage index is stale') from exc
        return str(self.coverage_index.manifest['built_at'])

    def search(self, vector, limit=40, source_ids=None, *, timeout=8.0, explore=False, include_related=True, filter_groups=None, lexical_query=None, collapse_time_series=False):
        from . import coverage
        from .dataset_series import series_key
        if type(collapse_time_series) is not bool:raise ValueError("collapse_time_series must be boolean")
        seen_series, collapsed_candidates = set(), 0
        started = time.monotonic()
        if not math.isfinite(timeout) or timeout<=0:raise TimeoutError('vector deadline expired')
        deadline = started + min(10.0, timeout)
        self.check_current()
        if type(limit) is not int or not 1 <= limit <= MAX_LIMIT:
            raise ValueError('limit must be between 1 and 40')
        if type(explore) is not bool or type(include_related) is not bool:
            raise ValueError('explore must be boolean')
        if source_ids is not None and (not isinstance(source_ids, list) or len(source_ids) > MAX_SOURCE_FILTERS or
                any(not isinstance(s, str) or not s or len(s) > 128 for s in source_ids)):
            raise ValueError('source_ids must be a bounded list of source IDs')
        source_ids = list(dict.fromkeys(source_ids)) if source_ids is not None else None
        filter_groups=coverage.validate_groups(filter_groups)
        query = normalized(vector).reshape(1, DIMENSIONS)
        # OpenMP's thread count is local to the calling thread. HTTP worker
        # threads must apply the bound themselves, not only during startup.
        self.faiss.omp_set_num_threads(self.threads)
        selector, mask, bitmap = None, self.active, None
        filter_mode = 'none'
        if source_ids is not None:
            selected = [self.source_codes[s] for s in source_ids if s in self.source_codes]
            source_mask = self.np.isin(self.sources, selected)
            mask = source_mask if mask is None else (mask & source_mask)
            filter_mode = 'before_top_k'
        coverage_generation=self._coverage_generation()
        eligible_count=None
        if filter_groups:
            if self.coverage_index is None:raise IndexUnavailable('coverage prefilter is unavailable')
            scope_key=_json([coverage_generation,coverage.VERSION,filter_groups]).encode()
            cached=self._coverage_cache.get(scope_key)
            if cached is None:
                labels=self.coverage_index.labels(filter_groups,deadline=deadline)
                if any(n<0 or n>=self.index.ntotal for n in labels):raise IndexUnavailable('coverage labels out of bounds')
                geo_mask=self.np.zeros(self.index.ntotal,dtype=bool)
                geo_mask[labels]=True
                cached=self.np.packbits(geo_mask,bitorder='little').tobytes()
                self._coverage_cache.put(scope_key,cached)
            else:
                geo_mask=self.np.unpackbits(self.np.frombuffer(cached,dtype=self.np.uint8),bitorder='little',count=self.index.ntotal).astype(bool)
            mask=geo_mask if mask is None else (mask & geo_mask)
            eligible_count=int(self.np.count_nonzero(mask))
        if mask is not None:
            bitmap=self.np.packbits(mask,bitorder='little')
            selector=self.faiss.IDSelectorBitmap(len(bitmap),self.faiss.swig_ptr(bitmap))
        approximate = self.manifest['approximate']
        candidate_count = min(self.index.ntotal, max(512, RERANK_CANDIDATES))
        exploration, topic_queries = None, None
        if explore:
            exploration, topic_queries = self.topics.select(query[0], deadline=deadline, include_related=include_related)
        ann_queries = 1
        topic_ann_hits = 0
        executed_ann_queries = 1
        if time.monotonic()>=deadline:raise TimeoutError('vector deadline expired before search')
        exact_filtered=filter_groups and eligible_count<=RERANK_CANDIDATES
        if exact_filtered:
            labels=self.np.flatnonzero(mask).reshape(1,-1)
            executed_ann_queries=ann_queries=0
        elif approximate:
            params = self.faiss.SearchParametersIVF()
            params.nprobe = min(self.manifest['nlist'], NPROBE)
            params.max_codes = MAX_CODES
            params.sel = selector
            if topic_queries is not None:
                # One batched ANN call shares the existing code/candidate
                # budgets across the query and at most three stored topics.
                queries = self.np.concatenate((query, topic_queries), axis=0)
                ann_queries = len(queries)
                params.max_codes = max(1, MAX_CODES // ann_queries)
                per_query=max(1,candidate_count//ann_queries)
                scope=_json([params.nprobe,params.max_codes,per_query,
                             sorted(selected) if source_ids is not None else None,
                             coverage_generation,filter_groups]).encode()
                keys=[None]+[hashlib.sha256(scope+b'\x00'+v.tobytes()).digest() for v in topic_queries]
                pools=[None]*ann_queries;missing=[0]
                for i in range(1,ann_queries):
                    cached=self._topic_ann_cache.get(keys[i])
                    if cached is None:missing.append(i)
                    else:
                        pools[i]=self.np.frombuffer(cached,dtype=self.np.int64)
                        topic_ann_hits+=1
                # Keep the original per-query budgets even when only one pool
                # needs computing. User query results are never cached here.
                _,computed=self.index.search(self.np.ascontiguousarray(queries[missing]),per_query,params=params)
                executed_ann_queries=len(missing)
                for j,i in enumerate(missing):
                    pools[i]=computed[j]
                    if i:self._topic_ann_cache.put(keys[i],computed[j].tobytes())
                labels=self.np.stack(pools)
                # Round-robin equally bounded pools before exact reranking.
                labels = labels.T.reshape(1, -1)
            else:
                _, labels = self.index.search(query, candidate_count, params=params)
        else:
            # Small fixtures/indexes use the full exact index. Filtering is
            # applied to that complete set before candidate truncation.
            _, labels = self.index.search(query, self.index.ntotal)
        label_list = list(dict.fromkeys(int(n) for n in labels[0]
            if n >= 0 and (mask is None or bool(mask[n]))))[:candidate_count]
        if time.monotonic() >= deadline:
            raise TimeoutError('vector search deadline exceeded')
        from .hybrid_search import keywords,fuse
        keyword_index=os.environ.get('DATAIEUM_KEYWORD_INDEX')
        if keyword_index:
            from .keyword_index import validate
            validate(self.catalog,keyword_index)
        keyword_labels,keyword_ids,keyword_state=keywords(keyword_index,self.directory/'mapping.sqlite3',lexical_query,mask,min(deadline,time.monotonic()+2.0))
        label_list=list(dict.fromkeys([*label_list,*keyword_labels]))
        ann_finished=time.monotonic()
        ranked, missing_metadata, invalid = [], 0, 0
        candidates, counts, metadata_lookups = [], Counter(), 0
        metadata_budget_exhausted = False
        coverage_rejected=Counter()
        query64 = query[0].astype(self.np.float64)
        embedding_batches=0
        with _connect(self.directory / 'mapping.sqlite3', deadline=deadline) as mapping, \
             _connect(self.embeddings, deadline=deadline) as embeddings, \
             _connect(self.catalog, deadline=deadline) as catalog:
            if self.updates is not None:
                embeddings.execute('ATTACH DATABASE ? AS updates', (self.updates.as_uri()+'?mode=ro&immutable=1',))
            records=[]
            for offset in range(0, len(label_list), 256):
                batch = label_list[offset:offset + 256]
                sql = 'SELECT * FROM records WHERE label IN (' + ','.join('?' for _ in batch) + ')'
                records.extend(mapping.execute(sql,batch))
            # Preserve every candidate and exact score, but read adjacent
            # original rows together instead of issuing thousands of queries.
            records.sort(key=lambda row:row['embedding_rowid'])
            for offset in range(0,len(records),256):
                batch=records[offset:offset+256]
                if time.monotonic()>=deadline:raise TimeoutError('vector reranking deadline exceeded')
                sql='SELECT rowid AS embedding_rowid,dataset_id,source_id,input_hash,vector FROM embeddings WHERE rowid IN ('+','.join('?' for row in batch if row['embedding_rowid']>0)+') AND exclusion IS NULL'
                originals={row['embedding_rowid']:row for row in embeddings.execute(sql,[row['embedding_rowid'] for row in batch if row['embedding_rowid']>0])} if any(row['embedding_rowid']>0 for row in batch) else {}
                delta_ids=[-row['embedding_rowid'] for row in batch if row['embedding_rowid']<0]
                if delta_ids:
                    if self.updates is None:raise IndexUnavailable('updated vectors are unavailable')
                    query_delta='SELECT -rowid AS embedding_rowid,dataset_id,source_id,input_hash,vector FROM updates.embeddings WHERE rowid IN ('+','.join('?' for _ in delta_ids)+')'
                    originals.update({row['embedding_rowid']:row for row in embeddings.execute(query_delta,delta_ids)})
                embedding_batches+=1
                blobs=[]
                for row in batch:
                    if time.monotonic() >= deadline:
                        raise TimeoutError('vector reranking deadline exceeded')
                    original = originals.get(row['embedding_rowid'])
                    if original is None or any(original[k] != row[k] for k in ('dataset_id', 'source_id', 'input_hash')):
                        raise IndexUnavailable('embedding identity changed')
                    blobs.append(original['vector'])
                scores = _batch_cosines(blobs, query64)
                if time.monotonic() >= deadline:
                    raise TimeoutError('vector reranking deadline exceeded')
                for row, cosine in zip(batch, scores):
                    if cosine is None:
                        invalid += 1
                        continue
                    ranked.append({'dataset_id': row['dataset_id'], 'source_id': row['source_id'],
                                   'cosine': cosine, 'input_hash': row['input_hash']})
            ranked.sort(key=lambda r: (-r['cosine'], r['dataset_id']))
            if keyword_ids:ranked=fuse(ranked,keyword_ids, vector_limit=None if collapse_time_series else 100)
            rerank_finished=time.monotonic()
            title_lookups = 0
            if collapse_time_series:
                from .dataset_series import diverse_title_order
                titles = {}
                for offset in range(0, len(ranked), 256):
                    if time.monotonic() >= deadline:
                        raise TimeoutError('vector title diversity deadline exceeded')
                    batch = ranked[offset:offset + 256]
                    sql = 'SELECT id,substr(title,1,281) AS title FROM datasets WHERE id IN (' + ','.join('?' for _ in batch) + ')'
                    titles.update((r['id'], r['title']) for r in catalog.execute(sql, [r['dataset_id'] for r in batch]))
                    title_lookups += len(batch)
                ranked = diverse_title_order(ranked, titles)
            # Metadata is large and frequently cold on disk. Rank original
            # vectors first, then read only records still eligible for output.
            for candidate in ranked:
                if time.monotonic() >= deadline:
                    raise TimeoutError('vector metadata deadline exceeded')
                sid = candidate['source_id']
                if metadata_lookups >= MAX_METADATA_LOOKUPS:
                    metadata_budget_exhausted = True
                    break
                metadata_lookups += 1
                record = catalog.execute('SELECT source_id,substr(title,1,280) AS title,substr(description,1,700) AS description,CASE WHEN length(metadata)<=262144 THEN metadata END AS metadata,substr(checked_at,1,80) AS checked_at FROM datasets WHERE id=?', (candidate['dataset_id'],)).fetchone()
                if record is None or record['source_id'] != sid:
                    missing_metadata += 1
                    continue
                metadata = project_metadata(record['metadata'], title=record['title'], description=record['description'])
                if metadata is None:
                    missing_metadata += 1
                    continue
                if filter_groups:
                    facts=coverage.profile(metadata)
                    assessments=[coverage.assess(facts,g) for g in filter_groups]
                    if not any(a['status']=='match' for a in assessments):
                        reason='unknown' if any(a['status']=='unknown' for a in assessments) else 'conflict'
                        coverage_rejected[reason]+=1
                        continue
                if collapse_time_series:
                    raw = json.loads(record['metadata'])
                    key = series_key(sid, raw) if raw.get('title') == record['title'] else None
                    if key is not None:
                        if key in seen_series:
                            collapsed_candidates += 1
                            continue
                        seen_series.add(key)
                candidates.append({**candidate, 'metadata': metadata, 'checked_at': record['checked_at']})
                counts[sid] += 1
                if len(candidates) >= limit:
                    break
        self.check_current()
        if exploration is not None and exploration['status'] == 'ready':
            from .topic_retrieval import TopicRetrievalUnavailable, unavailable
            try:
                memberships = self.topics.memberships([row['dataset_id'] for row in candidates],
                                                       exploration, deadline=deadline)
                for candidate in candidates:
                    if candidate['dataset_id'] in memberships:
                        candidate['topic_matches'] = memberships[candidate['dataset_id']]
            except TopicRetrievalUnavailable:
                exploration = unavailable()
        return {'candidates': candidates, **({'exploration': exploration} if explore else {}), 'retrieval': {
            'model': MODEL, 'dimensions': DIMENSIONS, 'input_version': INPUT_VERSION,
            'hybrid':{'method':'rrf' if keyword_ids else 'vector','k':60,'keyword_candidates':len(keyword_ids),'keyword_state':keyword_state,'additional_model_calls':0},
            'approximate': approximate, 'algorithm': self.manifest['algorithm'],
            'indexed_vectors': self.manifest['indexed_vectors'], 'candidate_budget': candidate_count,
            'nprobe': min(self.manifest['nlist'], NPROBE) if approximate else None,
            'scan_code_budget': MAX_CODES if approximate else None,
            'ann_query_count': ann_queries,
            'executed_ann_queries':executed_ann_queries,'topic_ann_cache_hits':topic_ann_hits,
            'reranked_vectors': len(label_list), 'metadata_rejected': missing_metadata, 'invalid_vectors': invalid,
            'metadata_lookups': metadata_lookups, 'metadata_lookup_budget': MAX_METADATA_LOOKUPS,
            'metadata_budget_exhausted': metadata_budget_exhausted,
            'time_series': {'enabled': collapse_time_series, 'collapsed_candidates': collapsed_candidates,
                            'title_lookups': title_lookups, 'title_lookup_budget': len(label_list),
                            'representative': 'retrieval_rank'},
            'score': 'exact_cosine_of_normalized_original_vectors', 'source_filter': filter_mode,
            'source_ids': source_ids, 'max_per_source': None, 'global_top_k_guaranteed': False,
            'coverage_generation':coverage_generation,'coverage_policy':coverage.VERSION,
            'coverage_filter':{'groups':filter_groups,'eligible_vectors':eligible_count,
                'exact_sparse_search':bool(exact_filtered),'rejected':dict(coverage_rejected)},
            'recall_note': 'IVF-PQ candidate retrieval can miss relevant records.' if approximate else
                           'Exact small-index retrieval over records with available metadata.',
            'milliseconds': round((time.monotonic() - started) * 1000, 2),
            'phase_milliseconds':{'ann':round((ann_finished-started)*1000,2),
                                  'rerank':round((rerank_finished-ann_finished)*1000,2),
                                  'metadata':round((time.monotonic()-rerank_finished)*1000,2)},
            'embedding_queries':embedding_batches,
            'index_built_at': self.manifest['built_at']}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    build = sub.add_parser('build')
    build.add_argument('--embeddings', required=True)
    build.add_argument('--catalog', required=True)
    build.add_argument('--output', required=True)
    build.add_argument('--training-size', type=int, default=32768)
    build.add_argument('--nlist', type=int, default=256)
    build.add_argument('--pq-m', type=int, default=96)
    build.add_argument('--iterations', type=int, default=12)
    build.add_argument('--batch-size', type=int, default=1024)
    build.add_argument('--threads', type=int, default=2)
    args = vars(parser.parse_args())
    args.pop('command')
    build_index(**args)


if __name__ == '__main__':
    main()
