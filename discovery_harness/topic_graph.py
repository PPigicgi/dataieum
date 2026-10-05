"""Bounded read-only views of the published dataset/subtopic cosine snapshot."""
from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
import html
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import time
from urllib.parse import urlsplit


MAX_RESPONSE_BYTES = 256 * 1024
MAX_MANIFEST_BYTES = 1024 * 1024
QUERY_SECONDS = 1.0
TOPIC_ID = re.compile(r'M\d{2}-S\d{2}\Z')
THRESHOLDS = {'low': .15, 'high': .25, 'relative': .9,
              'link_floor': 'record_band_floor', 'unclassified_links': False}


class TopicGraphUnavailable(RuntimeError):
    """A snapshot failed integrity validation or a bounded read could not finish."""


def _text(value, limit):
    if not isinstance(value, str):
        return ''
    value = html.unescape(value[:limit * 4])
    value = re.sub(r'<[^>]*>', ' ', value)
    return ' '.join(re.sub(r'[\x00-\x1f\x7f]', ' ', value).split())[:limit]


def _url(value):
    if not isinstance(value, str) or len(value) > 2048 or re.search(r'[\s\x00-\x1f\\]', value):
        return None
    try:
        parsed = urlsplit(value)
        host = (parsed.hostname or '').rstrip('.').lower()
        if parsed.scheme not in ('https', 'http') or not host or parsed.username or parsed.password:
            return None
        if parsed.port is not None and not 1 <= parsed.port <= 65535:
            return None
        try:
            if not ipaddress.ip_address(host).is_global:
                return None
        except ValueError:
            # Exclude local names and legacy numeric IP notations browsers accept.
            if '.' not in host or host.endswith(('.localhost', '.local', '.internal', '.lan', '.home')):
                return None
            if all(re.fullmatch(r'(?:0x[0-9a-f]+|[0-9]+)', part) for part in host.split('.')):
                return None
            if not all(re.fullmatch(r'[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?', part)
                       for part in host.encode('idna').decode().split('.')):
                return None
        return value
    except (ValueError, UnicodeError):
        return None


def _integer(value):
    return type(value) is int and value >= 0


def _cosine(value):
    if type(value) not in (int, float) or not math.isfinite(value) or not -1 <= value <= 1:
        raise TopicGraphUnavailable('Invalid classification similarity')
    return float(value)


def held_classification(db, identifier, row):
    """Read a curated exception only with preserved, internally consistent proof.

    max_similarity remains the ORIGINAL embedding maximum for held records.
    It is not the maximum of an accepted membership (there are none).
    """
    hold=db.execute('SELECT * FROM classification_holds WHERE dataset_id=?',(identifier,)).fetchone()
    if hold is None:return None
    score=_cosine(row['score'])
    original_band='high' if score>=.25 else 'low' if score>=.15 else 'unclassified'
    if (row['band']!='unclassified' or original_band=='unclassified' or hold['original_band']!=original_band
            or abs(_cosine(hold['original_score'])-score)>1e-7
            or type(hold['excluded_links']) is not int or not 1<=hold['excluded_links']<=101
            or db.execute('SELECT 1 FROM links WHERE dataset_id=? LIMIT 1',(identifier,)).fetchone()
            or db.execute('SELECT 1 FROM primary_links WHERE dataset_id=?',(identifier,)).fetchone()):
        raise TopicGraphUnavailable('Invalid curated classification hold')
    removed=db.execute('SELECT topic,original_link,reason,evidence_url FROM membership_corrections WHERE dataset_id=? LIMIT 102',(identifier,)).fetchall()
    if len(removed)!=hold['excluded_links']:raise TopicGraphUnavailable('Missing curated membership proof')
    scores=[]
    for entry in removed:
        link=json.loads(entry['original_link']);value=_cosine(link.get('score'))
        if (link.get('dataset_id')!=identifier or link.get('topic')!=entry['topic']
                or not TOPIC_ID.fullmatch(entry['topic']) or link.get('band')!=original_band
                or value>score+1e-7 or value+1e-7<.9*score or value<(.25 if original_band=='high' else .15)
                or not entry['reason'] or not _url(entry['evidence_url'])):
            raise TopicGraphUnavailable('Invalid curated membership proof')
        scores.append(value)
    if abs(max(scores)-score)>1e-7:raise TopicGraphUnavailable('Original maximum proof is missing')
    return {'level':'unclassified','max_similarity':score,'basis':'curated_no_accepted_membership',
            'original_level':original_band,'excluded_links':len(removed)}


def _limit(value):
    if type(value) is not int or not 1 <= value <= 20:
        raise ValueError('limit must be an integer between 1 and 20')
    return value


def _ids(values):
    if not isinstance(values, (list, tuple)) or len(values) > 20:
        raise ValueError('Select at most 20 dataset IDs')
    if any(not isinstance(v, str) or not v or len(v) > 2048
           or re.search(r'[\x00-\x1f\x7f]', v) for v in values):
        raise ValueError('Dataset IDs must be nonempty text of at most 2048 characters')
    return list(dict.fromkeys(values))


class TopicGraph:
    def __init__(self, confidence_path, catalog_path=None, manifest_path=None):
        self.path = Path(confidence_path).absolute() if confidence_path else None
        self.catalog_path = Path(catalog_path).absolute() if catalog_path else None
        manifest_path = manifest_path or os.environ.get('DATAIEUM_TOPIC_MANIFEST')
        self.manifest_path = (Path(manifest_path).absolute() if manifest_path
                              else self.path.with_name('manifest.json') if self.path else None)

    @staticmethod
    def _unavailable():
        return {'ready': False, 'state': 'unavailable', 'version': None}

    @staticmethod
    def _check(deadline):
        if time.monotonic() >= deadline:
            raise TopicGraphUnavailable('Topic graph query deadline exceeded')

    @staticmethod
    def _finish(value, deadline):
        TopicGraph._check(deadline)
        if len(json.dumps(value, ensure_ascii=False, allow_nan=False,
                          separators=(',', ':')).encode('utf-8')) > MAX_RESPONSE_BYTES:
            raise TopicGraphUnavailable('Topic graph response exceeds its byte budget')
        return value

    def _connect(self, path, deadline):
        self._check(deadline)
        db = sqlite3.connect(path.as_uri() + '?mode=ro&immutable=1', uri=True, timeout=.05)
        try:
            db.row_factory = sqlite3.Row
            db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, MAX_MANIFEST_BYTES)
            db.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
            db.execute('PRAGMA query_only=ON')
            db.execute('PRAGMA cache_size=-8192')
            db.execute('PRAGMA temp_store=FILE')
        except sqlite3.Error:
            db.close()
            raise
        return db

    def _identity(self):
        if self.path.is_symlink() or not self.path.is_file():
            raise TopicGraphUnavailable('Classification snapshot is not a regular file')
        wal = self.path.with_name(self.path.name + '-wal')
        if wal.exists() and wal.stat().st_size:
            raise TopicGraphUnavailable('Classification snapshot has a live WAL')
        stat = self.path.stat()
        return stat.st_ino, stat.st_size, stat.st_mtime_ns

    @contextmanager
    def _session(self):
        deadline = time.monotonic() + QUERY_SECONDS
        db = catalog = None
        try:
            before = self._identity()
            with self.manifest_path.open('rb') as stream:
                raw = stream.read(MAX_MANIFEST_BYTES + 1)
            if len(raw) > MAX_MANIFEST_BYTES:
                raise TopicGraphUnavailable('Classification manifest is too large')
            manifest = json.loads(raw)
            if (not isinstance(manifest, dict) or type(manifest.get('version')) is not int
                    or manifest.get('version') != 1
                    or manifest.get('taxonomy_version') != 'public-data-topics-v6'
                    or manifest.get('model') != 'text-embedding-3-small'
                    or manifest.get('dimensions') != 1536
                    or not _integer(manifest.get('bytes')) or manifest.get('bytes') != before[1]
                    or not isinstance(manifest.get('sha256'), str)
                    or not re.fullmatch('[a-f0-9]{64}', manifest['sha256'])
                    or manifest.get('thresholds') != THRESHOLDS):
                raise TopicGraphUnavailable('Classification manifest is incompatible')
            db = self._connect(self.path, deadline)
            topics, counts = self._load(db, manifest)
            if self.catalog_path and self.catalog_path.is_file():
                catalog = self._connect(self.catalog_path, deadline)
            yield db, catalog, manifest, topics, counts, deadline
            self._check(deadline)
            if self._identity() != before:
                raise TopicGraphUnavailable('Classification snapshot changed during the read')
        except (OSError, sqlite3.Error, UnicodeError, json.JSONDecodeError, TypeError) as error:
            raise TopicGraphUnavailable('Classification snapshot could not be read safely') from error
        finally:
            if catalog is not None:
                catalog.close()
            if db is not None:
                db.close()

    def _load(self, db, manifest):
        state = dict(db.execute("SELECT key,value FROM state WHERE key IN ('complete','count') LIMIT 3"))
        if state.get('complete') != '1':
            raise TopicGraphUnavailable('Classification snapshot is incomplete')
        rows = db.execute('SELECT id,main_id,substr(main_name,1,201) AS main_name,'
                          'substr(name,1,201) AS name,substr(definition,1,2049) AS definition '
                          'FROM topics ORDER BY id LIMIT 102').fetchall()
        definitions = manifest.get('topics')
        if len(rows) != 101 or not isinstance(definitions, list) or len(definitions) != 101:
            raise TopicGraphUnavailable('Classification taxonomy must contain 101 topics')
        translated = {}
        for item in definitions:
            if (not isinstance(item, dict) or not isinstance(item.get('id'), str)
                    or not TOPIC_ID.fullmatch(item['id']) or item['id'] in translated
                    or item.get('model') != manifest['model'] or item.get('dimensions') != 1536):
                raise TopicGraphUnavailable('Invalid taxonomy metadata')
            translated[item['id']] = item
        topics = {}
        for row in rows:
            item = translated.get(row['id'])
            if (not item or row['main_id'] != row['id'][:3]
                    or row['name'] != item.get('subtopic_ko')
                    or row['definition'] != item.get('definition_ko')
                    or not isinstance(row['main_name'], str) or len(row['main_name']) > 200
                    or not isinstance(row['name'], str) or not 0 < len(row['name']) <= 200
                    or not isinstance(item.get('subtopic_en'), str)
                    or not isinstance(item.get('definition_en'), str)):
                raise TopicGraphUnavailable('Taxonomy definitions do not match the snapshot')
            topics[row['id']] = {'id': row['id'], 'label': _text(row['name'], 120),
                'parent_id': row['main_id'], 'parent_label': _text(row['main_name'], 120),
                'definition': _text(row['definition'], 600),
                'label_en': _text(item['subtopic_en'], 120),
                'definition_en': _text(item['definition_en'], 600),
                'linked_datasets': 0, 'high_links': 0, 'low_links': 0}
        counts = {'high': 0, 'low': 0, 'unclassified': 0, 'links': 0}
        seen = set()
        aggregates = db.execute('SELECT band,main_id,topic,n FROM counts LIMIT 1000').fetchall()
        if len(aggregates) >= 1000:
            raise TopicGraphUnavailable('Too many classification aggregates')
        for row in aggregates:
            band, main, topic, n = tuple(row)
            key = band, main, topic
            if (band not in ('high', 'low', 'unclassified') or not _integer(n)
                    or key in seen or not isinstance(main, str) or not isinstance(topic, str)):
                raise TopicGraphUnavailable('Invalid classification aggregate')
            seen.add(key)
            if not main and not topic:
                counts[band] = n
            elif topic:
                if topic not in topics or main != topics[topic]['parent_id'] or band == 'unclassified':
                    raise TopicGraphUnavailable('Invalid topic membership count')
                topics[topic][band + '_links'] = n
                topics[topic]['linked_datasets'] += n
                counts['links'] += n
            elif main not in {t['parent_id'] for t in topics.values()}:
                raise TopicGraphUnavailable('Unknown main topic count')
        counts['total'] = sum(counts[b] for b in ('high', 'low', 'unclassified'))
        if (not all((b, '', '') in seen for b in ('high', 'low', 'unclassified'))
                or counts != manifest.get('counts') or state.get('count') != str(counts['total'])):
            raise TopicGraphUnavailable('Classification counts do not match the manifest')
        return topics, counts

    @staticmethod
    def _provenance(manifest):
        return {'basis': 'dataset_topic_cosine', 'taxonomy_version': manifest['taxonomy_version'],
                'model': manifest['model'], 'dimensions': manifest['dimensions'],
                'sha256': manifest['sha256'], 'thresholds': dict(THRESHOLDS)}

    def overview(self):
        if self.path is None or not self.path.exists():
            return self._unavailable()
        with self._session() as (_, __, manifest, topics, counts, deadline):
            return self._finish({'ready': True, 'version': manifest['taxonomy_version'],
                'counts': counts, 'topics': list(topics.values()),
                'provenance': self._provenance(manifest)}, deadline)

    def _datasets(self, db, catalog, ids, topics, deadline, selected=None):
        datasets, missing = [], []
        has_holds=db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='classification_holds'").fetchone()
        for identifier in ids:
            self._check(deadline)
            row = db.execute('SELECT id,band,score,substr(source,1,201) AS source,'
                             'substr(title,1,1601) AS title FROM records WHERE id=?', (identifier,)).fetchone()
            if row is None:
                missing.append(identifier)
                continue
            score = _cosine(row['score'])
            band = 'unclassified' if score < .15 else 'low' if score < .25 else 'high'
            classification=held_classification(db,identifier,row) if has_holds else None
            if classification:band='unclassified'
            if row['band'] != band:
                raise TopicGraphUnavailable('Classification band contradicts the maximum cosine')
            links = db.execute('SELECT topic,band,score FROM links WHERE dataset_id=? LIMIT 102',
                               (identifier,)).fetchall()
            if len(links) > 101 or (band == 'unclassified' and links) or (band != 'unclassified' and not links):
                raise TopicGraphUnavailable('Invalid number of dataset memberships')
            memberships = []
            for link in links:
                similarity = _cosine(link['score'])
                if (link['topic'] not in topics or link['band'] != band
                        or similarity > score + 1e-7 or similarity + 1e-7 < score * .9
                        or similarity < (.25 if band == 'high' else .15)):
                    raise TopicGraphUnavailable('Invalid dataset topic membership')
                memberships.append({'id': link['topic'], 'label': topics[link['topic']]['label'],
                                    'similarity': similarity, 'level': band})
            memberships.sort(key=lambda item: (-item['similarity'], item['id']))
            if memberships and abs(memberships[0]['similarity'] - score) > 1e-7:
                raise TopicGraphUnavailable('Maximum cosine membership is missing')
            item = {'id': identifier, 'title': _text(row['title'], 400), 'description': '',
                'url': None, 'source_id': _text(row['source'], 200), 'source_name': None,
                'formats': [], 'metadata_available': False,
                'classification': classification or {'level': band, 'max_similarity': score}, 'topics': memberships}
            if selected:
                match = next((t for t in memberships if t['id'] == selected), None)
                if not match:
                    raise TopicGraphUnavailable('Selected membership is missing')
                item['topic_similarity'] = match['similarity']
            if catalog is not None:
                self._enrich(catalog, item)
            datasets.append(item)
        return datasets, missing

    @staticmethod
    def _enrich(catalog, item):
        row = catalog.execute('SELECT substr(title,1,1601) AS title,substr(description,1,6001) AS description,'
                              'substr(metadata,1,16385) AS metadata FROM datasets WHERE id=?',
                              (item['id'],)).fetchone()
        if row is None:
            return
        try:
            metadata = json.loads(row['metadata']) if len(row['metadata'] or '') <= 16384 else {}
        except (ValueError, TypeError):
            metadata = {}
        if not isinstance(metadata, dict):
            metadata = {}
        item.update(title=_text(row['title'], 400) or item['title'],
                    description=_text(row['description'], 1500), metadata_available=True,
                    url=_url(metadata.get('url')))
        formats = metadata.get('formats', metadata.get('format', []))
        if isinstance(formats, str):
            formats = [formats]
        if isinstance(formats, list):
            item['formats'] = list(dict.fromkeys(_text(f, 30) for f in formats[:10]
                                                 if isinstance(f, str) and _text(f, 30)))
        source = catalog.execute('SELECT substr(info,1,16385) FROM sources WHERE id=?',
                                 (item['source_id'],)).fetchone()
        if source:
            try:
                info = json.loads(source[0])
                if isinstance(info, dict):
                    item['source_name'] = _text(info.get('name'), 200) or None
            except (ValueError, TypeError):
                pass

    def dataset_topics(self, ids):
        ids = _ids(ids)
        if self.path is None or not self.path.exists():
            return {**self._unavailable(), 'datasets': [], 'missing_ids': ids}
        with self._session() as (db, catalog, manifest, topics, _, deadline):
            datasets, missing = self._datasets(db, catalog, ids, topics, deadline)
            return self._finish({'ready': True, 'version': manifest['taxonomy_version'],
                'datasets': datasets, 'missing_ids': missing, 'basis': 'topic_classification',
                'provenance': self._provenance(manifest)}, deadline)

    def topic(self, topic_id, limit=20, band=None):
        limit = _limit(limit)
        if not isinstance(topic_id, str) or not TOPIC_ID.fullmatch(topic_id):
            raise ValueError('Invalid topic ID')
        if band not in (None, 'high', 'low'):
            raise ValueError('Topic band must be high or low')
        if self.path is None or not self.path.exists():
            return self._unavailable()
        with self._session() as (db, catalog, manifest, topics, _, deadline):
            if topic_id not in topics:
                raise KeyError(topic_id)
            sql='SELECT dataset_id FROM links WHERE topic=?'
            args=[topic_id]
            if band:sql+=' AND band=?';args.append(band)
            ids=[row[0] for row in db.execute(sql+' ORDER BY score DESC,dataset_id LIMIT ?',args+[limit+1])]
            datasets, missing = self._datasets(db, catalog, ids[:limit], topics, deadline, topic_id)
            if missing:
                raise TopicGraphUnavailable('Topic membership references a missing dataset')
            related = Counter(t['id'] for item in datasets for t in item['topics'] if t['id'] != topic_id)
            related_bands = Counter((t['id'], t['level']) for item in datasets for t in item['topics']
                                    if t['id'] != topic_id)
            related_topics = [{**topics[id], 'linked_datasets': n, 'basis': 'visible_dataset_sample',
                               'relation': 'co_classified', 'shared_datasets': n,
                               'high_links': related_bands[id, 'high'],
                               'low_links': related_bands[id, 'low'],
                               'sample_size': len(datasets)}
                              for id, n in sorted(related.items(), key=lambda pair: (-pair[1], pair[0]))[:12]]
            topic = dict(topics[topic_id])
            if band:
                topic['linked_datasets'] = topic[band + '_links']
            return self._finish({'ready': True, 'version': manifest['taxonomy_version'],
                'topic': topic, 'band': band, 'datasets': datasets, 'related_topics': related_topics,
                'has_more': len(ids) > limit, 'basis': 'topic_classification',
                'sample_size': len(datasets)}, deadline)

    def unclassified(self, limit=20):
        limit = _limit(limit)
        if self.path is None or not self.path.exists():
            return self._unavailable()
        with self._session() as (db, catalog, manifest, topics, _, deadline):
            ids = [r[0] for r in db.execute("SELECT id FROM records INDEXED BY records_band "
                    "WHERE band='unclassified' ORDER BY id LIMIT ?", (limit + 1,))]
            datasets, _ = self._datasets(db, catalog, ids[:limit], topics, deadline)
            return self._finish({'ready': True, 'version': manifest['taxonomy_version'],
                'topic': None, 'datasets': datasets, 'related_topics': [], 'has_more': len(ids) > limit,
                'basis': 'unclassified', 'sample_size': len(datasets)}, deadline)
