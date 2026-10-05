"""Source-region translations and title-only display. Reads never generate text."""
from contextlib import closing
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import time

from .dataset_intro import validate_ids
from .intro_store import connect, missing
from .title_store import lookup_title, validate_translation

VERSION = 'dataset-region-v1'


def source_region(row):
    try:
        metadata = json.loads(row['metadata'])
    except (ValueError, TypeError):
        return ''
    if type(metadata) is not dict:
        return ''
    value = metadata.get('region')
    if type(value) is not str or not value.strip() or len(value.encode()) > 1024:
        return ''
    normalized = value.strip().casefold()
    if (normalized in {'미상', '미확인', '없음', 'unknown', 'unspecified', 'not specified', 'n/a', '-', 'null', 'eu institutions'} or
            '://' in value or re.fullmatch(r'[\d\s.,+;()\[\]-]+', value) or
            any(term in normalized for term in ('상세 지역', '설명 참고', '원문 확인', 'see description', 'refer to description'))):
        return ''
    if any(ord(c) < 32 for c in value) or '\ufffd' in value:
        return ''
    return value


def region_key(text):
    return hashlib.sha256(json.dumps([VERSION, text], ensure_ascii=False).encode()).hexdigest()


def validate_region(source, ko, en):
    validate_translation(source, ko, en)
    if len(ko) > 160 or len(en) > 160:
        raise ValueError('region translation too long')


def lookup(path, catalogue, ids):
    """Only direct title/region translations are eligible for public display."""
    validate_ids(ids)
    result = {i: {**missing(i), 'region': {'ko': '', 'en': ''}} for i in ids}
    if path is None or not Path(path).is_file():
        return {'items': list(result.values())}
    try:
        with closing(connect(path, readonly=True)) as db, closing(connect(catalogue, readonly=True)) as source:
            tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('dataset_titles','dataset_regions')")}
            for identifier in ids:
                row = source.execute('SELECT id,title,metadata FROM datasets WHERE id=?', (identifier,)).fetchone()
                if not row:
                    continue
                if 'dataset_titles' in tables:
                    translated = lookup_title(db, identifier, row['title'])
                    if translated:
                        result[identifier].update(translated)
                original = source_region(row)
                if original and 'dataset_regions' in tables:
                    stored = db.execute('SELECT source_region,ko_region,en_region FROM dataset_regions WHERE key=?', (region_key(original),)).fetchone()
                    if stored and stored['source_region'] == original:
                        try:
                            validate_region(original, stored['ko_region'], stored['en_region'])
                            result[identifier]['region'] = {'ko': stored['ko_region'], 'en': stored['en_region']}
                        except (ValueError, TypeError):
                            pass
    except (OSError, ValueError, TypeError, KeyError, sqlite3.Error):
        return {'items': [{**missing(i), 'region': {'ko': '', 'en': ''}} for i in ids]}
    return {'items': [result[i] for i in ids]}


def import_regions(db, source, payload):
    if (type(payload) is not dict or payload.get('version') != VERSION or payload.get('model') != 'gpt-6-luna' or
            not re.fullmatch('[a-f0-9]{64}', payload.get('review_sha256', '')) or
            type(payload.get('items')) is not list or not 1 <= len(payload['items']) <= 10000):
        raise ValueError('invalid region import')
    counts = {'inserted': 0, 'reused': 0, 'existing_preserved': 0, 'changed_source': 0}
    seen, prepared = set(), []
    for item in payload['items']:
        if type(item) is not dict or set(item) != {'id', 'source_region', 'ko_region', 'en_region'}:
            raise ValueError('invalid region row')
        identifier = item['id']
        if (type(identifier) is not str or not identifier.strip() or identifier in seen or
                len(identifier.encode()) > 1024 or any(ord(c) < 32 for c in identifier)):
            raise ValueError('invalid region identity')
        seen.add(identifier)
        raw = item['source_region']
        if type(raw) is not str or source_region({'metadata': json.dumps({'region': raw})}) != raw or not raw:
            raise ValueError('unusable source region')
        validate_region(raw, item['ko_region'], item['en_region'])
        current = source.execute('SELECT metadata FROM datasets WHERE id=?', (identifier,)).fetchone()
        if current is None or source_region(current) != raw:
            counts['changed_source'] += 1
            continue
        prepared.append(item)
    with db:
        db.execute('''CREATE TABLE IF NOT EXISTS dataset_regions(
            key TEXT PRIMARY KEY,source_region TEXT NOT NULL,ko_region TEXT NOT NULL,en_region TEXT NOT NULL,
            model TEXT NOT NULL,contract TEXT NOT NULL,review_sha256 TEXT NOT NULL,created_at REAL NOT NULL)''')
        for item in prepared:
            key = region_key(item['source_region'])
            old = db.execute('SELECT source_region,ko_region,en_region FROM dataset_regions WHERE key=?', (key,)).fetchone()
            values = tuple(item[n] for n in ('source_region', 'ko_region', 'en_region'))
            if old:
                counts['reused' if tuple(old) == values else 'existing_preserved'] += 1
                continue
            db.execute('INSERT INTO dataset_regions VALUES(?,?,?,?,?,?,?,?)',
                       (key, *values, payload['model'], VERSION, payload['review_sha256'], time.time()))
            counts['inserted'] += 1
    return counts
