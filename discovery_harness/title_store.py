"""Reviewed title-only translations. Original catalogue and embeddings stay read-only."""
import hashlib
from collections import Counter
import json
import re
import time

VERSION = 'dataset-title-v1'


def title_key(title):
    if type(title) is not str or not title.strip() or len(title.encode()) > 16384:
        raise ValueError('invalid source title')
    return hashlib.sha256(json.dumps([VERSION, title], ensure_ascii=False).encode()).hexdigest()


def validate_titles(ko, en):
    for text in (ko, en):
        if (type(text) is not str or not text.strip() or len(text) > 512 or
                any(ord(c) < 32 for c in text) or '\ufffd' in text or '<' in text or '://' in text):
            raise ValueError('invalid translated title')
    if not re.search('[가-힣]', ko) or not re.search('[A-Za-z]', en) or re.search('[가-힣]', en):
        raise ValueError('wrong title language')


def validate_translation(source, ko, en):
    validate_titles(ko, en)
    # Preserve numeric qualifiers, including ranges, resolution and percentiles.
    # Ambiguous reformats require review instead of silently changing a measure.
    numbers = lambda text: Counter(re.findall(r'\d+(?:[.,]\d+)*', text))
    expected = numbers(source)
    month_names = [('january','enero'),('february','febrero'),('march','marzo'),('april','abril'),
                   ('may','mayo'),('june','junio'),('july','julio'),('august','agosto'),
                   ('september','septiembre'),('october','octubre'),('november','noviembre'),('december','diciembre')]
    converted_months = Counter({str(i):sum(len(re.findall(r'\b'+name+r'\b',source,re.I)) for name in names)
                                for i,names in enumerate(month_names,1)})
    for text in (ko,en):
        actual = numbers(text)
        if expected - actual or (actual - expected) - converted_months:
            raise ValueError('title numeric qualifiers changed')
    if '비율' in source and re.search(r'\brates?\b', en, re.I):
        raise ValueError('proportion cannot be inferred as a rate')
    if '당해연도' in source and re.search(r'\bcurrent[ -]year\b', en, re.I):
        raise ValueError('reference year cannot become current year')


def lookup_title(db, identifier, title):
    try:
        key = title_key(title)
    except ValueError:
        return None
    row = db.execute('SELECT source_title,ko_title,en_title FROM dataset_titles WHERE key=?',
                     (key,)).fetchone()
    if not row or row['source_title'] != title:
        return None
    try:
        validate_translation(title, row['ko_title'], row['en_title'])
    except ValueError:
        return None
    return {'id': identifier, 'available': True,
            'ko': {'title': row['ko_title'], 'summary': ''},
            'en': {'title': row['en_title'], 'summary': ''}}


def import_titles(db, source, payload):
    """Import completed text only, after checking the live ID and exact source title.

    Do not create model attempts, modify legacy content, or overwrite translations.
    Model provenance is separate from title identity so identical source titles can
    reuse reviewed text without claiming a different generating model.
    """
    if (type(payload) is not dict or payload.get('version') != VERSION or
            payload.get('model') not in {'gpt-6-luna', 'gpt-6-sol'} or
            not re.fullmatch('[a-f0-9]{64}', payload.get('review_sha256', '')) or
            type(payload.get('items')) is not list or not 1 <= len(payload['items']) <= 10000):
        raise ValueError('invalid title import')
    seen, prepared = set(), []
    for row in payload['items']:
        if type(row) is not dict or set(row) != {'id', 'source_title', 'ko_title', 'en_title'}:
            raise ValueError('invalid title row')
        identifier = row['id']
        if (type(identifier) is not str or not identifier.strip() or identifier in seen or
                len(identifier.encode()) > 1024 or any(ord(c) < 32 for c in identifier)):
            raise ValueError('invalid title id')
        seen.add(identifier)
        key = title_key(row['source_title'])
        validate_translation(row['source_title'], row['ko_title'], row['en_title'])
        prepared.append((key, row))
    counts = {'inserted': 0, 'reused': 0, 'existing_preserved': 0, 'changed_source': 0}
    with db:
        db.execute('''CREATE TABLE IF NOT EXISTS dataset_titles(
            key TEXT PRIMARY KEY, source_title TEXT NOT NULL, ko_title TEXT NOT NULL,
            en_title TEXT NOT NULL, model TEXT NOT NULL, contract TEXT NOT NULL,
            review_sha256 TEXT NOT NULL, created_at REAL NOT NULL)''')
        for key, row in prepared:
            actual = source.execute('SELECT title FROM datasets WHERE id=?', (row['id'],)).fetchone()
            if not actual or actual['title'] != row['source_title']:
                counts['changed_source'] += 1
                continue
            old = db.execute('SELECT source_title,ko_title,en_title FROM dataset_titles WHERE key=?', (key,)).fetchone()
            if old:
                same = tuple(old) == (row['source_title'], row['ko_title'], row['en_title'])
                counts['reused' if same else 'existing_preserved'] += 1
                continue
            db.execute('INSERT INTO dataset_titles VALUES(?,?,?,?,?,?,?,?)',
                (key,row['source_title'],row['ko_title'],row['en_title'],payload['model'],VERSION,
                 payload['review_sha256'],time.time()))
            counts['inserted'] += 1
    return counts
