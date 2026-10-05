"""Reviewed, per-dataset plain-language labels. Reads never call a model."""
import hashlib
import json
import re
import time

from .metadata_input import prepare_metadata

VERSION = 'dataset-brief-v1'
FIELDS = ('title', 'description', 'classification_paths', 'survey_name', 'tags')


def source_input(row):
    """Hash all source fields, including geography, without changing vector keys."""
    original = {k: row[k] for k in ('id', 'source_id', 'title', 'description')}
    original['metadata'] = json.loads(row['metadata'])
    digest = hashlib.sha256(json.dumps(original, ensure_ascii=False, sort_keys=True,
                                       separators=(',', ':')).encode()).hexdigest()
    record = dict(original['metadata'])
    record.update({k: original[k] for k in ('id', 'source_id', 'title', 'description')})
    return digest, prepare_metadata(record)['fields']


def key(identifier, source_hash):
    return hashlib.sha256(json.dumps([VERSION, identifier, source_hash]).encode()).hexdigest()


def _pair(value, limit, *, required=False):
    if type(value) is not dict or set(value) != {'ko', 'en'}:
        raise ValueError('invalid brief language pair')
    for language in ('ko', 'en'):
        text = value[language]
        if (type(text) is not str or len(text) > limit or text != text.strip() or
                any(ord(c) < 32 for c in text) or '\ufffd' in text or '<' in text or '://' in text):
            raise ValueError('invalid brief text')
    if bool(value['ko']) != bool(value['en']) or (required and not value['ko']):
        raise ValueError('incomplete brief languages')
    if value['ko'] and (not re.search('[가-힣]', value['ko']) or
                       not re.search('[A-Za-z]', value['en']) or re.search('[가-힣]', value['en'])):
        raise ValueError('wrong brief language')


def validate_brief(value, fields, glossary):
    required = {'country', 'region', 'subject', 'explanation', 'evidence', 'terms'}
    if type(value) is not dict or set(value) != required:
        raise ValueError('invalid brief fields')
    for name, limit in [('country', 60), ('region', 100), ('subject', 140), ('explanation', 200)]:
        _pair(value[name], limit, required=name == 'subject')
    if type(value['evidence']) is not list or not 1 <= len(value['evidence']) <= 12:
        raise ValueError('missing brief evidence')
    supported = set()
    for item in value['evidence']:
        if (type(item) is not dict or set(item) != {'target', 'field', 'quote'} or
                item['target'] not in {'country', 'region', 'subject'} or item['field'] not in FIELDS or
                type(item['quote']) is not str or not item['quote'].strip() or len(item['quote']) > 2400 or
                item['quote'] not in fields.get(item['field'], '')):
            raise ValueError('brief evidence not in source')
        supported.add(item['target'])
    if any(value[name]['ko'] and name not in supported for name in ('country', 'region', 'subject')):
        raise ValueError('unsupported brief component')
    if type(glossary) is not dict or type(glossary.get('items')) is not list:
        raise ValueError('invalid glossary')
    terms = glossary['items']
    if any(type(v) is not dict or not all(type(v.get(k)) is str and v[k] for k in
           ('term', 'scope', 'ko', 'en', 'url', 'authority', 'checked_on', 'evidence')) or
           not v['url'].startswith('https://') for v in terms):
        raise ValueError('invalid glossary evidence')
    known = {v['term']: v for v in terms}
    if (type(value['terms']) is not list or len(value['terms']) > 8 or
            any(type(t) is not str or t not in known for t in value['terms']) or
            len(set(value['terms'])) != len(value['terms'])):
        raise ValueError('unreviewed term expansion')
    if value['explanation']['ko'] and not value['terms']:
        raise ValueError('term explanation requires glossary evidence')
    source_text = ' '.join(fields.get(f, '') for f in FIELDS)
    def occurs(term):
        return term in source_text if re.search('[가-힣]', term) else bool(re.search(r'(?<!\w)' + re.escape(term) + r'(?!\w)', source_text, re.I))
    if any(not occurs(t) for t in value['terms']):
        raise ValueError('glossary term absent from dataset')
    numbers = lambda text: set(re.findall(r'\d+(?:[.,]\d+)*', text))
    allowed_numbers = numbers(source_text)
    for locale in ('ko', 'en'):
        text = ' '.join(value[n][locale] for n in ('country', 'region', 'subject', 'explanation'))
        if not numbers(text) <= allowed_numbers:
            raise ValueError('brief introduces numeric qualifiers')
    english = value['subject']['en'] + ' ' + value['explanation']['en']
    if '당해연도' in fields.get('title', '') and re.search(r'\bcurrent[ -]year\b', english, re.I):
        raise ValueError('reference year cannot become current year')
    if '비율' in fields.get('title', '') and re.search(r'\brates?\b', english, re.I):
        raise ValueError('proportion cannot be inferred as a rate')
    return value


def public_row(identifier, value):
    return {'id': identifier, 'available': True, 'brief': True, **{
        locale: {'title': ' · '.join(value[n][locale] for n in ('country', 'region', 'subject') if value[n][locale]),
                 'summary': value['explanation'][locale]} for locale in ('ko', 'en')}}


def lookup_brief(db, source):
    try:
        digest, fields = source_input(source)
        row = db.execute('SELECT content,glossary FROM dataset_briefs WHERE key=?',
                         (key(source['id'], digest),)).fetchone()
        if row:
            value = validate_brief(json.loads(row['content']), fields, json.loads(row['glossary']))
            return public_row(source['id'], value)
    except (ValueError, TypeError, KeyError):
        pass
    return None


def import_briefs(db, source, payload):
    if (type(payload) is not dict or payload.get('version') != VERSION or
            payload.get('model') != 'gpt-6-luna' or payload.get('reviewer') != 'gpt-6-sol' or
            not re.fullmatch('[a-f0-9]{64}', payload.get('review_sha256', '')) or
            type(payload.get('items')) is not list or not 1 <= len(payload['items']) <= 10000):
        raise ValueError('invalid reviewed brief import')
    glossary = payload.get('glossary')
    seen, prepared = set(), []
    counts = {'inserted': 0, 'reused': 0, 'existing_preserved': 0, 'changed_source': 0}
    for item in payload['items']:
        if type(item) is not dict or set(item) != {'id', 'source_hash', 'content'}:
            raise ValueError('invalid brief import row')
        identifier = item['id']
        if (type(identifier) is not str or not identifier.strip() or identifier in seen or
                len(identifier.encode()) > 1024 or any(ord(c) < 32 for c in identifier) or
                not re.fullmatch('[a-f0-9]{64}', item['source_hash'])):
            raise ValueError('invalid brief identity')
        seen.add(identifier)
        original = source.execute('SELECT id,source_id,title,description,metadata FROM datasets WHERE id=?', (identifier,)).fetchone()
        if original is None:
            counts['changed_source'] += 1
            continue
        digest, fields = source_input(original)
        if digest != item['source_hash']:
            counts['changed_source'] += 1
            continue
        value = validate_brief(item['content'], fields, glossary)
        prepared.append((identifier, digest, json.dumps(value, ensure_ascii=False, sort_keys=True)))
    encoded_glossary = json.dumps(glossary, ensure_ascii=False, sort_keys=True)
    with db:
        db.execute('''CREATE TABLE IF NOT EXISTS dataset_briefs(
            key TEXT PRIMARY KEY,dataset_id TEXT NOT NULL,source_hash TEXT NOT NULL,
            content TEXT NOT NULL,glossary TEXT NOT NULL,model TEXT NOT NULL,
            reviewer TEXT NOT NULL,review_sha256 TEXT NOT NULL,created_at REAL NOT NULL)''')
        for identifier, digest, content in prepared:
            identity = key(identifier, digest)
            old = db.execute('SELECT content,glossary FROM dataset_briefs WHERE key=?', (identity,)).fetchone()
            if old:
                counts['reused' if tuple(old) == (content, encoded_glossary) else 'existing_preserved'] += 1
                continue
            db.execute('INSERT INTO dataset_briefs VALUES(?,?,?,?,?,?,?,?,?)', (identity, identifier, digest,
                content, encoded_glossary, payload['model'], payload['reviewer'], payload['review_sha256'], time.time()))
            counts['inserted'] += 1
    return counts
