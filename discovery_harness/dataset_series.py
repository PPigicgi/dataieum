"""Conservative presentation grouping for timestamped catalogue records.

This does not assert identical contents or a verified publisher. Original
records remain available; the first eligible retrieval rank represents a family.
"""
from datetime import datetime
import hashlib
import json
import re
from urllib.parse import urlsplit

_STAMP = re.compile(r'^(?P<stem>.+?)[_\s-](?P<stamp>(?:19|20)\d{6}(?:\d{2}){0,3})$')
_UNKNOWN = {'', '미상', '제공처 확인', 'unknown', 'n/a'}
_VOLATILE = {'id', 'external_id', 'native_id', 'url', 'metadata_url', 'fingerprint',
             'checked_at', 'source_modified', 'title', 'description'}
_CONDITIONS = ('countries', 'regions', 'years', 'dates', 'frequency', 'formats',
               'fields', 'delivery', 'free_only', 'commercial_only', 'geography_level',
               'unit', 'measurement_basis', 'additional_requirements')
_TEMPORAL = re.compile(r'(?:19|20)\d{2}|시계열|기간별|연도별|년도별|월별|일별|시간별|시점별|'
                       r'추이|과거|최근|최신|전년|전월|작년|올해|내년|어제|오늘|내일|지난|이번|다음|비교|'
                       r'주별|분기별|매년|매월|시간에\s*따른|time\s*series|trend|historical|latest|'
                       r'annual|monthly|daily|hourly|over\s+time|compar|yesterday|today|tomorrow|'
                       r'\b(?:last|next|current|year|month|week|day|hour|period|temporal)\b', re.I)


def collapse_for_plan(plan, query=''):
    """Only broad primary discovery may discard time-specific candidates."""
    from .site_search import effective_plan
    scopes = [plan, *(effective_plan(plan, need) for need in plan.get('needs', []))]
    if any(scope.get(field) for scope in scopes for field in _CONDITIONS):
        return False
    text = ' '.join([query, str(plan.get('semantic_query', '')),
                     *(str(need.get(field, '')) for need in plan.get('needs', [])
                       for field in ('subject', 'reason'))])
    return not _TEMPORAL.search(text)


def _timestamp(title):
    if not isinstance(title, str) or len(title) > 280:
        return None
    match = _STAMP.fullmatch(title)
    if not match:
        return None
    stem, stamp = match.group('stem', 'stamp')
    if len(stem.strip()) < 4:
        return None
    try:
        date = datetime.strptime(stamp, {8:'%Y%m%d',10:'%Y%m%d%H',12:'%Y%m%d%H%M',14:'%Y%m%d%H%M%S'}[len(stamp)])
        # strptime accepts some short fields; an exact round-trip is required.
        if date.strftime({8:'%Y%m%d',10:'%Y%m%d%H',12:'%Y%m%d%H%M',14:'%Y%m%d%H%M%S'}[len(stamp)]) != stamp:
            return None
    except (TypeError, ValueError, KeyError):
        return None
    return stem, stamp, date


def diverse_title_order(rows, titles):
    """Defer repeated dated titles without removing any candidate or identity.

    This is only a broad-search inspection order, not evidence of equivalence.
    Full raw metadata still decides whether two candidates can be collapsed.
    """
    first, deferred, seen = [], [], set()
    for row in rows:
        stamp = _timestamp(titles.get(row['dataset_id']))
        key = (row['source_id'], stamp[0], len(stamp[1])) if stamp else None
        (deferred if key is not None and key in seen else first).append(row)
        if key is not None:seen.add(key)
    return first + deferred


def series_key(source_id, raw):
    """Use complete raw identity fields, never the lossy model projection.

    Unknown publishers stay separate except the official Korean linked-data
    catalogue's explicit platform label matching its institution title prefix.
    """
    if not isinstance(raw, dict):
        return None
    parsed = _timestamp(raw.get('title'))
    if parsed is None:
        return None
    stem, stamp, date = parsed
    try:
        url = urlsplit(raw.get('url', ''))
    except (TypeError, ValueError):
        return None
    if url.scheme not in {'https','http'} or not url.hostname or url.username or url.password:
        return None
    official_linked = (source_id == 'korea' and raw.get('native_type') == 'LINKED'
                       and url.hostname in {'www.data.go.kr','data.go.kr'}
                       and re.fullmatch(r'/data/[0-9]+/linkedData\.do', url.path))
    publisher = raw.get('publisher')
    if not isinstance(publisher, str) or publisher.strip().casefold() in _UNKNOWN:
        authority = stem.split('_', 1)[0]
        label = authority + ' 빅데이터 통합플랫폼'
        if not (official_linked
                and isinstance(raw.get('subjects'), list)
                and any(isinstance(item, dict) and item.get('label') == label
                        for item in raw['subjects'])):
            return None
    description = raw.get('description', '')
    if not isinstance(description, str):
        return None
    # Strip only a leading date that exactly agrees with the title timestamp.
    korean = f'{date.year}년 {date.month:02d}월 {date.day:02d}일'
    if len(stamp) >= 10:korean += f' {date.hour:02d}시'
    if len(stamp) >= 12:korean += f' {date.minute:02d}분'
    if len(stamp) >= 14:korean += f' {date.second:02d}초'
    if description.startswith(korean + ' '):
        description = description[len(korean):].lstrip()
    identity = {k:v for k,v in raw.items() if k not in _VOLATILE}
    # Only this catalogue route has a known record-ID slot. Numeric segments
    # elsewhere may identify a station or another substantive dimension.
    path = '/data/#/linkedData.do' if official_linked else url.path
    identity.update(source=source_id, stem=stem, precision=len(stamp), description=description,
                    destination=[url.scheme, url.netloc, path, url.query])
    try:
        encoded = json.dumps(identity, ensure_ascii=False, sort_keys=True,
                             separators=(',', ':'), allow_nan=False).encode()
    except (TypeError, ValueError, UnicodeError, RecursionError):
        return None
    return hashlib.sha256(encoded).hexdigest()
