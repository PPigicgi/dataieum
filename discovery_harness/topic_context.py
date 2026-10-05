"""Bounded, optional context from the local imported topic classification snapshot.

Only classification annotations cross this boundary. Official candidate metadata
continues to come from the existing vector retrieval and citation validators.
"""
import asyncio
from collections import Counter
import hashlib
import json
import math
import re
import time
from urllib.parse import urlsplit

from .dependency_stage import DependencyStage, DependencyBusy, DependencyTimeout


ENDPOINT = 'http://127.0.0.1:8090/api/ontology/dataset-topics'
MAX_IDS = 12
MAX_RESPONSE_BYTES = 256 * 1024
MODEL = 'text-embedding-3-small'
DIMENSIONS = 1536
_CONTROL = re.compile(r'[\x00-\x1f\x7f]')


def _text(value, characters, byte_limit=None):
    if (not isinstance(value, str) or not value.strip() or len(value) > characters
            or len(value.encode('utf-8')) > (byte_limit or characters * 3) or _CONTROL.search(value)):
        raise ValueError('Invalid topic context text')
    return value


def _ids(values):
    if not isinstance(values, (list, tuple)) or len(values) > MAX_IDS:
        raise ValueError('Topic context accepts at most 12 prepared candidate IDs')
    return sorted({_text(value, 512, 1536) for value in values})


def _score(value):
    if type(value) not in (int, float) or not -1 <= value <= 1 or not math.isfinite(value):
        raise ValueError('Invalid topic cosine')
    return value


def _provenance(value, version):
    if not isinstance(value, dict):
        raise ValueError('Invalid topic provenance')
    result = {}
    for key, expected in (('model', MODEL), ('dimensions', DIMENSIONS), ('taxonomy_version', version),
                          ('basis', 'dataset_topic_cosine')):
        if key in value:
            if value[key] != expected or (key == 'dimensions' and type(value[key]) is not int):
                raise ValueError('Incompatible topic provenance')
            result[key] = value[key]
    for key in ('sha256', 'generation_sha256'):
        if key in value:
            if not isinstance(value[key], str) or not re.fullmatch('[a-f0-9]{64}', value[key]):
                raise ValueError('Invalid topic snapshot hash')
            result[key] = value[key]
    if 'generation' in value:
        result['generation'] = _text(value['generation'], 100, 100)
    if 'thresholds' in value:
        expected = {'low': .15, 'high': .25, 'relative': .9,
                    'link_floor': 'record_band_floor', 'unclassified_links': False}
        if value['thresholds'] != expected:
            raise ValueError('Incompatible topic thresholds')
        result['thresholds'] = dict(expected)
    return result


def validate_topic_context(value):
    """Validate the entire annotation before exposing any of its fields."""
    if not isinstance(value, dict):
        raise ValueError('Invalid topic annotation')
    version = _text(value.get('version'), 100, 100)
    classification = value.get('classification')
    if not isinstance(classification, dict):
        raise ValueError('Invalid topic classification')
    score = _score(classification.get('max_similarity'))
    level = 'high' if score >= .25 else 'low' if score >= .15 else 'unclassified'
    curated=classification.get('basis')=='curated_no_accepted_membership'
    if curated:
        if (level=='unclassified' or classification.get('original_level')!=level
                or type(classification.get('excluded_links')) is not int
                or not 1<=classification['excluded_links']<=101):
            raise ValueError('Invalid curated topic classification')
        level='unclassified'
    if classification.get('level') != level:
        raise ValueError('Topic band contradicts its cosine')
    topics = value.get('topics')
    if (not isinstance(topics, list) or len(topics) > 101 or
            (level == 'unclassified' and topics) or (level != 'unclassified' and not topics)):
        raise ValueError('Invalid topic memberships')
    seen, memberships = set(), []
    for topic in topics:
        if not isinstance(topic, dict):
            raise ValueError('Invalid topic membership')
        ident = _text(topic.get('id'), 32, 32)
        if not re.fullmatch(r'M\d{2}-S\d{2}', ident) or ident in seen:
            raise ValueError('Invalid or duplicate topic ID')
        seen.add(ident)
        similarity = _score(topic.get('similarity'))
        if (topic.get('level') != level or similarity > score + 1e-7
                or similarity + 1e-7 < .9 * score or similarity < (.25 if level == 'high' else .15)):
            raise ValueError('Topic link contradicts classification rules')
        memberships.append({'id': ident, 'label': _text(topic.get('label'), 160, 480),
                            'similarity': similarity, 'level': level})
    memberships.sort(key=lambda topic: (-topic['similarity'], topic['id']))
    if memberships and abs(memberships[0]['similarity'] - score) > 1e-7:
        raise ValueError('Maximum topic membership is missing')
    result = {'version': version, 'classification': {'level': level, 'max_similarity': score},
              'topics': memberships}
    if curated:
        result['classification'].update(basis='curated_no_accepted_membership',
            original_level=classification['original_level'],excluded_links=classification['excluded_links'])
    if 'provenance' in value:
        result['provenance'] = _provenance(value['provenance'], version)
    return result


def safe_topic_context(value):
    try:
        return validate_topic_context(value)
    except (ValueError, TypeError, UnicodeError):
        return None


def unavailable():
    return {'status': 'unavailable', 'version': None, 'contexts': {}, 'missing_ids': []}


def _response(value, ids):
    if not isinstance(value, dict) or value.get('ready') is not True:
        raise ValueError('Topic snapshot is unavailable')
    version = _text(value.get('version'), 100, 100)
    datasets, missing = value.get('datasets'), value.get('missing_ids')
    if (not isinstance(datasets, list) or len(datasets) > len(ids) or not isinstance(missing, list)
            or len(missing) > len(ids) or len(_ids(missing)) != len(missing)):
        raise ValueError('Invalid topic response membership')
    contexts = {}
    provenance = {'provenance': _provenance(value['provenance'], version)} if 'provenance' in value else {}
    for dataset in datasets:
        if not isinstance(dataset, dict):
            raise ValueError('Invalid classified dataset')
        ident = _text(dataset.get('id'), 512, 1536)
        if ident not in ids or ident in contexts or ident in missing:
            raise ValueError('Unexpected or duplicate classified dataset')
        contexts[ident] = validate_topic_context({'version': version,
            'classification': dataset.get('classification'), 'topics': dataset.get('topics'), **provenance})
    if set(contexts) | set(missing) != set(ids):
        raise ValueError('Incomplete topic response membership')
    return {'status': 'ready', 'version': version, 'contexts': contexts, 'missing_ids': missing}


class TopicContextClient:
    def __init__(self, *, client=None, catalog_url='http://127.0.0.1:8090'):
        # Deployment configuration may select a loopback port; neither model
        # output nor user input can choose the host, route, or credentials.
        if not isinstance(catalog_url, str) or _CONTROL.search(catalog_url):
            raise ValueError('Topic catalog must be a local HTTP origin')
        target = urlsplit(catalog_url)
        if (target.scheme != 'http' or target.hostname not in {'127.0.0.1', 'localhost', '::1', 'atlas'}
                or target.username is not None or target.password is not None
                or target.path not in {'', '/'} or target.query or target.fragment
                or (target.port is not None and not 1 <= target.port <= 65535)):
            raise ValueError('Topic catalog must be a local HTTP origin')
        self.endpoint = catalog_url.rstrip('/') + '/api/ontology/dataset-topics'
        self.client = client  # Test transport only.
        self._client = None
        self.stage = DependencyStage(workers=4, queued=1021, wait_seconds=7200,
                                     operation_seconds=2, max_waiters=1025)
        self.calls = Counter()

    def status(self):
        return {'stage': self.stage.status(), 'calls': dict(self.calls),
                'max_response_bytes': MAX_RESPONSE_BYTES, 'max_candidate_ids': MAX_IDS,
                'max_connections': 4}

    async def aclose(self):
        await self.stage.aclose()
        if self._client is not None:
            await self._client.aclose()

    async def _fetch(self, ids, deadline):
        import httpx
        self.calls['http'] += 1
        try:
            # Finish optional-service failure handling before the scheduler's
            # hard limit; an equal deadline would race its session-wide timeout.
            async with asyncio.timeout(max(0., min(1.8, deadline - time.monotonic() - .05))):
                if self.client is None and self._client is None:
                    self._client = httpx.AsyncClient(timeout=1.8, follow_redirects=False, trust_env=False,
                        limits=httpx.Limits(max_connections=4, max_keepalive_connections=4))
                client = self.client if self.client is not None else self._client
                async with client.stream('GET', self.endpoint,
                        headers={'Host':'localhost'},
                        params={'ids': json.dumps(ids, ensure_ascii=False, separators=(',', ':'))}) as response:
                    if response.status_code != 200:
                        raise ValueError('Topic service unavailable')
                    content = bytearray()
                    async for chunk in response.aiter_bytes():
                        if len(content) + len(chunk) > MAX_RESPONSE_BYTES:
                            raise ValueError('Topic response exceeds byte budget')
                        content.extend(chunk)
                    result = _response(json.loads(content), ids)
            self.calls['ready'] += 1
            return result
        except (httpx.HTTPError, TimeoutError, ValueError, UnicodeError, TypeError, RecursionError):
            self.calls['unavailable'] += 1
            return unavailable()

    @staticmethod
    def _identity(ids):
        return hashlib.sha256(json.dumps(ids, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()

    async def lookup(self, ids, *, budget_seconds=None):
        ids = _ids(ids)
        self.calls['topic_context'] += 1
        try:
            return await self.stage.run(self._identity(ids), lambda deadline: self._fetch(ids, deadline),
                budget_seconds=budget_seconds, queue_seconds=2)
        except (DependencyBusy, DependencyTimeout):
            self.calls['unavailable'] += 1
            return unavailable()

    async def scheduled_lookup(self, session, ids):
        ids = _ids(ids)
        self.calls['topic_context'] += 1
        try:
            return await session.scheduled_tool(self.stage, self._identity(ids),
                lambda deadline: self._fetch(ids, deadline), operation_seconds=2)
        except (DependencyBusy, DependencyTimeout):
            self.calls['unavailable'] += 1
            return unavailable()
