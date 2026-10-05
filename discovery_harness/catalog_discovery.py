"""Scoped discovery over the existing catalog; bounded country/source sampling.

Country means provider country from sources.info, not dataset geographic coverage.
The ordinary catalog's ordering, deduplication and pagination remain unchanged.
"""
from collections import deque
import json


def country_list(value):
    if (not isinstance(value, list) or len(value) > 5 or
            any(not isinstance(v, str) or not v.strip() or v != v.strip() or
                len(v) > 60 or any(ord(c) < 32 for c in v) for v in value) or
            len(set(value)) != len(value)):
        raise ValueError('invalid country scope')
    return value


def discovery_snapshot(catalog, query, check=lambda: None):
    if set(query) - {'concept', 'countries'}:
        raise ValueError('unsupported discovery query')
    concept = query.get('concept', '')
    if concept not in {c['id'] for c in catalog.CONCEPTS}:
        raise ValueError('unknown concept')
    countries = country_list(json.loads(query.get('countries', '[]')))
    selected = set(countries)
    with catalog.database() as db:
        index = catalog.catalog_index(db)
        sources = [json.loads(row[0]) for row in db.execute('SELECT info FROM sources')]
        country_by_source = {s['id']: s.get('country', '') for s in sources}
        available = set(country_by_source.values())
        records = catalog.matching_records(db, index, {'concept': concept})
        total, groups = 0, {}
        for n, record in enumerate(records):
            if n % 4096 == 0:
                check()
            source = record['source']
            country = country_by_source.get(source, '')
            if selected and country not in selected:
                continue
            total += 1
            bucket = groups.setdefault(country, {}).setdefault(source, deque())
            # At most 30 references per registered source, never whole result lists.
            if len(bucket) < 30:
                bucket.append(record)
        # Rotate countries, then providers within each country. One large portal
        # cannot crowd every other country/provider out of the first screen.
        rotation = deque(deque(groups[c].values()) for c in sorted(groups))
        preview = []
        while rotation and len(preview) < 30:
            providers = rotation.popleft()
            bucket = providers.popleft()
            preview.append(bucket.popleft())
            if bucket:
                providers.append(bucket)
            if providers:
                rotation.append(providers)
        check()
        datasets = catalog.read_records(db, preview)
    return {'datasets': datasets, 'total': total, 'scope': {
        'countries': countries, 'basis': 'provider_country', 'diversified': True,
        'unavailable_countries': [c for c in countries if c not in available]}}
