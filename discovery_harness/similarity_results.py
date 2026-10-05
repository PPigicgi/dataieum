"""Rank stored vector matches directly; apply only explicit metadata filters.

No second model call, lexical topic approval, per-site quota or new relationships.
Similarity is a retrieval signal, not a claim that the source was live-verified.
"""
import copy
import math
import re

from . import coverage
from .site_search import (FORMATS, MAX_SITE_RESULTS, effective_plan, requested_countries,
                          scoped_condition_relaxation, validate_plan, validate_site_result)
from .topic_context import safe_topic_context
from .vector_judge import (EVIDENCE_FIELDS, _UNKNOWN, _field_text, _snippets, _structured_conflict,
                           _supports, _url, required_conditions, safe_topic_matches)

MAX_RETRIEVED = 40
_TYPED_FILTER_FIELDS = {
    'countries': ('coverage_countries', 'countries', 'coverage_regions', 'regions', 'region'),
    'regions': ('coverage_regions', 'regions', 'region'),
    'years': ('coverage_years', 'temporal_coverage', 'period'),
    'years_range': ('coverage_years', 'temporal_coverage', 'period'),
    'dates': ('dates', 'temporal_coverage', 'period'),
    'formats': ('formats', 'format'), 'formats_any': ('formats', 'format'),
    'frequency': ('frequency',), 'fields': ('fields',), 'delivery': ('delivery', 'method'),
    'geography_level': ('geography_level',), 'unit': ('unit',),
    'measurement_basis': ('measurement_basis',),
    'free_only': ('free', 'access', 'license'), 'commercial_only': ('commercial', 'license'),
}


def prepare_similarity_candidates(candidates, plan, *, filter_related=True):
    if not isinstance(candidates, list) or len(candidates) > MAX_RETRIEVED:
        raise ValueError('Invalid similarity candidates')
    rows = []
    exclude_related = filter_related and (plan.get('related_mode') == 'exclude' or bool(plan.get('related_exclusions')))
    for row in candidates:
        if not isinstance(row, dict):
            continue
        ident, source, score = (row.get(k) for k in ('dataset_id', 'source_id', 'cosine'))
        if (not isinstance(ident, str) or not ident or len(ident.encode('utf-8')) > 1536 or
                not isinstance(source, str) or not source or len(source.encode('utf-8')) > 80 or
                type(score) not in (int, float) or not math.isfinite(score) or not -1 <= score <= 1 or
                not isinstance(row.get('metadata'), dict)):
            continue
        row = {**row}
        matches = safe_topic_matches(row.pop('topic_matches', None))
        if matches and exclude_related:
            matches = [m for m in matches if m['origin'] == 'query_match']
            if not matches:
                continue
        if matches:
            row['topic_matches'] = matches
        rows.append(row)
    from .hybrid_search import rank
    rows.sort(key=lambda r: (-rank(r),-r['cosine'], r['dataset_id']))
    unique = {}
    for row in rows:
        unique.setdefault(row['dataset_id'], row)
    return list(unique.values())


def _assess_filters(plan, need, row):
    # Subject/meaning is already carried by Luna's semantic_query embedding.
    # It must not become a literal-word gate on multilingual vector matches.
    required = {token: value for token, value in required_conditions(plan, need).items()
                if token != 'relevance' and not token.startswith('subject:')}
    metadata = row['metadata']
    if not required:
        return [], [], []
    excerpts = _snippets({field: _field_text(metadata[field])
                          for field in EVIDENCE_FIELDS if field in metadata})
    evidence, missing, conflicts = [], [], []
    scope = effective_plan(plan, need)
    facts = coverage.profile(metadata)
    for token, value in required.items():
        kind = token.split(':', 1)[0]
        if kind in {'countries', 'regions', 'years', 'years_range'}:
            key = 'years' if kind == 'years_range' else kind
            criteria = {key: scope[key], 'years_mode': scope.get('years_mode', 'list')}
            assessment = coverage.assess(facts, criteria)
            if assessment['status'] != 'match':
                missing.append(token)
                if assessment['conflicts']:
                    conflicts.append(token)
            else:
                # Quotes retain the original metadata field; derived coverage
                # is never presented as a newly verified official assertion.
                for field in EVIDENCE_FIELDS:
                    if field in metadata and coverage.assess(coverage.profile({field: metadata[field]}), criteria)['status'] == 'match':
                        evidence.append({'condition': token, 'field': field, 'quote': _field_text(metadata[field])[:2000]})
                        break
            continue
        fields = _TYPED_FILTER_FIELDS.get(token.split(':', 1)[0], ())
        populated = {field for field in fields if metadata.get(field) not in (None, [], {})
                     and _field_text(metadata.get(field)).strip().casefold() not in _UNKNOWN}
        if token.startswith('countries:') and populated & {'coverage_countries', 'countries'}:
            populated &= {'coverage_countries', 'countries'}
        if token.startswith('countries:'):
            populated = {field for field in populated if _field_text(metadata[field]).casefold()
                         not in {'전국', '국내', 'global', 'world', 'worldwide'}}
        if token.startswith(('formats:', 'formats_any:')):
            populated = {field for field in populated if any(
                _supports(field, _field_text(metadata[field]), 'formats:'+format_, format_) for format_ in FORMATS)}
        # A structured value is authoritative. A narrative mentioning an old
        # format or another area cannot override format=JSON or region=Busan.
        eligible = [item for item in excerpts if item['field'] in populated] if populated else [
            item for item in excerpts if item['field'] == 'title']
        if not populated and token.split(':', 1)[0] in {'years', 'years_range', 'dates'}:
            eligible = [item for item in eligible if not re.search(
                r'갱신|공표|발행|수정|게시|발표|updated?|published|released?', item['text'], re.I)]
        if token.startswith('additional_requirements:'):
            eligible = excerpts
        contradiction = _structured_conflict(metadata, token, value)
        witness = None if contradiction else next(
            (item for item in eligible if _supports(item['field'], item['text'], token, value)), None)
        if witness is None:
            missing.append(token)
            other_country = (token.startswith('countries:') and not populated and any(
                country != value and _supports('title', _field_text(metadata.get('title')), 'countries:'+country, country)
                for country in ('대한민국', '미국', '일본', '중국', '영국')))
            if contradiction or populated or other_country:
                conflicts.append(token)
        else:
            evidence.append({'condition': token, 'field': witness['field'], 'quote': witness['text']})
    return evidence, missing, conflicts


def _eligible(assessment):
    _, missing, conflicts = assessment
    return not conflicts and not any(token.split(':', 1)[0] in
        {'countries', 'regions', 'years', 'years_range'} for token in missing)


def filter_evidence(plan, need, row):
    evidence, missing, _ = _assess_filters(plan, need, row)
    return evidence, missing


def _registry(sources):
    return {s['id']: s for s in sources} if isinstance(sources, list) else dict(sources)


def select_candidates(plan, candidates, sources, *, limit=MAX_SITE_RESULTS, filter_related=True):
    """Filter all bounded retrieval candidates before choosing the global top ten."""
    validate_plan(plan)
    registry = _registry(sources)
    selected = []
    for row in prepare_similarity_candidates(candidates, plan, filter_related=filter_related):
        source = registry.get(row['source_id'])
        if (not source or not _url(source.get('url')) or
                plan.get('source_ids') and row['source_id'] not in plan['source_ids'] or
                not _url(row['metadata'].get('url')) or not row['metadata'].get('title')):
            continue
        if any(_eligible(_assess_filters(plan, need, row)) for need in plan['needs']):
            selected.append(row)
        if len(selected) == limit:
            break
    return selected


def compose_similarity_result(plan, candidates, sources, retrieval):
    from .hybrid_search import rank
    hybrid=retrieval.get('hybrid',{}).get('method')=='rrf'
    judged = isinstance(retrieval.get('relevance'), dict)
    selected = select_candidates(plan, candidates, sources, limit=MAX_RETRIEVED if judged else MAX_SITE_RESULTS, filter_related=not judged)
    if judged:
        verified = []
        for row in selected:
            need = row.get('relevance_need')
            if (row.get('relevance_tier') not in {'direct', 'related'} or type(need) is not int or
                    not 1 <= need <= len(plan['needs']) or not row.get('relevance_evidence')):
                raise ValueError('Missing relevance decision')
            if not _eligible(_assess_filters(plan, plan['needs'][need-1], row)):
                raise ValueError('Relevance scope changed')
            if row['relevance_tier'] == 'related' and (plan.get('related_mode') == 'exclude' or row.get('relevance_exclusions')):
                raise ValueError('Excluded related result')
            verified.append(row)
        selected = sorted(verified, key=lambda row: (row['relevance_tier'] != 'direct', -rank(row), -row['cosine'], row['dataset_id']))[:MAX_SITE_RESULTS]
    registry = _registry(sources)
    groups, sites = [], {}
    semantic_empty = judged and not selected and retrieval['relevance'].get('eligible_candidates', 0) > 0
    gaps=coverage.groups_for_plan(plan) if not selected and not plan['question'] and not semantic_empty else []
    gap_labels=[]
    for scope in gaps:
        labels=[*scope['countries'],*scope['regions']]
        if scope['years']:labels.append(('~' if scope['years_mode']=='range' else ', ').join(map(str,scope['years']))+'년')
        if labels:gap_labels.append(' · '.join(labels))
    if not plan['question']:
        for row in selected:
            matching_needs = [plan['needs'][row['relevance_need']-1]] if judged else plan['needs']
            matching = [(evidence, missing) for need in matching_needs
                        for evidence, missing, conflicts in [_assess_filters(plan, need, row)]
                        if _eligible((evidence, missing, conflicts))]
            citations, missing = min(matching, key=lambda pair: len(pair[1]))
            source, metadata = registry[row['source_id']], row['metadata']
            card = sites.setdefault(row['source_id'], {'source_id': row['source_id'], 'name': source['name'],
                'url': source['url'], 'evidence': [], 'cosine_similarity': row['cosine']})
            raw_formats = metadata.get('formats', [metadata.get('format', '')])
            raw_formats = raw_formats if isinstance(raw_formats, list) else [raw_formats]
            title = str(metadata['title'])[:500]
            # Do not copy requested filters into a claim of the dataset's actual scope.
            countries = metadata.get('coverage_countries', metadata.get('countries', []))
            countries = [v for v in countries if isinstance(v, str)] if isinstance(countries, list) else []
            fact = {'dataset_id': row['dataset_id'], 'title': title, 'summary': title,
                'evidence_url': _url(metadata['url']), 'checked_on': str(row.get('checked_at', ''))[:40],
                'formats': [f for f in raw_formats if isinstance(f, str) and f in FORMATS],
                'countries': countries, 'access_note': '', 'cosine': row['cosine'],
                'cosine_similarity': row['cosine'], 'match_basis': 'query_similarity',
                'metadata_citations': citations, 'unverified_conditions': missing,
                'input_hash': row.get('input_hash', '')}
            if hybrid:fact.update(rrf_score=rank(row),retrieval_ranks=row.get('retrieval_ranks',{}))
            if judged:
                fact.update(relevance_tier=row['relevance_tier'], relevance_need=row['relevance_need'],
                            relevance_exclusions=list(row.get('relevance_exclusions', [])),
                            match_basis='metadata_relevance_and_query_similarity')
                fact['metadata_citations'] = [*citations, *copy.deepcopy(row['relevance_evidence'])]
            if row.get('topic_matches'):
                fact['topic_matches'] = copy.deepcopy(row['topic_matches'])
            context = safe_topic_context(row.get('topic_context'))
            if context is not None:
                fact['topic_context'] = context
            card['evidence'].append(fact)
        groups.append({'indicator': 'other', 'name': '질문 관련 자료',
            'reason': '질문과 관련된 자료', 'sites': list(sites.values()),
            'unverified': [] if sites else gap_labels})
    total = len({s['source_id'] for g in groups for s in g['sites']})
    state = ('clarify' if plan['question'] else 'results' if groups and all(g['sites'] for g in groups)
             else 'partial' if total else 'unverified')
    selection = {**retrieval.get('selection', {}), 'method': 'cosine_ranked', 'llm_judgment': False,
                 'selected_datasets': len(selected), 'max_datasets': MAX_SITE_RESULTS,
                 'unknown_filter_policy': 'exclude_explicit_country_region_year',
                 'coverage_policy': coverage.VERSION,'coverage_gaps':gaps}
    if judged:
        selection.update(method='relevance_then_cosine', llm_judgment=True,
            direct_datasets=sum(row['relevance_tier'] == 'direct' for row in selected),
            related_datasets=sum(row['relevance_tier'] == 'related' for row in selected),
            empty_reason='meaning_unverified' if semantic_empty else 'coverage_unverified' if not selected else '')
    if hybrid:selection['method']='relevance_then_hybrid_rrf' if judged else 'hybrid_rrf'
    relaxation=None
    if gaps:
        for need in plan['needs']:
            scope=effective_plan(plan,need)
            field='regions' if scope['regions'] else 'period' if scope['years'] else 'countries' if scope['countries'] else None
            if field:
                relaxation=scoped_condition_relaxation(field,need['indicator'])
                break
    result = {'version': 2, 'state': state, 'question': plan['question'], 'plan': copy.deepcopy(plan),
        'groups': groups, 'total': total, 'datasets': [], 'concept_id': 'site_recommendations',
        'scope': {'countries': requested_countries(plan), 'basis': 'requested_conditions', 'unavailable_countries': []},
        'evidence_scope': {'sites': retrieval.get('selection', {}).get('candidate_sites',
            len({r['source_id'] for r in selected})), 'registered_sites': len(registry), 'basis': 'catalog_metadata'},
        'retrieval': {**copy.deepcopy(retrieval), 'selection': selection},
        'relaxation': relaxation,
        'notice': ('요청한 내용에 직접 맞거나 함께 볼 자료의 근거를 확인하지 못했어요.' if semantic_empty else
                   '현재 카탈로그에서 지정한 조건을 함께 충족하는 근거를 확인하지 못했어요. 실제 자료가 없다는 뜻은 아니에요.') if state=='unverified' else ''}
    return validate_site_result(result)
