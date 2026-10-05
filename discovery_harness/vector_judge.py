"""A bounded agent judgment contract with server-checked metadata citations.

The agent judges meaning. These guards establish citation provenance and explicit
constraints, not a mathematical proof that arbitrary natural language entails a
claim. Responses intentionally describe catalog evidence, not live verification.
"""
from __future__ import annotations

import copy
import ipaddress
import json
import math
import re
from datetime import date
from pathlib import Path
from urllib.parse import urlsplit

from .topic_context import safe_topic_context

from .site_search import (FORMATS, INDICATORS, MAX_SITE_RESULTS, effective_plan, requested_countries, selected_concept, single_condition_relaxation, relaxation_concept,
                          validate_plan, validate_site_result)

SKILL_PATH = Path(__file__).with_name('skills') / 'vector-site-search' / 'SKILL.md'
MAX_CANDIDATES = 12
MAX_SELECTIONS = MAX_SITE_RESULTS
MAX_PROMPT_BYTES = 24000
EVIDENCE_FIELDS = (
    'title', 'description', 'coverage_countries', 'countries', 'region', 'regions',
    'coverage_regions', 'period', 'coverage_years', 'temporal_coverage', 'dates',
    'format', 'formats', 'frequency', 'fields', 'method', 'delivery', 'license',
    'access', 'free', 'commercial', 'geography_level', 'unit', 'measurement_basis',
    'native_catalog_path', 'native_catalog_paths', 'survey_name', 'survey_title',
    'native_survey_name', 'statistical_survey_name', 'subjects',
)
_UNKNOWN = {'', '미상', '미확인', '제공처 확인', 'unknown', 'n/a', 'none', 'null'}
_TEXT = {'title', 'description'}
_FIELD_RULES = {
    'relevance': set(EVIDENCE_FIELDS) - {'license', 'access', 'free', 'commercial'},
    'countries': _TEXT | {'coverage_countries', 'countries', 'region', 'regions', 'coverage_regions'},
    'regions': _TEXT | {'region', 'regions', 'coverage_regions'},
    'years': _TEXT | {'period', 'coverage_years', 'temporal_coverage'},
    'years_range': {'period', 'temporal_coverage'},
    'dates': _TEXT | {'period', 'temporal_coverage', 'dates'},
    'formats': _TEXT | {'format', 'formats'},
    'formats_any': _TEXT | {'format', 'formats'},
    'frequency': _TEXT | {'frequency'}, 'fields': _TEXT | {'fields'},
    'delivery': _TEXT | {'method', 'delivery'},
    'free_only': {'access', 'license', 'free'},
    'commercial_only': {'license', 'commercial'},
    'geography_level': _TEXT | {'geography_level'}, 'subject': _TEXT | {'subjects'},
    'unit': _TEXT | {'unit'}, 'measurement_basis': _TEXT | {'measurement_basis'},
    'additional_requirements': set(EVIDENCE_FIELDS),
}
_KOREAN_AREAS = ('서울특별시', '부산광역시', '대구광역시', '인천광역시', '광주광역시',
                 '대전광역시', '울산광역시', '세종특별자치시', '경기도', '강원특별자치도',
                 '충청북도', '충청남도', '전북특별자치도', '전라남도', '경상북도', '경상남도',
                 '제주특별자치도')
_ALIASES = {
    '대한민국': ('대한민국', '한국', 'South Korea', 'Republic of Korea', *_KOREAN_AREAS),
    '미국': ('미국', 'United States', 'USA'), '일본': ('일본', 'Japan'),
    '중국': ('중국', 'China'), '영국': ('영국', 'United Kingdom'),
    'download': ('download', '다운로드'), 'api': ('api',),
    'observed': ('observed', '관측', '실측'), 'estimated': ('estimated', '추정'),
    'forecast': ('forecast', '예측', '전망'), 'simulation': ('simulation', '시뮬레이션'),
}


def _dump(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def _clip(value, budget):
    return value.encode('utf-8')[:budget].decode('utf-8', errors='ignore')


def _field_text(value):
    if isinstance(value, str):
        return value
    if isinstance(value, (list, dict, bool, int, float)):
        try:
            return _dump(value)
        except (ValueError, TypeError):
            return ''
    return ''


def safe_topic_matches(value):
    """Bound service-verified exploration links; never create metadata evidence.

    The vector service verifies topic membership and relation provenance before
    attaching these annotations. This boundary validates their wire shape only;
    malformed annotations are discarded without discarding official metadata.
    """
    if not isinstance(value, list) or not 1 <= len(value) <= 3:
        return []
    required = {'id', 'label', 'definition', 'origin', 'dataset_topic_cosine'}
    result, seen = [], set()
    for match in value:
        if not isinstance(match, dict) or not required <= set(match) <= required | {'via_topic_id'}:
            return []
        ident, origin, score = match['id'], match['origin'], match['dataset_topic_cosine']
        if (not isinstance(ident, str) or not re.fullmatch(r'M\d{2}-S\d{2}', ident) or ident in seen or
                origin not in ('query_match', 'related') or type(score) not in (int, float) or
                not -1 <= score <= 1 or not math.isfinite(score)):
            return []
        for key, characters, bytes_ in (('label', 160, 480), ('definition', 600, 1800)):
            text = match[key]
            if (not isinstance(text, str) or not text.strip() or len(text) > characters or
                    re.search(r'[\x00-\x1f\x7f]', text)):
                return []
            try:
                if len(text.encode('utf-8')) > bytes_:
                    return []
            except UnicodeError:
                return []
        if 'via_topic_id' in match and (not isinstance(match['via_topic_id'], str) or
                not re.fullmatch(r'M\d{2}-S\d{2}', match['via_topic_id'])):
            return []
        seen.add(ident)
        result.append(dict(match))
    return result


def prepare_candidates(candidates, limit=MAX_CANDIDATES, *, plan=None):
    """Return a deterministic, bounded view; discard invalid vector scores/IDs.

    Never rewrite official metadata into inferred coverage fields. URLs and
    provenance remain available to the server but are not sent to the judge.
    """
    if not isinstance(candidates, list):
        raise ValueError('invalid candidate list')
    exclude_related = bool(plan and (plan.get('related_mode') == 'exclude' or plan.get('related_exclusions')))
    valid = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        ident, source = candidate.get('dataset_id'), candidate.get('source_id')
        score = candidate.get('cosine')
        if (not isinstance(ident, str) or not ident or len(ident) > 512 or len(ident.encode('utf-8')) > 1536 or
                not isinstance(source, str) or not source or len(source.encode('utf-8')) > 80 or
                isinstance(score, bool) or not isinstance(score, (int, float)) or
                not math.isfinite(score) or not -1.000001 <= score <= 1.000001 or
                not isinstance(candidate.get('metadata'), dict)):
            continue
        if 'topic_matches' in candidate:
            candidate = dict(candidate)
            matches = safe_topic_matches(candidate.pop('topic_matches'))
            if matches and exclude_related:
                # Until exclusions have a verified taxonomy mapping, disable all
                # relation expansion while retaining any direct query match.
                matches = [match for match in matches if match['origin'] == 'query_match']
                if not matches:
                    continue
            if matches:
                candidate['topic_matches'] = matches
        valid.append(candidate)
    valid.sort(key=lambda candidate: (-candidate['cosine'], candidate['dataset_id']))
    seen, source_counts, result = set(), {}, []
    for candidate in valid:
        if candidate['dataset_id'] in seen:
            continue
        seen.add(candidate['dataset_id'])
        source = candidate['source_id']
        if source_counts.get(source, 0) >= 2:
            continue
        source_counts[source] = source_counts.get(source, 0) + 1
        result.append(candidate)
        if len(result) >= min(MAX_CANDIDATES, max(1, limit)):
            break
    return result


def _project(candidate):
    metadata, remaining = {}, 760
    # Condition-bearing fields precede the long description, which is truncated
    # as data, never summarized by another model or supplemented with guesses.
    order = ['title', *[key for key in EVIDENCE_FIELDS if key not in {'title', 'description'}], 'description']
    for field in order:
        value = _field_text(candidate['metadata'].get(field))
        if value.strip().casefold() in _UNKNOWN:
            continue
        text = _clip(value, min(320 if field == 'description' else 200, remaining))
        if text:
            metadata[field] = text
            remaining -= len(text.encode('utf-8'))
        if remaining <= 0:
            break
    result = {'dataset_id': candidate['dataset_id'], 'source_id': candidate['source_id'],
              'cosine': round(candidate['cosine'], 7), 'metadata': metadata}
    matches = safe_topic_matches(candidate.get('topic_matches'))
    if matches:
        result['topic_matches'] = matches
    context = safe_topic_context(candidate.get('topic_context'))
    if context is not None:
        # Keep the exact numeric scores and snapshot identity. Topic labels are
        # supporting context only; they never become official citation snippets.
        total = len(context['topics'])
        while len(_dump(context).encode('utf-8')) > 768 and context['topics']:
            context['topics'].pop()
            context['omitted_topics_for_budget'] = total - len(context['topics'])
        if len(_dump(context).encode('utf-8')) <= 768:
            result['topic_context'] = context
    return result


def _views(candidates, plan=None):
    result, size = [], 0
    for candidate in prepare_candidates(candidates, plan=plan):
        view = _project(candidate)
        size += len(_dump(view).encode('utf-8')) + 1
        if size > 15000:
            break
        result.append(view)
    return result


def visible_candidates(candidates, *, plan=None):
    """Freeze the original shared byte budget before partitioning judgments."""
    visible = {view['dataset_id'] for view in _views(candidates, plan)}
    return [candidate for candidate in prepare_candidates(candidates, plan=plan) if candidate['dataset_id'] in visible]


def option_candidates(candidates, prompt):
    """Keep only globally visible lexical options; none is semantically approved."""
    context = json.loads(prompt)
    indices = {option['candidate'] for option in context['options_data']}
    ids = {view['dataset_id'] for index, view in enumerate(_views(candidates, context), 1) if index in indices}
    return [row for row in prepare_candidates(candidates, plan=context) if row['dataset_id'] in ids]


def required_conditions(plan, need):
    criteria = effective_plan(plan, need)
    relevance = INDICATORS[need['indicator']]
    try:
        definition = selected_concept(need['indicator'])
    except ValueError:
        definition = None
    if definition:
        relevance = definition['label'] + ': ' + definition['definition']
    if plan.get('selected_concept'):
        node = selected_concept(plan['selected_concept'])
        if need['indicator'] == (node.get('legacy_id') or 'other'):
            relevance = node['label'] + ': ' + node['definition']
    elif need['indicator'] == 'other':
        relevance = need.get('subject') or plan.get('purpose') or need.get('reason') or relevance
    conditions = {'relevance': relevance}
    for field in ('countries', 'regions', 'fields', 'additional_requirements'):
        for value in criteria.get(field, []):
            conditions[field + ':' + str(value)] = str(value)
    formats = criteria.get('formats', [])
    if len(formats) > 1:
        value = '|'.join(formats)
        conditions['formats_any:' + value] = value
    elif formats:
        conditions['formats:' + formats[0]] = formats[0]
    years = criteria.get('years', [])
    if years and criteria.get('years_mode') == 'range' and len(years) > 1:
        value = str(min(years)) + '..' + str(max(years))
        conditions['years_range:' + value] = value
    else:
        for year in years:
            conditions['years:' + str(year)] = str(year)
    if criteria.get('dates'):
        value = '..'.join(criteria['dates'])
        conditions['dates:' + value] = value
    for field in ('frequency', 'delivery', 'geography_level', 'unit', 'measurement_basis'):
        if criteria.get(field):
            conditions[field + ':' + criteria[field]] = criteria[field]
    for field in ('free_only', 'commercial_only'):
        if criteria.get(field):
            conditions[field + ':true'] = 'true'
    if need.get('subject'):
        conditions['subject:' + need['subject']] = need['subject']
    return conditions


def _request_context(plan):
    result = {'purpose': plan.get('purpose', ''), 'semantic_query':plan.get('semantic_query',''), 'needs': [
        {'indicator': need['indicator'], 'reason': need.get('reason', ''),
         'subject': need.get('subject', ''), 'required_conditions': required_conditions(plan, need)}
        for need in plan['needs']], 'source_ids': plan.get('source_ids', []),
        'related_mode': plan.get('related_mode', 'auto'),
        'related_exclusions': list(plan.get('related_exclusions', []))}
    if plan.get('selected_concept'):
        node = selected_concept(plan['selected_concept'])
        result['selected_concept'] = {key: node[key] for key in ('id', 'label', 'definition')}
    return result


def judge_contract(plan, candidates):
    validate_plan(plan)
    ids = [candidate['dataset_id'] for candidate in _views(candidates, plan)]
    conditions = sorted({key for need in plan['needs'] for key in required_conditions(plan, need)})
    citation = {'type': 'object', 'properties': {
        'condition': {'type': 'string', 'enum': conditions or ['relevance']},
        'field': {'type': 'string', 'enum': list(EVIDENCE_FIELDS)},
        'quote': {'type': 'string', 'minLength': 2, 'maxLength': 100}},
        'required': ['condition', 'field', 'quote'], 'additionalProperties': False}
    item = {'type': 'object', 'properties': {
        'dataset_id': {'type': 'string', 'enum': ids or ['__no_candidates__']},
        'indicator': {'type': 'string', 'enum': [need['indicator'] for need in plan['needs']] or ['other']},
        'accepted': {'type': 'boolean'},
        'evidence': {'type': 'array', 'items': citation, 'maxItems': 48}},
        'required': ['dataset_id', 'indicator', 'accepted', 'evidence'], 'additionalProperties': False}
    schema = {'type': 'object', 'properties': {
        'selections': {'type': 'array', 'items': item, 'maxItems': MAX_SELECTIONS}},
        'required': ['selections'], 'additionalProperties': False}
    instructions = SKILL_PATH.read_text(encoding='utf-8')
    return schema, instructions


def judge_prompt(query, plan, candidates):
    validate_plan(plan)
    if not isinstance(query, str) or not query.strip() or len(query.encode('utf-8')) > 2048:
        raise ValueError('invalid judgment query')
    payload = {'query_data': query, **_request_context(plan),
        'candidates_data': _views(candidates, plan)}
    prompt = _dump(payload)
    if len(prompt.encode('utf-8')) > MAX_PROMPT_BYTES:
        raise ValueError('judgment prompt exceeds byte budget')
    return prompt


def _snippets(metadata):
    """Exact, overlapping excerpts; never summarize or infer coverage text."""
    result = []
    for field, text in metadata.items():
        start = 0
        while start < len(text):
            excerpt = text[start:start + 100]
            if len(excerpt) >= 2:
                result.append({'field': field, 'text': excerpt})
            if start + 100 >= len(text):
                break
            start += 80
        # Preserve surrounding context above while also supplying exact range
        # substrings accepted by the strict coverage validator. These are copied
        # from explicit coverage fields only, never inferred from dates/URLs.
        if field in {'period', 'temporal_coverage'}:
            for atom in (r'\d{4}-\d{2}-\d{2}', r'\d{4}'):
                pattern = r'(?<!\d)' + atom + r'\s*(?:\.\.|[-~–—/]|부터)\s*' + atom + r'(?:까지)?(?!\d)'
                for match in re.finditer(pattern, text):
                    excerpt = {'field': field, 'text': match[0]}
                    if len(match[0]) <= 100 and excerpt not in result:
                        result.append(excerpt)
    return result


def compact_judgment(query, plan, candidates):
    """Return a compact wire contract and a provenance-checking decoder.

    Integer references reduce generation latency: the model selects supplied
    excerpts instead of recopying long dataset IDs and Korean quotation text.
    The decoder restores the full contract and runs the same strict validator.
    """
    validate_plan(plan)
    if not isinstance(query, str) or not query.strip() or len(query.encode('utf-8')) > 2048:
        raise ValueError('invalid judgment query')
    visible, snippets, mapping, snippet_id = [], {}, {}, 0
    for index, view in enumerate(_views(candidates, plan), 1):
        entries = []
        for excerpt in _snippets(view['metadata']):
            snippet_id += 1
            entries.append({'snippet': snippet_id, **excerpt})
            snippets[snippet_id] = {'candidate': index, **excerpt}
        if entries:
            mapping[index] = view['dataset_id']
            visible.append({'candidate': index, 'source_id': view['source_id'],
                            'cosine': view['cosine'], 'snippets': entries,
                            **({'topic_matches': view['topic_matches']} if 'topic_matches' in view else {}),
                            **({'topic_context': view['topic_context']} if 'topic_context' in view else {})})
    payload = {'query_data': query, **_request_context(plan),
        'candidates_data': visible}
    prompt = _dump(payload)
    # If excerpt framing exceeds the wire budget, omit lowest-ranked candidates
    # explicitly. No generated summaries or evidence shortening change meaning.
    omitted = 0
    while len(prompt.encode('utf-8')) > MAX_PROMPT_BYTES and visible:
        removed = visible.pop()
        del mapping[removed['candidate']]
        for excerpt in removed['snippets']:
            del snippets[excerpt['snippet']]
        omitted += 1
        payload['omitted_candidates_for_budget'] = omitted
        prompt = _dump(payload)
    if len(prompt.encode('utf-8')) > MAX_PROMPT_BYTES:
        raise ValueError('judgment prompt exceeds byte budget')
    conditions = sorted({key for need in plan['needs'] for key in required_conditions(plan, need)})
    citation = {'type': 'object', 'properties': {
        'condition': {'type': 'string', 'enum': conditions or ['relevance']},
        'snippet': {'type': 'integer', 'enum': list(snippets) or [-1]}},
        'required': ['condition', 'snippet'], 'additionalProperties': False}
    selection = {'type': 'object', 'properties': {
        'candidate': {'type': 'integer', 'enum': list(mapping) or [-1]},
        'indicator': {'type': 'string', 'enum': [need['indicator'] for need in plan['needs']] or ['other']},
        'evidence': {'type': 'array', 'items': citation, 'maxItems': 48}},
        'required': ['candidate', 'indicator', 'evidence'], 'additionalProperties': False}
    schema = {'type': 'object', 'properties': {
        'selections': {'type': 'array', 'items': selection, 'maxItems': MAX_SELECTIONS}},
        'required': ['selections'], 'additionalProperties': False}
    instructions = SKILL_PATH.read_text(encoding='utf-8') + (
        '\n## Active mode: compact judgment\n'
        'Use the supplied compact JSON schema. candidate and snippet are integer references '
        'from this prompt. Do not copy dataset IDs, quoted text, URLs or scores. Each selection '
        'is accepted; omit every rejected candidate. Cite only a snippet belonging to that '
        'candidate for each required condition. Its entire text must support the condition '
        'in context. The server restores the exact field and text, then runs all evidence '
        'checks. Budget-omitted candidates and absent snippets cannot be selected.\n')

    def decode(value):
        if (not isinstance(value, dict) or set(value) != {'selections'} or
                not isinstance(value['selections'], list) or len(value['selections']) > MAX_SELECTIONS):
            raise ValueError('invalid compact vector judgment')
        canonical = {'selections': []}
        for item in value['selections']:
            if (not isinstance(item, dict) or set(item) != {'candidate', 'indicator', 'evidence'} or
                    type(item['candidate']) is not int or item['candidate'] not in mapping or
                    not isinstance(item['evidence'], list) or len(item['evidence']) > 48):
                raise ValueError('invalid compact vector selection')
            evidence = []
            for citation_ in item['evidence']:
                if (not isinstance(citation_, dict) or set(citation_) != {'condition', 'snippet'} or
                        type(citation_['snippet']) is not int or citation_['snippet'] not in snippets):
                    raise ValueError('unknown compact vector snippet')
                snippet = snippets[citation_['snippet']]
                if snippet['candidate'] != item['candidate']:
                    raise ValueError('snippet belongs to another candidate')
                evidence.append({'condition': citation_['condition'], 'field': snippet['field'],
                                 'quote': snippet['text']})
            canonical['selections'].append({'dataset_id': mapping[item['candidate']],
                'indicator': item['indicator'], 'accepted': True, 'evidence': evidence})
        return validate_decision(canonical, plan, candidates)

    return schema, instructions, prompt, decode


def _option_evidence(view, candidate, required):
    """Shared provenance check for both approval options and empty-result facts."""
    excerpts, evidence, missing = _snippets(view['metadata']), [], []
    for token, value in required.items():
        ordered = sorted(excerpts, key=lambda excerpt: (
            0 if token == 'relevance' and excerpt['field'] == 'title' else
            1 if token == 'relevance' and excerpt['field'] == 'description' else 2))
        witness = None if _structured_conflict(candidate['metadata'], token, value) else next(
            (excerpt for excerpt in ordered if _supports(excerpt['field'], excerpt['text'], token, value)), None)
        if witness is None:
            missing.append(token)
        else:
            evidence.append({'condition': token, 'field': witness['field'], 'quote': witness['text']})
    return evidence, missing


def _empty_diagnosis(plan, need, candidates):
    """Describe only gaps demonstrated in the bounded candidates actually read.

    Do not claim a global absence or infer the agent's semantic rejection reason.
    A suggestion changes nothing until a user explicitly submits another turn.
    """
    required = required_conditions(plan, need)
    available = {row['dataset_id']: row for row in prepare_candidates(candidates, plan=plan)}
    missing_sets = [set(_option_evidence(view, available[view['dataset_id']], required)[1])
                    for view in _views(candidates, plan)
                    if not plan.get('source_ids') or view['source_id'] in plan['source_ids']]
    if not missing_sets:
        return ['현재 검색 범위에서 검토할 수 있는 자료 후보를 찾지 못했어요.'], None
    absent = set.intersection(*missing_sets)
    labels = {'countries': '대상 국가', 'regions': '세부 지역', 'years': '자료 연도',
        'years_range': '연속 제공 기간', 'dates': '자료 날짜', 'formats_any': '파일 형식',
        'formats': '파일 형식', 'frequency': '시간 단위', 'fields': '필요 컬럼',
        'delivery': '제공 방식', 'free_only': '무료 이용', 'commercial_only': '상업적 이용',
        'geography_level': '지역 단위', 'subject': '대상 집단', 'unit': '측정 단위',
        'measurement_basis': '측정 방식', 'additional_requirements': '추가 조건'}
    gaps, suggestion = [], None
    relaxations = {'formats_any': ('formats', '파일 형식'), 'formats': ('formats', '파일 형식'),
        'years': ('period', '기간'), 'years_range': ('period', '기간'), 'dates': ('period', '기간'),
        'frequency': ('frequency', '시간 단위'), 'geography_level': ('geography_level', '지역 단위'),
        'regions': ('regions', '세부 지역'), 'delivery': ('delivery', '제공 방식')}
    for token, value in required.items():
        kind = token.split(':', 1)[0]
        if token not in absent or kind not in labels:
            continue
        gaps.append(f'{labels[kind]}({value[:80]}): 검토한 후보의 메타데이터에서 근거를 확인하지 못했어요.')
        # A multi-concept result needs a separately typed per-concept action.
        # Keep this safe, single-condition shortcut for one-concept searches.
        if suggestion is None and relaxation_concept(plan) and kind in relaxations:
            field, _ = relaxations[kind]
            suggestion = single_condition_relaxation(field)
    if not gaps:
        gaps = ['조건별 근거를 한 자료에서 함께 확인하지 못했어요.'] if all(missing_sets) else [
            '후보를 검토했지만 요청한 데이터의 의미와 조건을 함께 충족한다고 판단할 근거가 부족해요.']
    return gaps, suggestion


def approval_judgment(query, plan, candidates):
    """Preassemble citation options; only the agent may approve their meaning.

    Lexical matching here is an evidence-provenance prefilter, not a relevance
    classifier. The model sees the preserved request and complete bounded
    metadata context, and can reject every option using a minimal wire response.
    """
    validate_plan(plan)
    if not isinstance(query, str) or not query.strip() or len(query.encode('utf-8')) > 2048:
        raise ValueError('invalid judgment query')
    views = _views(candidates, plan)
    available = {candidate['dataset_id']: candidate for candidate in prepare_candidates(candidates, plan=plan)}
    view_rows = {index: view for index, view in enumerate(views, 1)}
    choices_by_need = []
    for need in plan['needs']:
        choices = []
        required = required_conditions(plan, need)
        for index, view in view_rows.items():
            candidate = available[view['dataset_id']]
            if plan.get('source_ids') and candidate['source_id'] not in plan['source_ids']:
                continue
            evidence, missing = _option_evidence(view, candidate, required)
            if not missing:
                choices.append({'candidate': index, 'indicator': need['indicator'], 'evidence': evidence})
        choices_by_need.append(choices)
    # Round-robin concepts before adding each concept's next dataset so the first
    # requested indicator cannot consume the complete 24-option budget.
    options = []
    for rank in range(max((len(choices) for choices in choices_by_need), default=0)):
        for choices in choices_by_need:
            if rank < len(choices):
                options.append({'option': len(options) + 1, **choices[rank]})
                if len(options) == 24:
                    break
        if len(options) == 24:
            break

    def payload_for_options():
        shown = {option['candidate'] for option in options}
        return {'query_data': query, **_request_context(plan),
            'candidates_data': [{'candidate': index, 'source_id': view['source_id'],
                                'cosine': view['cosine'], 'metadata': view['metadata'],
                                **({'topic_matches': view['topic_matches']} if 'topic_matches' in view else {}),
                                **({'topic_context': view['topic_context']} if 'topic_context' in view else {})}
                               for index, view in view_rows.items() if index in shown],
            'options_data': options,
            'prefilter_basis': 'lexical citation matching only; agent semantic approval required'}

    payload = payload_for_options()
    prompt = _dump(payload)
    omitted = 0
    while len(prompt.encode('utf-8')) > MAX_PROMPT_BYTES and options:
        options.pop()
        omitted += 1
        payload = payload_for_options()
        payload['omitted_options_for_budget'] = omitted
        prompt = _dump(payload)
    if len(prompt.encode('utf-8')) > MAX_PROMPT_BYTES:
        raise ValueError('judgment prompt exceeds byte budget')
    option_map = {option['option']: option for option in options}
    schema = {'type': 'object', 'properties': {'accepted_options': {
        'type': 'array', 'items': {'type': 'integer', 'enum': list(option_map) or [-1]},
        'maxItems': MAX_SELECTIONS}}, 'required': ['accepted_options'], 'additionalProperties': False}
    skill = SKILL_PATH.read_text(encoding='utf-8')
    # Every shared evidence/security rule remains active. Exclude only the
    # inactive snippet-output protocol, which conflicts with this wire format.
    common = skill.split('## Compact judgment mode', 1)[0]
    option_mode = skill.partition('## Option approval mode')[2]
    instructions = common + '\n## Active mode: option approval\n' + option_mode + (
        '\nReturn ONLY {"accepted_options":[integer option IDs]}, at most ten IDs. '
        'Lexical matching DOES NOT establish meaning; reject merely generic titles. '
        'Read every proposed citation in its supplied metadata context. '
        'Prefer higher supplied query-dataset cosine among valid options and cover different '
        'requested indicators where supported. Never invent IDs or text outside the schema.\n')

    def decode(value):
        if (not isinstance(value, dict) or set(value) != {'accepted_options'} or
                not isinstance(value['accepted_options'], list) or len(value['accepted_options']) > MAX_SELECTIONS):
            raise ValueError('invalid vector option approval')
        accepted, seen = [], set()
        for identifier in value['accepted_options']:
            if type(identifier) is not int or identifier not in option_map or identifier in seen:
                raise ValueError('unknown or duplicate vector option')
            seen.add(identifier)
            accepted.append(option_map[identifier])
        accepted.sort(key=lambda option: (
            -view_rows[option['candidate']]['cosine'], option['option']))
        groups, selections = set(), []
        for option in accepted:
            view = view_rows[option['candidate']]
            group = (view['source_id'], option['indicator'])
            if group in groups:
                continue
            groups.add(group)
            selections.append({'dataset_id': view['dataset_id'], 'indicator': option['indicator'],
                               'accepted': True, 'evidence': copy.deepcopy(option['evidence'])})
        return validate_decision({'selections': selections}, plan, candidates)

    return schema, instructions, prompt, decode


def _has_value(quote, value):
    text = quote.casefold()
    for term in _ALIASES.get(value, (value,)):
        term = term.casefold()
        # Avoid 2024 matching 20240, API matching rapid, or Japan matching Japanese.
        if term.isascii():
            if re.search(r'(?<![\w])' + re.escape(term) + r'(?![\w])', text):
                return True
        elif term == '한국' and re.search(r'한국(?!어|인|학)', text):
            return True
        elif term != '한국' and term in text:
            return True
    return False


def _range_contains(quote, requested, *,dates=False):
    atom = r'\d{4}-\d{2}-\d{2}' if dates else r'\d{4}'
    match = re.fullmatch(r'\s*(' + atom + r')\s*(?:\.\.|[-~–—/]|부터)\s*(' + atom + r')(?:까지)?\s*', quote)
    if not match:
        return False
    if dates:
        try:
            date.fromisoformat(match[1])
            date.fromisoformat(match[2])
        except ValueError:
            return False
    bounds = requested.split('..')
    return match[1] <= bounds[0] <= bounds[-1] <= match[2]


def _supports(field, quote, token, requested):
    kind = token.split(':', 1)[0]
    if field not in _FIELD_RULES[kind] or quote.strip().casefold() in _UNKNOWN:
        return False
    # Agent context judgment remains necessary; this guard also catches common
    # directly negated evidence rather than accepting a matching quoted token.
    if re.search(r'제외|미포함|제공하지|지원하지|불가|\b(?:not|except|excluding|unavailable|prohibited)\b', quote, re.I):
        return False
    if kind == 'relevance':
        return True
    if kind == 'formats_any':
        return any(_has_value(quote, value) for value in requested.split('|'))
    if kind == 'years_range':
        return _range_contains(quote, requested)
    if kind == 'dates':
        return _range_contains(quote, requested, dates=True) if '..' in requested else _has_value(quote, requested)
    if kind == 'years' and field in {'period', 'temporal_coverage'}:
        return _has_value(quote, requested) or _range_contains(quote, requested)
    if kind == 'free_only':
        return (field == 'free' and quote == 'true') or any(_has_value(quote, word) for word in ('무료', 'free of charge', 'no charge', 'CC0'))
    if kind == 'commercial_only':
        return ((field == 'commercial' and quote == 'true') or
                any(_has_value(quote, word) for word in ('상업적 이용 가능', '상업적 이용 허용', 'commercial use permitted', 'CC0', 'CC BY 4.0')))
    return _has_value(quote, requested)


def _structured_conflict(metadata, token, value):
    """Explicit typed negatives override a cherry-picked narrative citation."""
    kind = token.split(':', 1)[0]
    if kind == 'years_range' or (kind == 'dates' and '..' in value):
        for field in ('period', 'temporal_coverage'):
            explicit = _field_text(metadata.get(field))
            if re.search(r'\b(?:not\s*continuous|discontinuous|excluding|except|gaps?|intermittent)\b|불연속|제외|누락|연속.{0,8}(?:않|아니)', explicit, re.I):
                return True
    if kind in {'free_only', 'commercial_only'}:
        key = 'free' if kind == 'free_only' else 'commercial'
        if metadata.get(key) is False or metadata.get(key) == 'false':
            return True
        license_ = _field_text(metadata.get('license', ''))
        if kind == 'commercial_only' and re.search(r'non.?commercial|\bNC\b|상업.{0,10}(?:불가|금지)|비영리', license_, re.I):
            return True
        if kind == 'free_only' and re.search(r'\bpaid\b|유료|요금 부과', _field_text(metadata.get('access', '')), re.I):
            return True
        return False
    fields = {'countries': ('coverage_countries', 'countries'), 'regions': ('coverage_regions', 'regions'),
              'formats': ('formats',), 'formats_any': ('formats',), 'years': ('coverage_years',)}.get(kind, ())
    alternatives = value.split('|') if kind == 'formats_any' else [value]
    for field in fields:
        explicit = metadata.get(field)
        if isinstance(explicit, list) and explicit and all(isinstance(v, (str, int)) for v in explicit):
            if not any(_has_value(str(v), alternative) for v in explicit for alternative in alternatives):
                return True
    if kind == 'years_range':
        explicit = metadata.get('coverage_years')
        if isinstance(explicit, list) and all(type(v) is int for v in explicit):
            start, stop = map(int, value.split('..'))
            if not set(range(start, stop + 1)) <= set(explicit):
                return True
    if kind in {'years', 'years_range'}:
        for field in ('period', 'temporal_coverage'):
            explicit = _field_text(metadata.get(field))
            if re.fullmatch(r'\s*\d{4}\s*(?:\.\.|[-~–—/]|부터)\s*\d{4}(?:까지)?\s*', explicit):
                if not _range_contains(explicit, value):
                    return True
    return False


def validate_decision(value, plan, candidates):
    validate_plan(plan)
    if not isinstance(value, dict) or set(value) != {'selections'} or not isinstance(value['selections'], list) or len(value['selections']) > MAX_SELECTIONS:
        raise ValueError('invalid vector judgment')
    views = {view['dataset_id']: view['metadata'] for view in _views(candidates, plan)}
    candidates_by_id = {candidate['dataset_id']: candidate for candidate in prepare_candidates(candidates, plan=plan)
                        if candidate['dataset_id'] in views}
    needs = {need['indicator']: need for need in plan['needs']}
    seen, representatives, validated = set(), {}, []
    for selection in value['selections']:
        if (not isinstance(selection, dict) or set(selection) != {'dataset_id', 'indicator', 'accepted', 'evidence'} or
                type(selection['accepted']) is not bool or not isinstance(selection['evidence'], list) or len(selection['evidence']) > 48):
            raise ValueError('invalid vector selection')
        ident, indicator = selection['dataset_id'], selection['indicator']
        if not isinstance(ident, str) or ident not in candidates_by_id or not isinstance(indicator, str) or indicator not in needs:
            raise ValueError('unknown vector selection')
        if (ident, indicator) in seen:
            raise ValueError('duplicate vector selection')
        seen.add((ident, indicator))
        candidate = candidates_by_id[ident]
        if selection['accepted']:
            representative = representatives.setdefault((candidate['source_id'], indicator), ident)
            if representative != ident:
                raise ValueError('multiple representative datasets for one site and indicator')
        if plan.get('source_ids') and candidate['source_id'] not in plan['source_ids'] and selection['accepted']:
            raise ValueError('unrequested source selected')
        required = required_conditions(plan, needs[indicator])
        supplied, view = set(), views[ident]
        for evidence in selection['evidence']:
            if not isinstance(evidence, dict) or set(evidence) != {'condition', 'field', 'quote'}:
                raise ValueError('invalid vector evidence')
            token, field, quote = (evidence.get(key) for key in ('condition', 'field', 'quote'))
            if (not all(isinstance(v, str) for v in (token, field, quote)) or token not in required or
                    token in supplied or field not in view or not 2 <= len(quote) <= 100 or
                    quote not in view[field] or not _supports(field, quote, token, required[token]) or
                    _structured_conflict(candidate['metadata'], token, required[token])):
                raise ValueError('unsubstantiated vector condition')
            supplied.add(token)
        if selection['accepted'] and supplied != set(required):
            raise ValueError('missing required vector evidence')
        validated.append(copy.deepcopy(selection))
    return {'selections': validated}


def _url(value):
    if not isinstance(value, str) or len(value) > 4096:
        return None
    try:
        url = urlsplit(value)
        if url.scheme not in {'https', 'http'} or not url.hostname or url.username or url.password:
            return None
        if url.hostname == 'localhost' or url.hostname.endswith(('.localhost', '.local')):
            return None
        try:
            if not ipaddress.ip_address(url.hostname).is_global:
                return None
        except ValueError:
            pass
        return value
    except ValueError:
        return None


def merge_decisions(plan, candidates, decisions):
    """Merge independently checked packets without dropping requested concepts."""
    available = {row['dataset_id']: row for row in prepare_candidates(candidates, plan=plan)}
    selections = [selection for decision in decisions
                  for selection in validate_decision(decision, plan, candidates)['selections']
                  if selection['accepted']]
    selections.sort(key=lambda row: (-available[row['dataset_id']]['cosine'], row['dataset_id']))
    unique, seen = [], set()
    for selection in selections:
        key = (available[selection['dataset_id']]['source_id'], selection['indicator'])
        if key not in seen:
            seen.add(key); unique.append(selection)
    chosen = []
    for need in plan['needs']:
        first = next((row for row in unique if row['indicator'] == need['indicator']), None)
        if first is not None:
            chosen.append(first)
    for selection in unique:
        if len(chosen) == MAX_SELECTIONS:
            break
        if selection not in chosen:
            chosen.append(selection)
    return validate_decision({'selections': chosen}, plan, candidates)


def parallel_input_bounds(contracts, remaining_tokens):
    """Reserve the remaining input budget equally for bounded evidence packets.

    Wire bytes are a size guard, not token counts (especially for Korean UTF-8).
    Each branch reserves at least 8k tokens; reported usage must fit its actual
    reservation, including provider framing. Oversized/empty packets use one
    judge. This is admission accounting, not an upstream hard token cap.
    """
    if len(contracts) != 2 or not all(json.loads(c[2])['options_data'] for c in contracts):
        return None
    sizes = [len((c[1]+c[2]).encode('utf-8')) + len(_dump(c[0]).encode('utf-8')) for c in contracts]
    bound = min(16000, remaining_tokens // 2)
    return [bound,bound] if max(sizes) <= 16000 and bound >= 8000 else None


def compose_result(plan, candidates, decision, sources, retrieval):
    decision = validate_decision(decision, plan, candidates)
    available = {candidate['dataset_id']: candidate for candidate in prepare_candidates(candidates, plan=plan)}
    registry = {source['id']: source for source in sources} if isinstance(sources, list) else dict(sources)
    accepted = [selection for selection in decision['selections'] if selection['accepted']]
    accepted.sort(key=lambda selection: (-available[selection['dataset_id']]['cosine'], selection['dataset_id']))
    source_order = []
    usable = []
    for selection in accepted:
        candidate = available[selection['dataset_id']]
        source = registry.get(candidate['source_id'])
        if not source or not _url(source.get('url')):
            continue
        if candidate['source_id'] not in source_order:
            if len(source_order) >= MAX_SITE_RESULTS:
                continue
            source_order.append(candidate['source_id'])
        usable.append(selection)
    groups, relaxation = [], None
    for need in plan['needs'] if not plan['question'] else []:
        sites = {}
        for selection in usable:
            if selection['indicator'] != need['indicator']:
                continue
            candidate = available[selection['dataset_id']]
            metadata, source = candidate['metadata'], registry[candidate['source_id']]
            card = sites.setdefault(candidate['source_id'], {'source_id': candidate['source_id'], 'name': source['name'],
                'url': source['url'], 'evidence': [], 'cosine_similarity': candidate['cosine']})
            formats = metadata.get('formats', [metadata.get('format', '')])
            if not isinstance(formats, list):
                formats = [formats]
            formats = [format_ for format_ in formats if format_ in FORMATS]
            conditions = effective_plan(plan, need)
            relevance = next(item['quote'] for item in selection['evidence'] if item['condition'] == 'relevance')
            card['evidence'].append({'title': str(metadata.get('title', ''))[:500],
                'summary': '카탈로그 메타데이터의 관련 근거: ' + relevance,
                'evidence_url': _url(metadata.get('url')) or _url(metadata.get('metadata_url')) or source['url'],
                'checked_on': str(candidate.get('checked_at', ''))[:40],
                'countries': list(conditions.get('countries', [])), 'regions': list(conditions.get('regions', [])),
                'years': list(conditions.get('years', [])), 'formats': formats, 'fields': list(conditions.get('fields', [])),
                'delivery': [conditions['delivery']] if conditions.get('delivery') else [],
                'access_note': '수집된 공식 메타데이터를 기준으로 판단했어요. 원문·다운로드의 현재 작동 여부는 확인하지 않았어요.',
                'dataset_id': candidate['dataset_id'], 'cosine': candidate['cosine'],
                'cosine_similarity': candidate['cosine'],
                'metadata_citations': copy.deepcopy(selection['evidence']), 'input_hash': candidate.get('input_hash', '')})
            matches = safe_topic_matches(candidate.get('topic_matches'))
            if matches:
                card['evidence'][-1]['topic_matches'] = matches
            context = safe_topic_context(candidate.get('topic_context'))
            if context is not None:
                card['evidence'][-1]['topic_context'] = context
        gaps, suggestion = ([], None) if sites else _empty_diagnosis(plan, need, candidates)
        relaxation = relaxation or suggestion
        groups.append({'indicator': need['indicator'], 'name': INDICATORS[need['indicator']],
            'reason': need['reason'], 'sites': list(sites.values()),
            'unverified': gaps})
    total = len({site['source_id'] for group in groups for site in group['sites']})
    state = ('clarify' if plan['question'] else 'results' if groups and all(group['sites'] for group in groups)
             else 'partial' if total else 'unverified')
    result = {'version': 2, 'state': state, 'question': plan['question'], 'plan': copy.deepcopy(plan),
        'groups': groups, 'total': total, 'datasets': [], 'concept_id': 'site_recommendations',
        'scope': {'countries': requested_countries(plan), 'basis': 'dataset_coverage', 'unavailable_countries': []},
        'evidence_scope': {'sites': len({candidate['source_id'] for candidate in available.values()}),
                           'registered_sites': len(registry), 'basis': 'catalog_metadata'},
        'relaxation': relaxation, 'retrieval': copy.deepcopy(retrieval),
        'notice': ('검색한 후보에서 조건을 모두 뒷받침하는 근거를 확인하지 못했어요. 전체 자료에 없다는 뜻은 아니에요.'
                   if state in {'unverified', 'partial'} else
                   '임베딩 유사도로 찾은 후보를 메타데이터 근거로 검토했어요. 유사도는 정확도나 검증 확률이 아니에요.')}
    return validate_site_result(result)
