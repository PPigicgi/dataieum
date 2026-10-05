"""One bounded metadata judgment; exact provenance, no invented scores or scope.

The model assesses meaning, not source availability. Server checks prove which
visible metadata it used, not that an arbitrary natural-language judgment is true.
"""
import copy
import json
from pathlib import Path

from .coverage import SYNTHETIC
from .similarity_results import _assess_filters, _eligible, select_candidates
from .site_search import effective_plan, validate_plan, INDICATORS
from .vector_judge import _field_text, _UNKNOWN

VERSION = 'metadata-relevance-v3'
MAX_PROMPT_BYTES = 23000  # Existing provider contract allows at most 24,000.
MAX_CANDIDATES = 40
SKILL_PATH = Path(__file__).with_name('skills') / 'dataset-relevance' / 'SKILL.md'
MEANING_FIELDS = {'title', 'description', 'description_tail', 'subjects', 'fields', 'native_catalog_path', 'native_catalog_paths',
                  'survey_name', 'survey_title', 'native_survey_name', 'statistical_survey_name'}


def dump(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def _project(metadata):
    remaining, result = 1100, {}
    ordered = ['title', 'description', 'coverage_countries', 'countries', 'coverage_regions',
               'regions', 'region', 'coverage_years', 'temporal_coverage', 'period', 'dates',
               'native_catalog_path', 'native_catalog_paths', 'subjects', 'fields', 'survey_name', 'survey_title',
               'native_survey_name', 'statistical_survey_name', 'format', 'formats', 'unit',
               'frequency', 'measurement_basis', 'geography_level', 'method', 'delivery',
               'access', 'free', 'commercial', 'license']
    for field in ordered:
        value = _field_text(metadata.get(field))
        if value.strip().casefold() in _UNKNOWN:
            continue
        if field in {'coverage_countries', 'countries', 'coverage_regions', 'regions', 'region'} and SYNTHETIC.search(value):
            continue  # Ingest-generated country hints are not official scope.
        size = min(300 if field == 'title' else 650 if field == 'description' else 240, remaining)
        raw=value.encode('utf-8')
        if field == 'description' and len(raw)>size and size>=2:
            # Keep two exact excerpts: dataset fields often follow introductory prose.
            excerpts={field:raw[:size//2].decode('utf-8',errors='ignore'),
                      'description_tail':raw[-(size-size//2):].decode('utf-8',errors='ignore')}
            for key,text in excerpts.items():
                if text.strip():
                    result[key]=text
                    remaining-=len(text.encode('utf-8'))
            continue
        text = raw[:size].decode('utf-8', errors='ignore')
        if text.strip():
            result[field] = text
            remaining -= len(text.encode('utf-8'))
        if remaining <= 0:
            break
    return result


def contract(query, plan, candidates, sources, *, per_need_limit=None, compact=False):
    validate_plan(plan)
    if per_need_limit not in (None,1,2):raise ValueError('Invalid per-need result bound')
    if not isinstance(query, str) or not query.strip() or len(query.encode('utf-8')) > 2048:
        raise ValueError('Invalid relevance query')
    rows = select_candidates(plan, candidates, sources, limit=MAX_CANDIDATES, filter_related=False)
    exclusions = plan.get('related_exclusions', [])
    scope_fields=('countries','regions','years','years_mode','dates','frequency','formats','fields','delivery',
                  'free_only','commercial_only','source_ids','geography_level','additional_requirements','unit','measurement_basis')
    needs = [{ 'need': i, 'subject': need.get('subject', ''), 'indicator': need['indicator'],
               'scope': {k:v for k,v in effective_plan(plan,need).items() if k in scope_fields}}
             for i, need in enumerate(plan['needs'], 1)]
    if per_need_limit:
        from .ontology_definitions import definitions
        vocabulary={c['id']:c for c in definitions()['concepts']}
        for need in needs:
            concept=vocabulary[need['indicator']]
            need['concept_definition']=concept['definition']
            need['concept_label']=concept['label']
            need['concept_unit']=concept['canonical_unit']
    packet = {'version': VERSION, 'query_data': query, 'semantic_query': plan.get('semantic_query', ''),
              'needs': needs, 'related_allowed': plan.get('related_mode') != 'exclude',
              'related_exclusions': {key: INDICATORS[key] for key in exclusions},
              'candidates_data': []}
    for number, row in enumerate(rows, 1):
        eligible = [i for i, need in enumerate(plan['needs'], 1)
                    if _eligible(_assess_filters(plan, need, row)) and
                    (per_need_limit is None or i in row.get('retrieval_scores',{}))]
        view = {'candidate': number, 'eligible_needs': eligible, 'metadata': _project(row['metadata'])}
        packet['candidates_data'].append(view)
        if len(dump(packet).encode('utf-8')) > MAX_PROMPT_BYTES:
            packet['candidates_data'].pop()
            break
    if len(dump(packet).encode('utf-8')) > MAX_PROMPT_BYTES:
        raise ValueError('Relevance context exceeds budget')
    visible = {view['candidate']: view for view in packet['candidates_data']}
    max_matches = min(len(needs)*per_need_limit if per_need_limit else 10, len(visible))
    if per_need_limit:packet['max_matches_per_need']=per_need_limit
    schema = {'type': 'object', 'properties': {'matches': {'type': 'array', 'maxItems': max_matches,
        'items': {'type': 'object', 'properties': {
            'candidate': {'type': 'integer', 'enum': list(visible) or [0]},
            'need': {'type': 'integer', 'enum': [n['need'] for n in needs] or [0]},
            'tier': {'type': 'string', 'enum': ['direct', 'related']}},
            'required': ['candidate', 'need', 'tier'], 'additionalProperties': False}}},
        'required': ['matches'], 'additionalProperties': False}
    if exclusions:
        item_schema = schema['properties']['matches']['items']
        item_schema['properties']['excluded'] = {'type':'array','maxItems':len(exclusions),
            'items':{'type':'string','enum':exclusions}}
        item_schema['required'].append('excluded')

    def check(value):
        if not isinstance(value, dict) or set(value) != {'matches'} or not isinstance(value['matches'], list) or len(value['matches']) > max_matches:
            raise ValueError('Invalid relevance decision')
        seen = set();seen_needs={}
        for match in value['matches']:
            required = {'candidate', 'need', 'tier'} | ({'excluded'} if exclusions else set())
            if not isinstance(match, dict) or set(match) != required:
                raise ValueError('Invalid relevance selection')
            ident, need, tier = (match[key] for key in ('candidate', 'need', 'tier'))
            if type(ident) is not int or ident not in visible or ident in seen:
                raise ValueError('Unknown or repeated relevance candidate')
            view = visible[ident]
            if type(need) is not int or need not in view['eligible_needs']:
                raise ValueError('Relevance selection changed requested scope')
            seen_needs[need]=seen_needs.get(need,0)+1
            if per_need_limit and seen_needs[need]>per_need_limit:raise ValueError('Repeated related concept')
            if tier not in ('direct', 'related') or tier == 'related' and not packet['related_allowed']:
                raise ValueError('Invalid or excluded relevance tier')
            if exclusions:
                excluded = match['excluded']
                if (not isinstance(excluded, list) or any(not isinstance(key,str) or key not in exclusions for key in excluded)
                        or len(set(excluded)) != len(excluded)):
                    raise ValueError('Invalid related exclusion judgment')
            if not any(field in MEANING_FIELDS and text.strip() for field,text in view['metadata'].items()):
                raise ValueError('Missing visible metadata evidence')
            seen.add(ident)
        return copy.deepcopy(value)

    def apply(value):
        accepted = []
        for match in check(value)['matches']:
            if match['tier'] == 'related' and match.get('excluded'):
                continue
            view, row = visible[match['candidate']], rows[match['candidate']-1]
            if per_need_limit:row={**row,'cosine':row['retrieval_scores'][match['need']]}
            # This is the supplied decision context, not a model-selected citation
            # or a proof of semantic correctness. All excerpts come from this row.
            evidence = [{'condition': 'relevance_context', 'field': 'description' if field=='description_tail' else field,
                         'quote': text} for field,text in view['metadata'].items() if text.strip()]
            accepted.append({**row, 'relevance_tier': match['tier'], 'relevance_need': match['need'],
                             'relevance_exclusions': list(match.get('excluded', [])),
                             'relevance_evidence': evidence})
        accepted.sort(key=lambda row: (row['relevance_tier'] != 'direct', -row['cosine'], row['dataset_id']))
        return {'candidates': accepted[:10], 'assessment': {
            'version': VERSION, 'eligible_candidates': len(rows), 'judged_candidates': len(visible),
            'omitted_for_budget': len(rows)-len(visible), 'accepted_candidates': len(accepted)}}

    instructions=SKILL_PATH.read_text(encoding='utf-8')
    if per_need_limit:instructions+=f'\nSelect at most {per_need_limit} supported direct matches per need, retaining a fallback for deduplication. Keep eligible_needs; do not fill a quota. The supplied concept_definition and concept_unit define each requested measure precisely. Domestic moves, even by foreign nationals, are not international migration across national borders. Fertility, crude birth rates and birth counts are different measures. A related concept still requires a DIRECT match to that concept definition.\n'
    if compact and len(needs)==1 and not packet['related_allowed'] and not exclusions and per_need_limit is None:
        # Only identifiers vary in a one-need, direct-only decision. Preserve
        # the same evidence and scope validation without repeated output fields.
        compact_schema={'type':'object','properties':{'candidate_ids':{'type':'array','maxItems':max_matches,
            'items':{'type':'integer','enum':list(visible) or [0]}}},
            'required':['candidate_ids'],'additionalProperties':False}
        instructions=instructions.replace(
            'For each selected candidate output only candidate, need and tier (plus excluded\n'
            'when that field is required by the schema).',
            'Output only {"candidate_ids":[IDs]} for supported direct matches to need 1.\n'
            'Do not output need, tier, or matches; all selected IDs mean need 1 and direct.')
        def expanded(value):
            if not isinstance(value,dict) or set(value)!={'candidate_ids'} or not isinstance(value['candidate_ids'],list):
                raise ValueError('Invalid compact relevance decision')
            return {'matches':[{'candidate':ident,'need':1,'tier':'direct'} for ident in value['candidate_ids']]}
        def compact_check(value):
            check(expanded(value))
            return copy.deepcopy(value)
        def compact_apply(value):
            return apply(expanded(value))
        return compact_schema,instructions,dump(packet),compact_check,compact_apply
    return schema, instructions, dump(packet), check, apply
