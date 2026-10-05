"""Bounded user intent and evidence-backed recommendations of registered sites.

Model output describes the request only. All claims about providers come from
reviewed public evidence, never the model or the browser's conversation state.
"""
import copy
import json
import re
from datetime import date
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlsplit
from .catalog_discovery import country_list
from .security import check_prompt

INDICATORS = {
    'population':'인구', 'fertility_rate':'합계출산율', 'death_rate':'조사망률',
    'net_migration':'국제순이동', 'elderly_share':'65세 이상 인구비율',
    'households':'가구', 'business_count':'사업체·점포 수',
    'business_sales':'업종별 매출', 'foot_traffic':'유동인구',
    'temperature':'기온', 'precipitation':'강수량', 'traffic_volume':'차량 통행량',
    'hospital_locations':'병원 위치', 'medical_specialties':'진료과목', 'opening_hours':'운영·진료시간',
    'pharmacies':'약국', 'health_indicators':'보건 통계', 'hospital_beds':'병상 수',
    'health_access':'의료 접근성', 'life_expectancy':'기대수명', 'boundaries':'행정구역 경계',
    'facilities':'생활편의시설', 'housing':'주택', 'employment':'고용',
    'income':'소득', 'prices':'물가', 'energy':'에너지', 'environment':'환경',
    'education':'교육', 'agriculture':'농림어업', 'culture':'문화·관광',
    'finance':'금융', 'trade':'무역', 'welfare':'복지', 'digital':'정보통신',
    'law':'법률', 'science':'실험·연구', 'safety':'안전', 'water':'물·홍수',
    'nature':'생태', 'geology':'지질', 'government':'행정·조달',
    'development':'국제개발', 'diplomacy':'외교·국방',
    'transport':'교통·이동','geography':'토지·행정구역','basemap':'지도·측량·지형',
    'spatial_services':'공간자료·조회서비스','weather':'기후·기상','economy':'경제 지표',
    'social':'사회 지표','research_data':'학술·연구 자료','statistics':'종합통계·통계체계',
    'other':'기타 요청 자료'}
FILTERS = ('countries','regions','years','dates','frequency','formats','fields','delivery','free_only','commercial_only','source_ids','geography_level','workflow_ids','additional_requirements','related_exclusions')
FORMATS = ('CSV','JSON','XML','XLSX','XLS','SHP','GeoJSON','PDF','SDMX','NetCDF','WMS','WFS')
MAX_SITE_RESULTS = 10
SCOPE_FIELDS = {'countries':5,'regions':4,'years':10,'dates':2,'geography_level':1,'additional_requirements':4}
RELAXATION_LABELS = {'formats': '파일 형식', 'period': '기간', 'frequency': '시간 단위',
                     'geography_level': '지역 단위', 'regions': '세부 지역', 'delivery': '제공 방식', 'countries':'국가'}
REGION_NAMES={'서울':'서울특별시','부산':'부산광역시','대구':'대구광역시',
              '인천':'인천광역시','광주':'광주광역시','대전':'대전광역시',
              '울산':'울산광역시','세종':'세종특별자치시','제주':'제주특별자치도'}


def _preserve_simple_region(plan, previous, query):
    """Ground a fresh, whole region + known indicator phrase before retrieval.

    This is deliberately not a general place-name extractor. Provider names,
    comparisons, exclusions, additional clauses and followups remain modeled.
    """
    if previous is not None or not isinstance(query,str) or len(plan['needs'])!=1 or plan['question']:
        return
    # These short names can denote a different city or an entire province.
    # Keep ambiguous forms in the ordinary interpretation/clarification path.
    regions={**{key:value for key,value in REGION_NAMES.items() if key not in {'광주','제주'}},
             **{value:value for value in REGION_NAMES.values()}}
    pattern='|'.join(re.escape(value) for value in sorted(regions,key=len,reverse=True))
    match=re.fullmatch(rf'({pattern})(?:의)?\s+(.+)',query.strip())
    if not match:return
    from .ontology_definitions import definitions
    identifiers=set()
    for concept in definitions()['concepts']:
        names={concept['label'],*concept['aliases']}
        names_pattern='|'.join(re.escape(value) for value in sorted(names,key=len,reverse=True))
        if re.fullmatch(rf'(?:{names_pattern})(?:\s*(?:데이터|자료|통계)(?:를)?)?'
                        r'(?:\s*(?:찾아줘|알려줘|보여줘|찾아주세요|알려주세요|보여주세요))?[.!?]?',match[2]):
            identifiers.add(concept['id'])
    need=plan['needs'][0]
    if identifiers!={need['indicator']}:return
    region=regions[match[1]]
    plan['regions']=[region]
    for scope in need.get('scope',[]):
        if scope['field']=='regions':scope.update(values=[region],mode='list')


def single_condition_relaxation(field):
    label = RELAXATION_LABELS[field]
    return {'field': field, 'label': label + ' 조건만 빼고 다시 찾기',
            'query': label + ' 조건만 해제하고, 방금 검색한 데이터 종류와 나머지 모든 조건은 그대로 유지해 주세요.'}


def scoped_condition_relaxation(field, indicator):
    label=RELAXATION_LABELS[field]
    return {'field':field,'need_indicator':indicator,'label':label+' 조건만 빼고 다시 찾기',
            'query':INDICATORS[indicator]+' 자료의 '+label+' 조건만 해제하고, 다른 데이터 종류와 나머지 모든 조건은 그대로 유지해 주세요.'}


def relaxation_concept(plan):
    if len(plan.get('needs', [])) != 1:
        return None
    try:
        concept = selected_concept(plan.get('selected_concept') or plan['needs'][0]['indicator'])
        # A routing topic such as environment cannot preserve "indoor PM2.5"
        # when old prose is removed. Only a defined indicator is specific enough
        # for this bounded shortcut; broad topics keep ordinary follow-up input.
        return concept if concept['kind'] == 'indicator' else None
    except ValueError:
        # Free-form concepts may encode meaning/filters only in purpose prose.
        # Do not discard that meaning to create a one-click action.
        return None


def _explicit_relaxation(previous, query, semantic_query=''):
    if previous is not None:
        for need in previous['needs']:
            for field in ('countries','regions','period'):
                if query!=scoped_condition_relaxation(field,need['indicator'])['query']:continue
                result=copy.deepcopy(validate_plan(previous))
                target=next(n for n in result['needs'] if n['indicator']==need['indicator'])
                fields=('years','dates') if field=='period' else (field,)
                target['scope']=[s for s in target.get('scope',[]) if s['field'] not in fields]
                target['scope'].extend({'field':key,'values':[],'mode':'list'} for key in fields)
                target['clear']=[]
                # An exact one-click action changes this scope only. Preserve
                # all other needs and the already normalized semantic meaning.
                result.update(question='',clear=[],followup=True,selection_action='keep')
                return validate_plan(result)
    concept = relaxation_concept(previous) if previous is not None else None
    if concept is None:
        return None
    field = next((field for field in RELAXATION_LABELS
                  if query == single_condition_relaxation(field)['query']), None)
    if field is None:
        return None
    result = copy.deepcopy(validate_plan(previous))
    fields = ('years', 'dates') if field == 'period' else (field,)
    for key in fields:
        result[key] = [] if isinstance(result[key], list) else ''
        for need in result['needs']:
            if key in need:
                need[key] = [] if isinstance(need[key], list) else ''
            need['scope'] = [entry for entry in need.get('scope', []) if entry['field'] != key]
            need['clear'] = []
    # A published button is an explicit action. Luna still interprets the turn,
    # but may not alter other conditions or replace its selected concept.
    # Generated summaries can still say "CSV only" after formats was cleared.
    # For this known concept rebuild active prose; the original request remains
    # in the conversation. Subject and every remaining structured filter survive.
    result['purpose'] = concept['label'] + ' 자료 탐색'
    # Exact-filter removal does not remove the normalized data meaning. Prefer
    # Luna's fresh wording; legacy requests can retain the last normalized one.
    result['semantic_query'] = semantic_query or previous.get('semantic_query','')
    result['needs'][0]['reason'] = concept['label'] + ' 자료 탐색'
    result.update(question='', clear=[], followup=True, selection_action='keep')
    return validate_plan(result)


def effective_plan(plan, need):
    """Apply only this indicator's explicit overrides to the shared criteria."""
    result={**plan,**{k:need[k] for k in ('frequency','formats','fields','unit','measurement_basis') if need.get(k)}}
    for entry in need.get('scope',[]):
        field=entry['field'];values=entry['values']
        if field=='years':result.update(years=[int(v) for v in values],years_mode=entry['mode'])
        elif field=='geography_level':result[field]=values[0] if values else ''
        else:result[field]=list(values)
    return result


def requested_countries(plan):
    scopes=[effective_plan(plan,n)['countries'] for n in plan['needs']]
    if any(not countries for countries in scopes):return []
    return list(dict.fromkeys(c for countries in scopes for c in countries)) if scopes else plan['countries']

def plan_schema():
    from .ontology_definitions import workflow_catalog
    string=lambda n:{'type':'string','maxLength':n}
    array=lambda item,n:{'type':'array','items':item,'maxItems':n}
    props={'purpose':string(120),'semantic_query':string(320),'needs':array({'type':'object','properties':{
        'indicator':{'type':'string','enum':list(INDICATORS)},'reason':string(90),'subject':string(50),
        'frequency':string(40),'formats':array({'type':'string','enum':list(FORMATS)},4),'fields':array(string(40),8),
        'unit':string(40),'measurement_basis':{'type':'string','enum':['','observed','estimated','forecast','simulation']},
        'scope':array({'type':'object','properties':{
            'field':{'type':'string','enum':list(SCOPE_FIELDS)},'values':array(string(120),10),
            'mode':{'type':'string','enum':['list','range']}},
            'required':['field','values','mode'],'additionalProperties':False},len(SCOPE_FIELDS)),
        'clear':array({'type':'string','enum':['subject','frequency','formats','fields','unit','measurement_basis']},6)},
        'required':['indicator','reason','subject','frequency','formats','fields','unit','measurement_basis','scope','clear'],'additionalProperties':False},5),
        'need_selection':{'type':'string','enum':['explicit','workflow']},
        'selection_action':{'type':'string','enum':['','keep','replace']},
        'related_mode':{'type':'string','enum':['auto','include','exclude']},
        'related_exclusions':array({'type':'string','enum':list(INDICATORS)},10),
        'workflow_ids':array({'type':'string','enum':list(workflow_catalog())},2),
        'additional_requirements':array(string(120),4),
        'countries':array(string(60),5),'regions':array(string(60),4),
        'years':array({'type':'integer','minimum':1600,'maximum':2200},10),
        'years_mode':{'type':'string','enum':['range','list']},
        'dates':array(string(10),2),
        'frequency':string(40),'formats':array({'type':'string','enum':list(FORMATS)},4),
        'fields':array(string(40),8),'delivery':{'type':'string','enum':['','api','download']},
        'free_only':{'type':'boolean'},'commercial_only':{'type':'boolean'},
        'source_ids':array(string(60),3),'geography_level':string(40),
        'followup':{'type':'boolean'},'clear':array({'type':'string','enum':[*FILTERS,'subjects','needs']},len(FILTERS)+2),
        'question':string(120)}
    return {'type':'object','properties':props,'required':list(props),'additionalProperties':False}

def validate_plan(value):
    def validate(v,s):
        kind=s['type']
        if kind=='object':
            required=set(s['required']);legacy_optional={'workflow_ids','unit','measurement_basis','need_selection','additional_requirements','scope','related_mode','related_exclusions','selection_action','semantic_query'}
            if not isinstance(v,dict) or not required-legacy_optional<=set(v)<=required:raise ValueError('invalid plan keys')
            for k,c in s['properties'].items():
                if k in v:validate(v[k],c)
        elif kind=='array':
            if not isinstance(v,list) or len(v)>s['maxItems']:raise ValueError('invalid plan array')
            for item in v:validate(item,s['items'])
        elif kind=='string':
            if not isinstance(v,str) or len(v)>s.get('maxLength',200) or any(ord(c)<32 for c in v):raise ValueError('invalid plan string')
            if 'enum' in s and v not in s['enum']:raise ValueError('invalid plan enum')
        elif kind=='boolean':
            if type(v) is not bool:raise ValueError('invalid plan flag')
        elif kind=='integer':
            if type(v) is not int or not s['minimum']<=v<=s['maximum']:raise ValueError('invalid plan year')
    # UI-selected context is a separate, server-validated concept reference.
    # It is not an expanded fixed vocabulary that the model must memorize.
    if not isinstance(value,dict):raise ValueError('invalid plan')
    if value.get('selected_concept'):
        selected_concept(value['selected_concept'])
    elif 'selected_concept' in value and value['selected_concept']!='':
        raise ValueError('invalid selected concept')
    validate({k:v for k,v in value.items() if k!='selected_concept'},plan_schema());country_list(value['countries'])
    if len(json.dumps(value,ensure_ascii=False,separators=(',',':')).encode())>4096:raise ValueError('plan too large')
    for day in value['dates']:date.fromisoformat(day)
    if len({n['indicator'] for n in value['needs']})!=len(value['needs']):raise ValueError('duplicate need')
    for need in value['needs']:
        scopes=need.get('scope',[])
        if len({s['field'] for s in scopes})!=len(scopes):raise ValueError('duplicate scope field')
        for entry in scopes:
            field=entry['field'];values=entry['values']
            if len(values)>SCOPE_FIELDS[field] or any(not v.strip() for v in values):raise ValueError('invalid scope values')
            if field!='years' and entry['mode']!='list':raise ValueError('invalid scope mode')
            if field=='years' and any(not v.isascii() or not v.isdigit() or not 1600<=int(v)<=2200 for v in values):raise ValueError('invalid scope year')
            if field=='dates':
                for day in values:date.fromisoformat(day)
            if field=='countries':country_list(values)
    return value

def parse_context(raw):
    if not raw:return None
    if len(raw.encode())>4096:raise ValueError('context too large')
    value=validate_plan(json.loads(raw))
    check_prompt(json.dumps(value,ensure_ascii=False))
    return selection_plan(value,value['selected_concept']) if value.get('selected_concept') else value


@lru_cache(maxsize=1)
def _selected_concept_index(snapshot):
    # Key by the immutable snapshot itself so a replaced ontology cannot reuse
    # old definitions. Keep cached dictionaries private to avoid caller mutation.
    result = {}
    for node in json.loads(snapshot)['nodes']:
        result.setdefault(node['id'], node)
    return result


def selected_concept(identifier):
    from .concept_hierarchy import snapshot_bytes
    if not isinstance(identifier,str) or len(identifier)>80:raise ValueError('invalid selected concept')
    node=_selected_concept_index(snapshot_bytes()).get(identifier)
    if node is None:raise ValueError('unknown selected concept')
    return copy.deepcopy(node)


def selection_plan(context,identifier):
    node=selected_concept(identifier)
    plan=copy.deepcopy(validate_plan(context))
    same_selection = plan.get('selected_concept') == identifier
    routing=node['legacy_id'] or 'other'
    need=next((n for n in plan['needs'] if n['indicator']==routing),None)
    need=need or (plan['needs'][0] if plan['needs'] else node_plan('population')['needs'][0])
    # Once a single node is selected, make that node's geographic/time scope
    # the portable context, including when the original question had many needs.
    criteria=effective_plan(plan,need)
    for key in SCOPE_FIELDS:plan[key]=copy.deepcopy(criteria.get(key,[] if key!='geography_level' else ''))
    plan['years_mode']=criteria['years_mode']
    need.update(indicator=routing,reason='선택한 개념: '+node['label'])
    plan.update(selected_concept=identifier,needs=[need],purpose=node['label']+' 자료 탐색',
                need_selection='explicit',workflow_ids=[])
    if not same_selection:plan['semantic_query']=''
    return validate_plan(plan)


def node_plan(indicator, countries=None, context=None):
    """A selected ontology ID is explicit input, not a model interpretation."""
    if indicator not in INDICATORS or indicator == 'other':
        raise ValueError('select a defined data concept')
    if context is not None:
        plan = copy.deepcopy(validate_plan(context))
        selected = next((n for n in plan['needs'] if n['indicator'] == indicator), None)
        selected = selected or (plan['needs'][0] if plan['needs'] else node_plan(indicator)['needs'][0])
        selected.update(indicator=indicator, reason='선택한 개념 · 기존 검색 조건 유지')
        plan.pop('selected_concept',None)
        plan.update(needs=[selected], question='', followup=False, clear=[], workflow_ids=[], need_selection='explicit',semantic_query='')
        if countries is not None:
            plan['countries'] = country_list(countries)
            selected['scope'] = [s for s in selected.get('scope',[]) if s['field'] != 'countries']
        return validate_plan(plan)
    countries = [] if countries is None else country_list(countries)
    plan = {key: [] if spec['type'] == 'array' else False if spec['type'] == 'boolean' else ''
            for key, spec in plan_schema()['properties'].items()}
    plan.update(needs=[{'indicator': indicator, 'reason': '선택한 개념의 제공 근거',
                        'subject': '', 'frequency': '', 'formats': [], 'fields': [],
                        'unit': '', 'measurement_basis': '', 'scope': [], 'clear': []}],
                countries=list(countries), years_mode='list', need_selection='explicit', related_mode='auto')
    return validate_plan(plan)

def intent_prompt(query,previous):
    if not isinstance(query,str) or not query.strip() or len(query.encode())>2048 or any(ord(c)<32 and c not in '\n\r\t' for c in query):raise ValueError('invalid query')
    if previous is not None:validate_plan(previous)
    check_prompt(query)
    if previous is not None:check_prompt(json.dumps(previous,ensure_ascii=False))
    value={'today':date.today().isoformat(),'query':query,'previous':previous}
    if previous and previous.get('selected_concept'):
        node=selected_concept(previous['selected_concept'])
        value['selection']={k:node[k] for k in ('id','label','definition','legacy_id')}
    prompt=json.dumps(value,ensure_ascii=False,separators=(',',':'))
    if len(prompt.encode())>8192:raise ValueError('serialized intent too large')
    return prompt

def merge_plan(previous,current,*,query=None):
    validate_plan(current)
    action = _explicit_relaxation(previous, query, current.get('semantic_query',''))
    if action is not None:
        return action
    keep_selection=previous and previous.get('selected_concept') and current.get('followup') and current.get('selection_action')!='replace' and 'needs' not in current.get('clear',[])
    if keep_selection:
        current=selection_plan({**current,'selected_concept':previous['selected_concept']},previous['selected_concept'])
    result=copy.deepcopy(validate_plan(current))
    # A fresh, exact UI vocabulary label identifies one declared concept. The
    # model still interprets every question; it cannot replace this explicit
    # identifier with a neighboring category. Free text/followups stay modeled.
    exact=next((i for i,label in INDICATORS.items() if i!='other' and query is not None and query.strip()==label),None)
    if exact and previous is None:
        result.update(needs=[{'indicator':exact,'reason':INDICATORS[exact]+' 자료 탐색','subject':'','frequency':'',
                              'formats':[],'fields':[],'unit':'','measurement_basis':'','scope':[],'clear':[]}],
                      need_selection='explicit',workflow_ids=[],question='',followup=False)
    result.setdefault('workflow_ids',[])
    result.setdefault('additional_requirements',[])
    result.setdefault('related_exclusions',[])
    if result.get('related_mode','auto')=='auto':result['related_mode']=(previous or {}).get('related_mode','include') if result['followup'] else 'include'
    if result['related_mode']=='auto':result['related_mode']='include'
    result.setdefault('need_selection','explicit' if result['needs'] else 'workflow' if result['workflow_ids'] else 'explicit')
    for need in result['needs']:
        need.setdefault('unit','');need.setdefault('measurement_basis','')
        # A count's conventional unit must not become a user restriction. This
        # narrow grounding guard removes an absent unit, while preserving a
        # previously explicit unit in an actual followup. Unit aliases written
        # by the user remain supported; this is not a full language parser.
        if query is not None and need['unit']=='명':
            # Only the reproduced invented head-count unit is guarded here.
            # Other translated/scientific unit names cannot be rejected merely
            # because their normalized spelling differs from the user's words.
            text=query.casefold()
            if not any(token in text for token in ('명','person','people','단위','unit')):
                old=next((n for n in (previous or {}).get('needs',[]) if n['indicator']==need['indicator']),{})
                need['unit']=old.get('unit','') if result['followup'] else ''
    for field in result['clear']:
        if field in ('subjects','needs'):continue
        result[field]=[] if isinstance(result[field],list) else False if isinstance(result[field],bool) else ''
    if previous and result['followup']:
        validate_plan(previous)
        if not current['needs'] and 'needs' not in current['clear']:
            result['need_selection']=previous.get('need_selection','explicit')
        if not result['years'] and 'years' not in result['clear']:result['years_mode']=previous['years_mode']
        for field in (*FILTERS,'purpose'):
            if not result[field] and field not in result['clear']:result[field]=copy.deepcopy(previous.get(field,result[field]))
        if 'additional_requirements' not in result['clear']:
            result['additional_requirements']=list(dict.fromkeys(
                [*previous.get('additional_requirements',[]),*result['additional_requirements']]))
        old_needs={n['indicator']:n for n in previous['needs']}
        # A model may emit both a replacement and a removal. Never silently
        # broaden that constraint; retain the old value until one clarification.
        labels={'subject':'대상 집단','frequency':'시간 단위','formats':'데이터 형식','fields':'필요 컬럼','unit':'측정 단위','measurement_basis':'측정 방식'}
        for need in result['needs']:
            need.setdefault('unit','');need.setdefault('measurement_basis','')
            for field in list(need['clear']):
                if need[field]:
                    proposed=' / '.join(need[field]) if isinstance(need[field],list) else need[field]
                    result['question']=f"{INDICATORS[need['indicator']]} 자료의 {labels[field]}을 {proposed}로 바꿀까요, 제한을 없앨까요?"[:120]
                    need['clear'].remove(field)
                    need[field]=copy.deepcopy(old_needs.get(need['indicator'],{}).get(field,[] if isinstance(need[field],list) else ''))
        if 'needs' not in result['clear'] and result.get('selection_action')!='replace':
            updated=copy.deepcopy(old_needs)
            updated.update({n['indicator']:n for n in result['needs']})
            result['needs']=list(updated.values())
        for need in result['needs']:
            need.setdefault('unit','');need.setdefault('measurement_basis','')
            old=old_needs.get(need['indicator'],{})
            if not need['subject'] and 'subjects' not in result['clear'] and 'subject' not in need['clear']:need['subject']=old.get('subject','')
            for field in ('frequency','formats','fields','unit','measurement_basis'):
                if field in result['clear'] or field in need['clear'] or current.get(field):need[field]=[] if isinstance(need[field],list) else ''
                elif not need[field] and field in old:need[field]=copy.deepcopy(old[field])
        if 'needs' in result['clear'] and not result['needs']:result['workflow_ids']=[]
    if 'subjects' in result['clear']:
        for need in result['needs']:need['subject']=''
    # A followup changes only the supplied dimensions of the supplied needs.
    # Shared replacements/clears apply across indicators, except where this
    # same turn supplies an explicit per-indicator override.
    supplied={n['indicator']:n for n in current['needs']}
    prior={n['indicator']:n for n in (previous or {}).get('needs',[])}
    for need in result['needs']:
        incoming={s['field']:copy.deepcopy(s) for s in supplied.get(need['indicator'],{}).get('scope',[])}
        merged={s['field']:copy.deepcopy(s) for s in prior.get(need['indicator'],{}).get('scope',[])} if previous and result['followup'] else {}
        if 'needs' in result['clear']:merged={}
        for field in SCOPE_FIELDS:
            if field in current['clear']:merged.pop(field,None)
            elif field=='additional_requirements' and current.get(field) and field in merged:
                merged[field]['values']=list(dict.fromkeys([*merged[field]['values'],*current[field]]))
            elif current.get(field) and field not in incoming:merged.pop(field,None)
        for field,entry in incoming.items():
            if field=='additional_requirements' and entry['values'] and previous and result['followup']:
                inherited=merged[field]['values'] if field in merged else result.get(field,[])
                entry['values']=list(dict.fromkeys([*inherited,*entry['values']]))
            merged[field]=entry
        if merged:need['scope']=list(merged.values())
        elif 'scope' in need:need['scope']=[]
    for field in ('frequency','formats','fields'):
        if result[field] and any(field in n['clear'] for n in result['needs']):
            for need in result['needs']:
                if field not in need['clear'] and not need[field]:need[field]=copy.deepcopy(result[field])
            result[field]=[] if isinstance(result[field],list) else ''
    for need in result['needs']:
        for field in need['clear']:need[field]=[] if isinstance(need[field],list) else ''
        need['clear']=[]
    # Global off is a toggle, not an instruction to blacklist every neighbor.
    # Preserve earlier selective exclusions so off/on cannot revive one, but
    # do not persist the model's redundant enumeration during the off turn.
    if result['related_mode']=='exclude':
        result['related_exclusions']=copy.deepcopy((previous or {}).get('related_exclusions',[])) if result['followup'] else []
        if 'related_exclusions' in current['clear']:result['related_exclusions']=[]
    result['clear']=[]
    result['years']=sorted(set(result['years']))
    # A new topic ends the selected-node context. A followup keeps it even when
    # the legacy routing hint is `other`; parent source evidence never leaks in.
    if keep_selection:
        result['selected_concept']=previous['selected_concept']
        result=selection_plan(result,previous['selected_concept'])
    else:result.pop('selected_concept',None)
    _preserve_simple_region(result,previous,query)
    # Preserve a previously normalized subject for filter-only follow-ups. A
    # changed/cleared subject or selected node must not revive stale semantics.
    def semantic_identity(plan):
        return (plan.get('selected_concept',''), plan.get('purpose',''),
                [(n['indicator'],n.get('subject',''),n.get('measurement_basis','')) for n in plan['needs']])
    if (previous and result['followup'] and not result.get('semantic_query')
            and semantic_identity(previous)==semantic_identity(result)):
        result['semantic_query']=previous.get('semantic_query','')
    return validate_plan(result)

def planning_instructions(sources):
    from .ontology_definitions import workflow_catalog
    instruction=Path(__file__).with_name('intent_instructions.txt').read_text(encoding='utf-8')
    return (instruction+'\n상대 날짜의 기준인 오늘 날짜는 입력 JSON의 today 필드다.\n'+
            '\n지표 사전: '+json.dumps(INDICATORS,ensure_ascii=False)+
            '\n업무 사전(데이터 종류를 말하지 않은 목적에 사용): '+
            json.dumps({k:v['label'] for k,v in workflow_catalog().items()},ensure_ascii=False)+
            '\n등록 사이트: '+json.dumps({s['id']:s['name'] for s in sources},ensure_ascii=False))


def related_search_plan(plan,source_need,indicator,reason):
    """A separate related search retains portable constraints, not source units."""
    criteria=effective_plan(plan,source_need)
    target=copy.deepcopy(plan)
    target.pop('selected_concept',None)
    for field in SCOPE_FIELDS:
        target[field]=copy.deepcopy(criteria.get(field,[] if field!='geography_level' else ''))
    target['years_mode']=criteria['years_mode']
    target['frequency']=criteria['frequency'];target['formats']=list(criteria['formats']);target['fields']=[]
    target.update(purpose=INDICATORS[indicator]+' 자료 탐색',needs=[{
        'indicator':indicator,'reason':reason[:90],'subject':'','frequency':'','formats':[],
        'fields':[],'unit':'','measurement_basis':source_need.get('measurement_basis',''), 'scope':[],'clear':[]}],
        workflow_ids=[],need_selection='explicit',question='',followup=False,clear=[])
    return validate_plan(target)


def related_source_conditions(plan,source_need):
    omitted=[]
    for key,label in [('unit','단위'),('subject','대상 집단'),('fields','컬럼')]:
        value=source_need.get(key) or (plan.get(key) if key=='fields' else None)
        if value:omitted.append({'field':key,'label':label,'value':', '.join(value) if isinstance(value,list) else value})
    return omitted

class SiteRecommendations:
    def __init__(self,sources,evidence=None,*,catalog=None):
        self.sources={s['id']:s for s in sources}
        self.catalog=catalog
        data=evidence if evidence is not None else json.loads(Path(__file__).with_name('site_evidence.json').read_text(encoding='utf-8'))
        self.claims=data['claims']
        self.region_countries={}
        if len(self.claims)>256:raise ValueError('unbounded evidence')
        for c in self.claims:
            if c['source_id'] not in self.sources or not c.get('evidence_url') or not c.get('checked_on'):raise ValueError('unregistered or unsupported evidence')
            for region in c.get('regions',[]):self.region_countries.setdefault(region,set()).update(c.get('countries',[]))

    @staticmethod
    def missing(plan,c):
        gaps=[]
        if plan['countries'] and not set(plan['countries'])&set(c.get('countries',[])):gaps.append('대상 국가')
        # Nationwide support is not proof of every granular place or field.
        if plan['regions'] and not set(plan['regions'])<=set(c.get('regions',[])):gaps.append('세부 지역')
        requested_years=set(range(min(plan['years']),max(plan['years'])+1)) if plan['years'] and plan['years_mode']=='range' else set(plan['years'])
        if not requested_years<=set(c.get('years',[])):gaps.append('기간')
        if plan['dates'] and (not c.get('dates') or min(plan['dates'])<min(c['dates']) or max(plan['dates'])>max(c['dates'])):gaps.append('기간')
        if plan['frequency'] and plan['frequency'] not in c.get('frequencies',[]):gaps.append('시간 단위')
        level=plan['geography_level'];documented_levels=set(c.get('geography_levels',[]))
        # A generic administrative-unit request accepts a documented member
        # level, without making those boundary levels equivalent or joinable.
        administrative={'시도','시군구','읍면동','행정동','법정동','집계구'}
        if level and not (bool(documented_levels & administrative) if level=='행정구역' else level in documented_levels):gaps.append('지역 단위')
        if plan['formats'] and not set(plan['formats'])&set(c.get('formats',[])):gaps.append('데이터 형식')
        if plan['delivery'] and plan['delivery'] not in c.get('delivery',[]):gaps.append('제공 방식')
        if plan['fields'] and not set(plan['fields'])<=set(c.get('fields',[])):gaps.append('필요 컬럼')
        if plan['free_only'] and c.get('free') is not True:gaps.append('무료 이용')
        if plan['commercial_only'] and c.get('commercial') is not True:gaps.append('상업적 이용')
        if plan.get('unit') and plan['unit']!=c.get('observed_unit'):gaps.append('측정 단위')
        if plan.get('measurement_basis') and plan['measurement_basis']!=c.get('measurement_basis'):gaps.append('관측·추정·예보 구분')
        gaps.extend('추가 조건: '+item for item in plan.get('additional_requirements',[]))
        return gaps

    def normalize(self,plan):
        plan=copy.deepcopy(validate_plan(plan))
        region_names=REGION_NAMES
        plan['regions']=[region_names.get(r,r) for r in plan['regions']]
        periods={'시간별':'시간','일별':'일','월별':'월','연간':'년','연도별':'년','학기별':'학기'}
        plan['frequency']=periods.get(plan['frequency'],plan['frequency'])
        # Repeating the entity name is not a narrower subgroup. Preserve actual
        # subtypes (e.g. 종합병원, 소아청소년과) for evidence checks.
        for need in plan['needs']:
            for entry in need.get('scope',[]):
                if entry['field']=='regions':entry['values']=[region_names.get(r,r) for r in entry['values']]
            criteria=effective_plan(plan,need)
            # Infer coverage for this need only. A scoped region must not set
            # the country of unrelated needs in a multi-country question.
            scope=need.get('scope',[])
            if (not criteria['countries'] and criteria['regions'] and
                    any(entry['field']=='regions' for entry in scope) and
                    not any(entry['field']=='countries' for entry in scope)):
                coverage=[self.region_countries.get(region,set()) for region in criteria['regions']]
                if all(len(countries)==1 for countries in coverage):
                    need.setdefault('scope',[]).append({'field':'countries','values':sorted(set.union(*coverage)),'mode':'list'})
                    criteria=effective_plan(plan,need)
            need['frequency']=periods.get(need['frequency'],need['frequency'])
            if need['indicator']=='hospital_locations' and need['subject'] in ('병원','의료기관'):
                need['subject']=''
            if need['subject'] in {'population':('전체 인구',),'households':('전체 가구',),
                    'housing':('전체 주택',),'business_count':('전체 업종','모든 업종'),
                    'boundaries':('행정구역','행정구역 경계'),
                    'facilities':('생활편의시설',)}.get(need['indicator'],()):
                need['subject']=''
            # A country repeated verbatim as the subject of country-level life
            # expectancy is already checked by the coverage filter. Qualifiers
            # such as nationality, sex or age remain distinct subgroup filters.
            if need['indicator']=='life_expectancy' and len(criteria['countries'])==1 and need['subject'] in criteria['countries']:
                need['subject']=''
            # Weather subjects sometimes repeat an already selected area and
            # the basic measure ("한국 기온"). This adds no subtype restriction.
            # Only exact whole phrases are canonicalized; stations, altitude,
            # water temperature, forecasts and narrower areas are preserved.
            weather_names={'temperature':('기온','대기 온도'),
                           'precipitation':('강수량','강수')}.get(need['indicator'])
            if weather_names:
                # In a multi-area request, a subject may qualify just one
                # indicator (Korean temperature versus Japanese rainfall).
                # Retain that distinction until per-indicator scope is modeled.
                areas=(set(criteria['countries']) | set(criteria['regions'])) if len(criteria['countries'])<=1 and len(criteria['regions'])<=1 else set()
                if '대한민국' in areas:areas.add('한국')
                redundant=set(weather_names) | areas
                redundant.update(area+join+name for area in areas for join in (' ','의 ') for name in weather_names)
                if need['subject'] in redundant:need['subject']=''
        # An explicitly selected, verified region determines data coverage;
        # the language of the query and provider nationality never do.
        if not plan['countries'] and plan['regions']:
            coverage=[self.region_countries.get(region,set()) for region in plan['regions']]
            if all(len(countries)==1 for countries in coverage):plan['countries']=sorted(set.union(*coverage))
        return plan

    async def query_graph(self,session,plan):
        return self.compose_graph(*await self.traverse_graph(session,plan))

    async def traverse_graph(self,session,plan):
        from .ontology import DiscoveryOntology
        if not hasattr(self,'ontology'):self.ontology=DiscoveryOntology(self.sources,self.claims,catalog=self.catalog)
        return await self.ontology.query(session,self.normalize(plan))

    def compose_graph(self,expanded,candidates,trace):
        result=self.recommend(expanded,candidates=candidates)
        result['related_groups']=[]
        for related in trace['related_indicators']:
            source_need=next(n for n in expanded['needs'] if n['indicator']==related['source_indicator'])
            context=related_search_plan(expanded,source_need,related['indicator'],related['reason'])
            group=self.recommend(context,candidates=candidates)['groups'][0]
            group.update(source_indicator=related['source_indicator'],relation=related['relation'],
                         reason=related['reason'],references=related['references'],context=context,
                         source_only_conditions=related_source_conditions(expanded,source_need))
            result['related_groups'].append(group)
        result['related_total']=len({s['source_id'] for g in result['related_groups'] for s in g['sites']})
        graph_nodes={n['id']:n for n in trace['nodes']}
        for group in [*result['groups'],*result['related_groups']]:
            group['catalog_domains']=[{'node_id':edge['target'],'name':graph_nodes[edge['target']]['label'],
                 **graph_nodes[edge['target']]['properties']}
                for edge in trace['edges'] if edge['source']=='indicator:'+group['indicator'] and edge['relation']=='in_catalog_domain']
        accepted={(g['indicator'],s['source_id'],e['ontology_claim_id']) for g in [*result['groups'],*result['related_groups']] for s in g['sites'] for e in s['evidence']}
        trace['paths']=[p for p in trace['paths'] if (p['indicator'],p['source_id'],p['claim_id']) in accepted]
        trace['summary']=f"개념 {trace['usage']['nodes']}개와 관계 {trace['usage']['edges']}개를 탐색해 추천 경로 {len(trace['paths'])}개를 확인했어요."
        if trace['truncated']:
            result['notice']+=' 그래프 탐색 한도에 도달해 확인하지 못한 경로가 있어요.'
        result['ontology']=trace
        return result

    def recommend(self,plan,*,candidates=None):
        plan=self.normalize(plan)
        groups=[]
        for need in plan['needs'] if not plan['question'] else []:
            criteria=effective_plan(plan,need)
            reachable=self.claims if candidates is None else candidates.get(need['indicator'],[])
            choices=[c for c in reachable if need['indicator'] in c['indicators'] and
                (not plan['source_ids'] or c['source_id'] in plan['source_ids'])]
            matches={};failed=[]
            for c in choices:
                gaps=self.missing(criteria,c)
                if need['subject'] and need['subject'] not in c.get('subjects',[]):gaps.append('대상 업종·집단')
                if gaps:failed.append(gaps);continue
                sid=c['source_id'];source=self.sources[sid]
                card=matches.setdefault(sid,{'source_id':sid,'name':source['name'],'url':source['url'],'evidence':[]})
                card['evidence'].append({k:c.get(k) for k in ['title','summary','evidence_url','checked_on','countries','regions','years','formats','delivery','fields','access_note','observed_unit','measurement_basis','ontology_claim_id','ontology_resource_id','ontology_evidence_id']})
            unverified=[] if matches else list(dict.fromkeys(g for gaps in sorted(failed,key=len)[:1] for g in gaps))
            if matches:
                covered={country for site in matches.values() for fact in site['evidence'] for country in fact['countries']}
                missing_countries=[country for country in criteria['countries'] if country not in covered]
                if missing_countries:unverified=['대상 국가: '+', '.join(missing_countries)]
            if not matches and not unverified:unverified=['요청 자료의 제공 근거']
            groups.append({'indicator':need['indicator'],'name':INDICATORS[need['indicator']],
                'reason':need['reason'],'sites':list(matches.values())[:MAX_SITE_RESULTS],'unverified':unverified})
        all_sites={s['source_id'] for g in groups for s in g['sites']}
        state='clarify' if plan['question'] else 'results' if groups and all(g['sites'] and not g['unverified'] for g in groups) else 'partial' if all_sites else 'unverified'
        # Only propose one filter. No changes are applied without another user turn.
        gaps=[(g,x) for g in groups for x in g['unverified']]
        relax_map={'세부 지역':('regions','세부 지역 조건을 빼고 다시 찾기'),'기간':('period','기간 조건을 빼고 다시 찾기'),
            '시간 단위':('frequency','시간 단위를 지정하지 않고 다시 찾기'),'지역 단위':('geography_level','지역 단위를 지정하지 않고 다시 찾기'),
            '데이터 형식':('formats','파일 형식을 지정하지 않고 다시 찾기'),'제공 방식':('delivery','제공 방식을 지정하지 않고 다시 찾기'),
            '무료 이용':('free_only','무료 조건을 빼고 다시 찾기'),'상업적 이용':('commercial_only','상업적 이용 조건을 빼고 다시 찾기')}
        relaxation=None
        for group,gap in gaps:
            if gap in relax_map:
                field,label=relax_map[gap]
                scoped=any(n.get('scope') for n in plan['needs'])
                if scoped:label=group['name']+' 자료만 '+label
                relaxation={'field':field,'label':label,
                    'query':label+'. 방금 말한 자료의 조건만 해제하고 다른 자료를 포함한 그 밖의 모든 조건과 데이터 종류는 유지해 주세요.' if scoped else label+'. 방금 말한 조건만 해제하고 그 밖의 모든 조건과 데이터 종류는 유지해 주세요.'};break
        return {'version':2,'state':state,'question':plan['question'],'plan':plan,'groups':groups,
          'evidence_scope':{'sites':len({c['source_id'] for c in self.claims}),'registered_sites':len(self.sources)},
          'relaxation':relaxation,'total':len(all_sites),'datasets':[], 'concept_id':'site_recommendations',
          'scope':{'countries':requested_countries(plan),'basis':'dataset_coverage','unavailable_countries':[]},
            'notice':'직접 요청한 자료의 조건을 모두 충족하는 근거를 확인하지 못했어요. 자료가 존재하지 않는다는 뜻은 아니에요.' if state in {'unverified','partial'} else ''}

def validate_site_result(value):
    if not isinstance(value,dict) or value.get('version')!=2:raise ValueError('invalid site response')
    similarity_ranked = (isinstance(value.get('retrieval'),dict) and
        isinstance(value['retrieval'].get('selection'),dict) and
        value['retrieval']['selection'].get('method') in {'cosine_ranked','relevance_then_cosine','hybrid_rrf','relevance_then_hybrid_rrf'})
    relevance_ranked = similarity_ranked and value['retrieval']['selection']['method'] in {'relevance_then_cosine','relevance_then_hybrid_rrf'}
    plan=validate_plan(value.get('plan'))
    groups=value.get('groups');scope=value.get('scope')
    related=value.get('related_groups',[])
    if not isinstance(related,list) or len(related)>4:raise ValueError('invalid related groups')
    related_ids=set();direct_ids={n['indicator'] for n in plan['needs']}
    from .ontology_schema import RELATED_RELATIONS
    for group in related:
        if not isinstance(group,dict) or group.get('indicator') not in INDICATORS or group.get('source_indicator') not in direct_ids or group.get('indicator') in direct_ids|related_ids or group.get('relation') not in RELATED_RELATIONS:raise ValueError('invalid related concept')
        related_ids.add(group.get('indicator'))
        if not isinstance(group.get('reason'),str):raise ValueError('invalid related reason')
        source=next(n for n in plan['needs'] if n['indicator']==group['source_indicator'])
        if group.get('context')!=related_search_plan(plan,source,group['indicator'],group['reason']):raise ValueError('related conditions changed')
        if group.get('source_only_conditions')!=related_source_conditions(plan,source):raise ValueError('related condition disclosure changed')
        if group['indicator'] in plan.get('related_exclusions',[]) or plan.get('related_mode')=='exclude':raise ValueError('excluded related concept')
    if related and (plan['question'] or plan.get('need_selection')=='workflow'):raise ValueError('unexpected related expansion')
    if value.get('state') not in {'clarify','results','partial','unverified'} or not isinstance(groups,list) or len(groups)>5:raise ValueError('invalid groups')
    if not isinstance(scope,dict) or scope.get('basis')!=('requested_conditions' if similarity_ranked else 'dataset_coverage') or scope.get('countries')!=requested_countries(plan):raise ValueError('invalid coverage')
    def url(v):
        if not isinstance(v,str) or len(v)>4096:raise ValueError('invalid url')
        u=urlsplit(v)
        if u.scheme not in {'https','http'} or not u.hostname or u.username or u.password:raise ValueError('invalid url')
    unique=set();related_unique=set()
    for index,group in enumerate([*groups,*related]):
        if not isinstance(group,dict) or group.get('indicator') not in INDICATORS or not isinstance(group.get('sites'),list) or len(group['sites'])>MAX_SITE_RESULTS:raise ValueError('invalid group')
        for key in ('name','reason'):
            if not isinstance(group.get(key),str) or len(group[key])>300:raise ValueError('invalid group text')
        if index>=len(groups):
            refs=group.get('references');omitted=group.get('source_only_conditions')
            if not isinstance(refs,list) or len(refs)>4:raise ValueError('invalid related references')
            for ref in refs:url(ref)
            if not isinstance(omitted,list) or len(omitted)>3 or any(not isinstance(item,dict) or item.get('field') not in {'unit','subject','fields'} or any(not isinstance(item.get(k),str) or len(item[k])>400 for k in ('label','value')) for item in omitted):raise ValueError('invalid related condition note')
        if not isinstance(group.get('unverified'),list) or any(not isinstance(g,str) for g in group['unverified']):raise ValueError('invalid gaps')
        domains=group.get('catalog_domains',[])
        if not isinstance(domains,list) or len(domains)>3:raise ValueError('invalid catalog domains')
        for domain in domains:
            if not isinstance(domain,dict) or not isinstance(domain.get('catalog_id'),str) or domain.get('browse_path')!='/ontology?domain='+domain['catalog_id']:raise ValueError('invalid domain link')
        for site in group['sites']:
            if not isinstance(site,dict) or not isinstance(site.get('source_id'),str) or not isinstance(site.get('name'),str):raise ValueError('invalid site')
            url(site.get('url'));(unique if index<len(groups) else related_unique).add(site['source_id'])
            if not isinstance(site.get('evidence'),list) or not 1<=len(site['evidence'])<=32:raise ValueError('invalid evidence')
            for evidence in site['evidence']:
                if not isinstance(evidence,dict):raise ValueError('invalid evidence')
                url(evidence.get('evidence_url'))
                for field in ('title','summary','checked_on','access_note'):
                    if not isinstance(evidence.get(field),str):raise ValueError('invalid evidence text')
                if not isinstance(evidence.get('formats'),list) or any(f not in FORMATS for f in evidence['formats']):raise ValueError('invalid formats')
    if type(value.get('total')) is not int or value['total']!=len(unique) or value.get('datasets')!=[]:raise ValueError('invalid totals')
    if relevance_ranked:
        facts = [e for group in groups for site in group['sites'] for e in site['evidence']]
        if len(facts) > MAX_SITE_RESULTS or len({f.get('dataset_id') for f in facts}) != len(facts):
            raise ValueError('Invalid relevance result count')
        for fact in facts:
            if (fact.get('relevance_tier') not in {'direct', 'related'} or type(fact.get('relevance_need')) is not int or
                    not 1 <= fact['relevance_need'] <= len(plan['needs'])):
                raise ValueError('Invalid relevance tier or need')
            exclusions = fact.get('relevance_exclusions', [])
            if not isinstance(exclusions,list) or any(not isinstance(key,str) or key not in plan.get('related_exclusions',[]) for key in exclusions):
                raise ValueError('Invalid relevance exclusions')
            if fact['relevance_tier'] == 'related' and (plan.get('related_mode') == 'exclude' or exclusions):
                raise ValueError('Excluded related data')
        selection = value['retrieval']['selection']
        for tier in ('direct', 'related'):
            if selection.get(tier+'_datasets') != sum(f['relevance_tier'] == tier for f in facts):
                raise ValueError('Invalid relevance tier count')
    if 'related_total' in value and (type(value['related_total']) is not int or value['related_total']!=len(related_unique)):raise ValueError('invalid related total')
    # A query-level similarity list does not assert fulfillment of each
    # requested indicator. Its plan and explicit filters remain unchanged.
    expected_groups=[] if plan['question'] else ['other'] if similarity_ranked else [n['indicator'] for n in plan['needs']]
    if [g['indicator'] for g in groups]!=expected_groups:raise ValueError('groups do not match needs')
    expected_state='clarify' if plan['question'] else 'results' if groups and all(g['sites'] and not g['unverified'] for g in groups) else 'partial' if unique else 'unverified'
    if value['state']!=expected_state or value.get('question')!=plan['question']:raise ValueError('invalid outcome state')
    if not isinstance(value.get('notice'),str):raise ValueError('invalid notice')
    relaxation=value.get('relaxation')
    if relaxation is not None and (not isinstance(relaxation,dict) or any(not isinstance(relaxation.get(k),str) or len(relaxation[k])>512 for k in ('field','label','query'))):raise ValueError('invalid relaxation')
    if 'ontology' in value:
        from .ontology import validate_graph_response
        validate_graph_response(value['ontology'],value)
    return value
