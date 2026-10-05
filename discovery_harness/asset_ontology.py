"""Typed objects over every existing catalogue record; no source DB writes.

Metadata declarations, analyst interpretations and verified data offers are
different statements. A shared object is not proof that two datasets can join.
"""
import copy
import hashlib
import json
import re
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlsplit

from .ontology_definitions import definitions
from .ontology_schema import LINK_TYPES, SCHEMA_VERSION

OBJECT_TYPES = {
 'catalog_record':{'label':'등록 데이터셋','identity':'source_id와 카탈로그 id','meaning':'현재 DB의 실제 메타데이터 레코드. 원본 관측값·파일 자체와 구분한다.'},
 'provider':{'label':'등록 제공처','identity':'sources.id','meaning':'카탈로그 접근 사이트. 원자료 생산기관이나 자료 대상 국가를 추측하지 않는다.'},
 'publisher_declaration':{'label':'명시된 발행자','identity':'source_id와 publisher 원문','meaning':'제공처 메타데이터의 발행자 표기. 기관의 공식 식별자와 별도로 확인한다.'},
 'scope_declaration':{'label':'범위 표기','identity':'자료 id와 범위 종류','meaning':'지역·기간 원문을 보존한 선언. 같은 이름의 지역·기간을 자동 결합하지 않는다.'},
 'format_declaration':{'label':'형식 표기','identity':'정규화하지 않은 format 원문','meaning':'카탈로그에 명시된 형식. 실제 다운로드·응답 형식 검증과 구분한다.'},
 'access_reference':{'label':'접근 경로','identity':'공식 카탈로그에 기록된 URL','meaning':'메타데이터·소개·접근 경로. URL이 존재한다는 것은 호출 성공이나 이용 허가가 아니다.'},
 'domain':{'label':'카탈로그 분류','identity':'원본 concept_id','meaning':'원본 분류 주장과 방법·상태·근거를 보존한다. 세부 지표 제공 사실이 아니다.'},
 'indicator':{'label':'데이터 개념','identity':'분석가가 정의한 concept id','meaning':'측정 대상·의미·단위·시간·공간을 구분한 개념.'},
 'object_model':{'label':'업무 객체 유형','identity':'분석가가 정의한 object model id','meaning':'업무상 객체·속성·관계와 연결에 필요한 조건. 실제 원본 필드 확인 범위는 별도다.'},
 'semantic_assertion':{'label':'분석가 해석','identity':'자료 id와 분석가·해석 근거','meaning':'실제 메타데이터를 읽은 해석 또는 미해결 사유. 원본 행·값 검증이 아니다.'},
 'record_reference':{'label':'원본이 참조한 자료 ID','identity':'원본 duplicate_of 표기','meaning':'카탈로그가 명시한 중복 참조. 실제 관측값의 동등성은 별도 확인한다.'},
}

RELATIONS = {
 'registered_in':{'label':'등록 제공처','domain':['catalog_record'],'range':['provider'],'cardinality':'1','source_field':'source_id','meaning':'원본 자료의 등록 제공처 식별자.'},
 'declared_publisher':{'label':'발행자 표기','domain':['catalog_record'],'range':['publisher_declaration'],'cardinality':'0..1','source_field':'publisher','meaning':'기관 동일성이나 실제 생산 역할을 추가로 추론하지 않는다.'},
 'classified_under':{'label':'원본 분류 주장','domain':['catalog_record'],'range':['domain'],'cardinality':'0..*','source_field':'mappings','meaning':'분류의 status·method·evidence를 그대로 보존한다.'},
 'rejected_classification':{'label':'기각된 분류','domain':['catalog_record'],'range':['domain'],'cardinality':'0..*','source_field':'mappings','meaning':'기각된 분류는 활성 주제 관계로 사용하지 않는다.'},
 'declared_spatial_scope':{'label':'지역 표기','domain':['catalog_record'],'range':['scope_declaration'],'cardinality':'0..1','source_field':'region','meaning':'카탈로그의 지역 원문. 기관 소재지 또는 실제 관측 지역과의 동일성을 보장하지 않는다.'},
 'declared_temporal_scope':{'label':'기간 표기','domain':['catalog_record'],'range':['scope_declaration'],'cardinality':'0..1','source_field':'period/reference_years/year_basis','meaning':'기간 원문과 추출 근거를 보존한다. 갱신일을 관측 기준일로 대체하지 않는다.'},
 'declares_format':{'label':'형식 표기','domain':['catalog_record'],'range':['format_declaration'],'cardinality':'0..1','source_field':'format','meaning':'메타데이터의 형식 표기만 연결한다.'},
 'metadata_at':{'label':'메타데이터 원문','domain':['catalog_record'],'range':['access_reference'],'cardinality':'0..1','source_field':'metadata_url','meaning':'자료의 메타데이터 원문 경로.'},
 'access_via':{'label':'등록 접근 경로','domain':['catalog_record'],'range':['access_reference'],'cardinality':'0..*','source_field':'url/access_paths','meaning':'등록 경로. 실제 가용성·무료 여부·컬럼·조인 가능성은 별도 확인한다.'},
 'interpreted_as':{'label':'분석가의 해석','domain':['catalog_record'],'range':['semantic_assertion'],'cardinality':'0..*','source_field':'reviewed asset assertions','meaning':'현재 필드값에서 근거가 다시 확인되는 해석만 연결한다.'},
 'uses_object_model':{'label':'업무 객체 모델','domain':['semantic_assertion'],'range':['object_model'],'cardinality':'0..1','source_field':'object_model','meaning':'객체 유형의 정의와 현재 데이터 필드 확인 범위를 구분한다.'},
 'describes_concept':{'label':'메타데이터에서 확인한 개념','domain':['semantic_assertion'],'range':['indicator'],'cardinality':'0..*','source_field':'analyst indicators + evidence','meaning':'분석가가 원문에서 확인한 주제/측정 개념. 개별 데이터 값은 미검증이다.'},
 'business_context':{'label':'함께 분석할 업무 맥락','domain':['catalog_record'],'range':['catalog_record'],'cardinality':'0..*','source_field':'analyst dataset_relationships','meaning':'실제 두 자료를 읽어 정의한 업무 관계. 물리 조인·인과관계를 보장하지 않는다.'},
 'possible_comparison':{'label':'비교 검토 대상','domain':['catalog_record'],'range':['catalog_record'],'cardinality':'0..*','source_field':'analyst dataset_relationships','meaning':'비교 전 단위·범위·방법론·기준시점을 검증해야 한다.'},
 'catalogue_series':{'label':'자료 묶음 맥락','domain':['catalog_record'],'range':['catalog_record'],'cardinality':'0..*','source_field':'analyst dataset_relationships','meaning':'메타데이터에 드러난 조사·자료 묶음의 관계. 같은 관측·모집단이라고 단정하지 않는다.'},
 'duplicate_reference':{'label':'명시된 중복 참조','domain':['catalog_record'],'range':['record_reference'],'cardinality':'0..1','source_field':'duplicate_of','meaning':'기존 카탈로그의 중복 참조를 보존한다. 원본 값의 동일성이나 조인 가능성을 새로 증명하지 않는다.'},
}


def identity(kind,value):
 return kind+':'+hashlib.sha256(json.dumps(value,ensure_ascii=False,sort_keys=True).encode()).hexdigest()[:24]


def safe_url(value):
 if not isinstance(value,str) or len(value)>4096 or any(ord(c)<32 for c in value):return None
 try:
  u=urlsplit(value)
  return value if u.scheme in {'http','https'} and u.hostname and not u.username and not u.password else None
 except ValueError:return None


def field_value(record,path,default=None):
 """Small JSON field path language, never executable expressions."""
 if not isinstance(path,str) or not re.fullmatch(r'[a-zA-Z_][a-zA-Z_0-9]*(?:(?:\.[a-zA-Z_][a-zA-Z_0-9]*)|(?:\[\d{1,3}\]))*',path):return default
 value=record
 try:
  for name,index in re.findall(r'([a-zA-Z_][a-zA-Z_0-9]*)|\[(\d+)\]',path):
   value=value[name] if name else value[int(index)]
  return value
 except (KeyError,IndexError,TypeError):return default


def evidence_matches(record,evidence):
 if not isinstance(evidence,list) or not evidence:return False
 missing=object()
 for item in evidence:
  if not isinstance(item,dict):return False
  actual=field_value(record,item.get('field'),missing);excerpt=item.get('value')
  if actual is missing:return False
  if item.get('comparison')=='equals':
   if json.dumps(actual,ensure_ascii=False,sort_keys=True)!=json.dumps(excerpt,ensure_ascii=False,sort_keys=True):return False
   continue
  if item.get('comparison','contains')!='contains':return False
  if actual is None or not isinstance(excerpt,str) or not excerpt.strip():return False
  rendered=actual if isinstance(actual,str) else json.dumps(actual,ensure_ascii=False)
  if excerpt not in rendered:return False
 return True


@lru_cache(maxsize=1)
def asset_models():
 path=Path(__file__).with_name('ontology_asset_models.json')
 if not path.exists():return {'version':1,'object_models':[],'asset_assertions':[],'dataset_relationships':[]}
 data=json.loads(path.read_text(encoding='utf-8'))
 if data.get('version')!=1 or not isinstance(data.get('object_models'),list) or len(data['object_models'])>64:raise ValueError('invalid asset ontology')
 if not isinstance(data.get('asset_assertions'),list) or len(data['asset_assertions'])>1200:raise ValueError('unbounded asset assertions')
 if not isinstance(data.get('dataset_relationships'),list) or len(data['dataset_relationships'])>128:raise ValueError('unbounded dataset relationships')
 return validate_asset_models(data)


def validate_asset_models(data):
 ids=[m['id'] for m in data['object_models']]
 if len(ids)!=len(set(ids)):raise ValueError('duplicate object model')
 targets=data.get('target_type_definitions',[])
 target_ids=[t['id'] for t in targets]
 if len(targets)>128 or len(set(ids+target_ids))!=len(ids+target_ids):raise ValueError('duplicate or unbounded target type')
 types=set(ids+target_ids)
 for model in [*data['object_models'],*targets]:
  if not re.fullmatch('[a-z][a-z0-9_]{0,99}',model['id']) or not model.get('label') or not model.get('definition'):raise ValueError('invalid object type definition')
 for model in data['object_models']:
  names=[a['name'] for a in model['attributes']]
  if not names or len(names)!=len(set(names)):raise ValueError('invalid model attributes')
  for a in model['attributes']:
   if not a.get('meaning') or not a.get('data_type') or type(a.get('required_for_join')) is not bool:raise ValueError('invalid attribute definition')
  for r in model['relationships']:
   if r.get('target_type') not in types or not r.get('cardinality') or not r.get('meaning') or not r.get('limitations'):raise ValueError('invalid model relationship')
   if contract:=r.get('cardinality_contract'):
    if contract.get('enforcement')!='design_only' or contract.get('actual_join_verified') is not False or contract.get('target_type')!=r['target_type']:raise ValueError('unsafe cardinality enforcement')
    for direction in ['source_to_target','target_to_source']:
     bound=contract[direction]
     if type(bound.get('min')) is not int or bound['min']<0 or (bound.get('max') is not None and (type(bound['max']) is not int or bound['max']<bound['min'])):raise ValueError('invalid cardinality bounds')
  if not isinstance(model.get('source_mapping'),dict):raise ValueError('missing field mapping')
 concepts={c['id'] for c in definitions()['concepts']}
 for a in data['asset_assertions']:
  if not isinstance(a.get('dataset_id'),str) or a.get('status') not in {'reviewed_metadata','unresolved'}:raise ValueError('invalid asset interpretation')
  if a.get('object_model') is not None and a['object_model'] not in ids:raise ValueError('unknown object model')
  if not set(a.get('indicators',[]))<=concepts:raise ValueError('unknown interpreted concept')
  if a.get('join_keys')!=[]:raise ValueError('raw keys require a separate verification contract')
  if a['status']=='unresolved' and a.get('indicators'):raise ValueError('unresolved interpretation cannot assert concepts')
  if not a.get('evidence') or not a.get('limitations'):raise ValueError('missing interpretation evidence or limits')
  if not isinstance(a.get('granularity'),dict) or set(a['granularity'])!={'space','time','unit'}:raise ValueError('invalid granularity')
 assets={a['dataset_id'] for a in data['asset_assertions']}
 for r in data['dataset_relationships']:
  if r.get('predicate') not in {'business_context','possible_comparison','catalogue_series'} or r.get('join_status')!='not_verified':raise ValueError('unsupported dataset relationship')
  endpoints={r['source_dataset'],r['target_dataset']}
  if len(endpoints)!=2 or not endpoints<=assets or not endpoints<={e.get('dataset_id') for e in r.get('evidence',[])}:raise ValueError('dataset relation requires evidence at both ends')
  if not r.get('meaning') or not r.get('limitations') or not r.get('required_keys'):raise ValueError('missing relationship conditions')
 return data


def reviewed_records(rows,indicator):
 """Select only current, evidence-matching analyst interpretations of a concept."""
 if indicator not in {c['id'] for c in definitions()['concepts']}:raise ValueError('unknown indicator')
 assertions={}
 for a in asset_models()['asset_assertions']:
  if a['status']=='reviewed_metadata' and indicator in a['indicators']:
   assertions.setdefault(a['dataset_id'],[]).append(a)
 return [r for r in rows if any(evidence_matches(r,a['evidence']) for a in assertions.get(r['id'],[]))]


def relationship_context_ids(identifiers):
 identifiers=set(identifiers);targets=[]
 for relation in asset_models()['dataset_relationships']:
  if relation['source_dataset'] in identifiers:
   targets.extend([relation['target_dataset'],*[e['dataset_id'] for e in relation.get('evidence',[]) if e.get('dataset_id')]])
 return list(dict.fromkeys(x for x in targets if x not in identifiers))[:64]


def scheme_snapshot(overview):
 vocabulary=definitions();models=asset_models()
 counts=overview.get('summary',{}).get('concept_counts',{})
 nodes=[{'id':'domain:'+d['id'],'kind':'domain','label':d['label'],'properties':{**d,'record_count':counts.get(d['id'])}} for d in vocabulary.get('catalog_domains',[])]
 nodes.extend({'id':'indicator:'+c['id'],'kind':'indicator','label':c['label'],'properties':copy.deepcopy(c)} for c in vocabulary['concepts'])
 edges=[{'source':'indicator:'+r['source'],'target':'indicator:'+r['target'],'relation':r['relation'],
          'label':LINK_TYPES[r['relation']]['label'],'reason':r['reason'],'references':r.get('references',[]),'basis':'curated_domain_rule'} for r in vocabulary.get('related_links',[])]
 edges.extend({'source':'indicator:'+b['indicator'],'target':'domain:'+domain,'relation':'in_catalog_domain','label':'탐색할 분야','reason':b['reason'],'references':[],'basis':'curated_domain_definition'} for b in vocabulary.get('catalog_bindings',[]) for domain in b['domains'])
 return {'version':SCHEMA_VERSION,'engine':'catalogue_ontology','nodes':nodes,'edges':edges,
         'object_types':copy.deepcopy(OBJECT_TYPES),'relation_types':copy.deepcopy(RELATIONS),
    'semantic_relation_types':copy.deepcopy(LINK_TYPES),'object_models':copy.deepcopy(models['object_models']),
         'target_type_definitions':copy.deepcopy(models.get('target_type_definitions',[])),
         'summary':{'records':overview.get('total',overview.get('summary',{}).get('datasets',0)),
                    'domains':len(vocabulary.get('catalog_domains',[])),'concepts':len(vocabulary['concepts']),
                    'related_relations':len(vocabulary.get('related_links',[])),'providers':len(overview.get('sources',[])),
                    'asset_interpretations':len(models['asset_assertions']),
                    'reviewed_metadata_interpretations':sum(a['status']=='reviewed_metadata' for a in models['asset_assertions']),
                    'unresolved_interpretations':sum(a['status']=='unresolved' for a in models['asset_assertions']),
                    'unique_interpreted_assets':len({a['dataset_id'] for a in models['asset_assertions']}),
                    'dataset_business_relations':len(models['dataset_relationships']),
                    'classification_counts':{k:overview.get('summary',{}).get(k) for k in ['approved','pending']}},
         'sources':[{k:s.get(k) for k in ['id','name','url','country','dataset_count','stored_count']} for s in overview.get('sources',[])],
         'scope':'전체 카탈로그에 적용되는 객체·관계 규칙과 검토된 개념 모델입니다. 모든 원자료 값이나 조인 가능성을 검증했다는 뜻은 아닙니다.'}


def record_projection(snapshot,*,max_nodes=192,max_edges=384):
 """The same field-to-object rules apply lazily to every existing dataset ID."""
 data=asset_models();vocabulary=definitions();concepts={c['id']:c for c in vocabulary['concepts']}
 sources={s['id']:s for s in snapshot['sources']};domains={d['id']:d for d in vocabulary.get('catalog_domains',[])}
 models={m['id']:m for m in data['object_models']};nodes={};edges=[];records=[];truncated=False
 def node(kind,key,label,properties):
  nonlocal truncated
  nid=key if kind in {'domain','indicator','object_model'} else identity(kind,key)
  if nid not in nodes:
   if len(nodes)>=max_nodes:truncated=True;return None
   nodes[nid]={'id':nid,'kind':kind,'label':str(label)[:300],'properties':properties}
  return nid
 def edge(source,target,relation,field,*,status='declared_metadata',evidence=None,properties=None):
  nonlocal truncated
  if not source or not target:return
  if len(edges)>=max_edges:truncated=True;return
  edges.append({'source':source,'target':target,'relation':relation,'label':RELATIONS[relation]['label'],
                'provenance':{'basis':status,'source_field':field,'evidence':evidence},'properties':properties or {}})
 visible={r['id']:r for r in snapshot['datasets']}
 # Admit every page record before optional properties use the view budget.
 for r in snapshot['datasets']:
  props={k:copy.deepcopy(r.get(k)) for k in ('id','source_id','native_id','title','publisher','region','period','reference_years','year_basis','format','checked_at')}
  props['source_fields_verified']='catalogue_metadata_only';props['raw_values_verified']=False
  nid=node('catalog_record',[r['source_id'],r['id']],r.get('title') or r['id'],props)
  records.append({'id':r['id'],'node_id':nid,'source_id':r['source_id'],'title':r.get('title'),
                  'mappings':copy.deepcopy(r.get('mappings',[])),'interpretations':[]})
 record_nodes={r['id']:r['node_id'] for r in records}
 for r,view in zip(snapshot['datasets'],records):
  rid=view['node_id'];sid=r['source_id'];source=sources.get(sid)
  if not source:raise ValueError('unregistered catalogue provider')
  provider=node('provider',sid,source['name'],{'source_id':sid,'url':safe_url(source.get('url')),'provider_country':source.get('country'),'country_is_dataset_coverage':False})
  edge(rid,provider,'registered_in','source_id',evidence=sid)
  if isinstance(r.get('duplicate_of'),str) and r['duplicate_of']:
   reference=node('record_reference',r['duplicate_of'],r['duplicate_of'],{'catalogue_id':r['duplicate_of'],'raw_equivalence_verified':False})
   edge(rid,reference,'duplicate_reference','duplicate_of',evidence=r['duplicate_of'])
  for field,kind,relation,label in [('publisher','publisher_declaration','declared_publisher','발행자'),('region','scope_declaration','declared_spatial_scope','지역'),('format','format_declaration','declares_format','형식')]:
   value=r.get(field)
   if isinstance(value,str) and value.strip():
    key=[sid,value] if field=='publisher' else value if field=='format' else [r['id'],field]
    target=node(kind,key,label+': '+value,{'value':value,'source_field':field,'verification':'metadata_declaration_only'})
    edge(rid,target,relation,field,evidence=value)
  if r.get('period') or r.get('reference_years'):
   target=node('scope_declaration',[r['id'],'period'],'기간: '+str(r.get('period') or '연도 표기'),
               {k:copy.deepcopy(r.get(k)) for k in ['period','reference_years','year_basis']})
   edge(rid,target,'declared_temporal_scope','period/reference_years/year_basis')
  links=[('metadata_url',r.get('metadata_url'),'metadata_at'),('url',r.get('url'),'access_via')]
  links.extend((f'access_paths[{i}]',u,'access_via') for i,u in enumerate(r.get('access_paths') or []) if i<8)
  for field,value,relation in links:
   if url:=safe_url(value):
    target=node('access_reference',url,'메타데이터 원문' if relation=='metadata_at' else '등록 접근 경로',{'url':url,'tested':False})
    edge(rid,target,relation,field,evidence=url)
  for i,mapping in enumerate(r.get('mappings',[])):
   domain=domains.get(mapping.get('concept_id'))
   if not domain:continue
   target=node('domain','domain:'+domain['id'],domain['label'],copy.deepcopy(domain))
   relation='rejected_classification' if mapping.get('status')=='rejected' else 'classified_under'
   edge(rid,target,relation,f'mappings[{i}]',status='catalogue_classification',evidence=mapping.get('evidence'),properties={'status':mapping.get('status'),'method':mapping.get('method'),'content_verified':False})
  for assertion in data['asset_assertions']:
   if assertion['dataset_id']!=r['id']:continue
   matches=evidence_matches(r,assertion.get('evidence'))
   view['interpretations'].append({**copy.deepcopy(assertion),'current_evidence_matches':matches})
   if not matches:continue
   aid=node('semantic_assertion',[r['id'],assertion],'분석가 해석' if assertion['status']=='reviewed_metadata' else '해석 보류',copy.deepcopy(assertion))
   edge(rid,aid,'interpreted_as','analyst asset assertion',status=assertion['status'],evidence=copy.deepcopy(assertion['evidence']))
   if assertion.get('object_model'):
    model=models[assertion['object_model']];mid=node('object_model','object_model:'+model['id'],model['label'],copy.deepcopy(model))
    edge(aid,mid,'uses_object_model','object_model',status='analyst_object_model')
   if assertion['status']=='reviewed_metadata':
    for iid in assertion.get('indicators',[]):
     concept=concepts[iid];cid=node('indicator','indicator:'+iid,concept['label'],copy.deepcopy(concept))
     edge(aid,cid,'describes_concept','indicators',status='analyst_metadata_interpretation')
 # Cross-dataset edges are qualified analyst statements, never generated from
 # co-occurrence or a shared publisher/country. Out-of-page targets are links.
 related=[];context={r['id']:r for r in [*snapshot.get('related_records',[]),*snapshot['datasets']]}
 for relation in data['dataset_relationships']:
  if relation['source_dataset'] not in visible:continue
  evidence=relation.get('evidence',[])
  if not evidence or not all(e.get('dataset_id') in context and evidence_matches(context[e['dataset_id']],[e]) for e in evidence):continue
  related.append(copy.deepcopy(relation))
  if relation['target_dataset'] in visible:
   edge(record_nodes[relation['source_dataset']],record_nodes[relation['target_dataset']],relation['predicate'],
        'analyst dataset relationship',status='analyst_business_context',evidence=relation.get('evidence'),properties=copy.deepcopy(relation))
 result={'version':1,'engine':'catalogue_object_graph','nodes':list(nodes.values()),'edges':edges,'records':records,
         'dataset_relationships':related,'truncated':truncated,'limits':{'nodes':max_nodes,'edges':max_edges,'datasets':32},
         'page':snapshot.get('page',1),'pages':snapshot.get('pages',1),'total':snapshot.get('total',len(records)),
         'object_types':copy.deepcopy(OBJECT_TYPES),'relation_types':copy.deepcopy(RELATIONS),
         'scope':'기존 데이터셋의 메타데이터를 객체·속성·관계로 표시합니다. 분류·발행자·기간 표기를 실제 측정·지역·조인 사실로 확대하지 않습니다.'}
 validate_record_projection(result)
 return result


def validate_record_projection(value):
 if value.get('engine')!='catalogue_object_graph':raise ValueError('invalid record ontology')
 nodes=value['nodes'];edges=value['edges'];ids={n['id']:n for n in nodes}
 if len(ids)!=len(nodes) or len(nodes)>192 or len(edges)>384 or len(value['records'])>32:raise ValueError('record graph budget')
 for n in nodes:
  if n['kind'] not in OBJECT_TYPES:raise ValueError('unknown object type')
 for e in edges:
  spec=RELATIONS.get(e['relation'])
  if not spec or e['source'] not in ids or e['target'] not in ids:raise ValueError('dangling record relation')
  if ids[e['source']]['kind'] not in spec['domain'] or ids[e['target']]['kind'] not in spec['range']:raise ValueError('record relation domain/range')
  if e['relation']=='classified_under' and e['properties'].get('status')=='rejected':raise ValueError('rejected classification is active')
  if e['relation'] in {'business_context','possible_comparison','catalogue_series'} and e['properties'].get('join_status')!='not_verified':raise ValueError('unverified join claimed')
 for record in value['records']:
  if record['node_id'] not in ids or ids[record['node_id']]['kind']!='catalog_record':raise ValueError('missing primary record')
  if not value['truncated'] and sum(e['source']==record['node_id'] and e['relation']=='registered_in' for e in edges)!=1:raise ValueError('record needs exactly one registered provider')
 return value
