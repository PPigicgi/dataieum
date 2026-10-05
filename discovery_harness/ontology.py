"""Typed, evidence-scoped knowledge graph used by the live recommendation tool."""
import copy
import hashlib
import json
from collections import defaultdict, deque
from urllib.parse import urlsplit, urlunsplit

from .graph import Edge, Node
from .ontology_definitions import CONCEPT_FIELDS, definitions, validate_definitions
from .ontology_schema import (OBJECT_TYPES, LINK_TYPES, INFERENCE_RULES, RELATED_RELATIONS,
                              COMPARABILITY_REQUIREMENTS, SCHEMA_VERSION)

def stable_id(kind, value):
    data=json.dumps(value,sort_keys=True,ensure_ascii=False,separators=(',',':')).encode()
    return kind+':'+hashlib.sha256(data).hexdigest()[:24]

def canonical_url(value):
    u=urlsplit(value)
    if u.scheme not in {'https','http'} or not u.hostname or u.username or u.password:raise ValueError('unsafe ontology URL')
    return urlunsplit((u.scheme.lower(),u.netloc.lower(),u.path,u.query,''))

def valid_property(value,kind):
    if kind=='text':return isinstance(value,str) and len(value)<=2000
    if kind=='nullable_text':return value is None or valid_property(value,'text')
    if kind=='bool':return type(value) is bool
    if kind=='nullable_bool':return value is None or type(value) is bool
    if kind=='nullable_integer':return value is None or (type(value) is int and 0<=value<=10**10)
    if kind=='texts':return isinstance(value,list) and len(value)<=128 and all(valid_property(v,'text') for v in value)
    if kind=='integers':return isinstance(value,list) and len(value)<=256 and all(type(v) is int for v in value)
    if kind=='url':
        if not isinstance(value,str) or len(value)>4096:return False
        try:canonical_url(value);return True
        except ValueError:return False
    if kind=='urls':return isinstance(value,list) and len(value)<=32 and all(valid_property(v,'url') for v in value)
    return False


class DiscoveryOntology:
    """Immutable after construction in production; IDs have stable provenance.

    Availability assertions retain their own filters. No query combines a CSV
    fact from one assertion with a year, place or column from another assertion.
    """
    def __init__(self,sources,claims,vocabulary=None,catalog=None):
        self.vocabulary=copy.deepcopy(validate_definitions(vocabulary) if vocabulary is not None else definitions())
        self.nodes={};self.links={};self.adjacency=defaultdict(list);self.claims={};self.workflows={}
        self.indicators={c['id']:c for c in self.vocabulary['concepts']}
        counts=(catalog or {}).get('summary',{}).get('concept_counts',{})
        for domain in self.vocabulary.get('catalog_domains',[]):
            self.add_node('domain:'+domain['id'],'domain',domain['label'],
                          {'catalog_id':domain['id'],'definition':domain['definition'],
                           'record_count':counts.get(domain['id']),
                           'count_basis':'카탈로그 분류 전체 수; 현재 질문의 국가·기간·지표 조건 미적용',
                           'browse_path':'/ontology?domain='+domain['id']},
                          {'basis':'catalog_metadata','reference':'catalogue_topic_vocabulary'})
        for c in self.vocabulary['concepts']:
            self.add_node('indicator:'+c['id'],'indicator',c['label'],{k:c[k] for k in CONCEPT_FIELDS},
                          {'basis':'curated_domain_definition','reference':'ontology_domains.json'})
        for binding in self.vocabulary.get('catalog_bindings',[]):
            for domain in binding['domains']:
                self.add_link('indicator:'+binding['indicator'],'domain:'+domain,'in_catalog_domain',binding['reason'])
        for c in self.vocabulary['concepts']:
            for other in c['not_equivalent_to']:
                self.add_link('indicator:'+c['id'],'indicator:'+other,'not_equivalent_to',
                              '측정 의미·대상·단위가 달라 자동 대체하지 않음')
        for relation in sorted(self.vocabulary.get('related_links',[]),key=lambda r:r.get('priority',1)):
            key=('indicator:'+relation['source'],'indicator:'+relation['target'],relation['relation'])
            self.add_link(*key,relation['reason'])
            self.links[key]['references']=list(relation.get('references',[]))
        for w in self.vocabulary['workflows']:
            self.workflows[w['id']]=w
            wid='purpose:'+w['id']
            self.add_node(wid,'purpose',w['label'],{'description':w['description'],'limitations':w['limitations']},
                          {'basis':'curated_domain_rule','reference':'ontology_domains.json'})
            for a in w['analyses']:
                aid='analysis:'+w['id']+':'+a['id']
                self.add_node(aid,'analysis',a['label'],{'reason':a['reason']},
                              {'basis':'curated_domain_rule','reference':'ontology_domains.json'})
                self.add_link(wid,aid,'requires_analysis',a['reason'])
                for n in a['indicators']:self.add_link(aid,'indicator:'+n['id'],'uses_indicator',n['reason'])
        for sid,s in sources.items():
            self.add_node('provider:'+sid,'provider',s['name'],
                          {'url':s['url'],'catalogue_country':s.get('country') or '', 'role':'registered_access_site'},
                          {'basis':'catalog_metadata','reference':'registered_sources'})
        for original in claims:
            c=copy.deepcopy(original);url=canonical_url(c['evidence_url'])
            claim_id=stable_id('assertion',c)
            # This is a document-anchored identity, not a claim that two raw
            # files/observations are equal. Shared evidence is deduplicated.
            # Production claims use manually assigned stable resource keys;
            # fixtures without one retain a distinct assertion-scoped identity.
            rid=stable_id('dataset',c.get('resource_key') or ['unresolved_resource',claim_id])
            eid=stable_id('evidence',[url,c['checked_on']])
            sample_verified=c.get('sample_verified') is True
            references=list(dict.fromkeys([url,*c.get('reference_urls',[])]))
            if rid in self.nodes:
                previous_refs=self.nodes[rid]['properties']['reference_urls']
                previous_refs.extend(ref for ref in references if ref not in previous_refs)
            else:
                self.add_node(rid,'dataset',c['title'],
                              {'identity_basis':'curated resource key; raw observation/file identity unverified',
                               'resource_key':c.get('resource_key') or claim_id,'reference_urls':references},
                              {'basis':'official_documentation','reference':'availability_assertions'})
            self.add_node(eid,'evidence','공식 제공 안내',
                          {'url':url,'checked_on':c['checked_on'],'level':'official_documentation',
                           'sample_verified':sample_verified,'independent_evidence_key':stable_id('document',url)},
                          {'basis':'official_documentation','reference':url})
            props={k:copy.deepcopy(c.get(k,[])) for k in ('countries','regions','years','dates','frequencies','geography_levels','formats','delivery','fields','subjects')}
            props.update(claim_id=claim_id,source_id=c['source_id'],resource_id=rid,
                         free=c.get('free'),commercial=c.get('commercial'),observed_unit=c.get('observed_unit'),
                         measurement_basis=c.get('measurement_basis'),sample_verified=sample_verified)
            self.add_node(claim_id,'assertion',c['title'],props,{'basis':'official_documentation','reference':url})
            c.update(ontology_claim_id=claim_id,ontology_resource_id=rid,ontology_evidence_id=eid)
            self.claims[claim_id]=c
            for indicator in c['indicators']:
                self.add_link(claim_id,'indicator:'+indicator,'documents_indicator',c['summary'])
                self.add_link('indicator:'+indicator,claim_id,'has_documented_offer',c['summary'],inverse=True)
            self.add_link(claim_id,rid,'describes_resource',c['title'])
            self.add_link(claim_id,'provider:'+c['source_id'],'available_at','등록 홈페이지에서 접근')
            self.add_link(claim_id,eid,'evidenced_by','공식 문서 및 명시 범위의 API 표본 확인' if sample_verified else '공식 문서 확인; 원본 표본 미검증')
            for level,values in [('country',c.get('countries',[])),('region',c.get('regions',[]))]:
                for area in values:
                    aid=stable_id('area',[level,area]);self.add_node(aid,'area',area,{'level':level,'boundary_version':None},
                        {'basis':'official_documentation','reference':'availability_assertions'})
                    self.add_link(claim_id,aid,'covers_area','자료 대상 지역; 기관 소재지와 구분')
            for fmt in c.get('formats',[]):
                fid='format:'+fmt;self.add_node(fid,'format',fmt,{'name':fmt},{'basis':'format_vocabulary','reference':'availability_assertions'})
                self.add_link(claim_id,fid,'supports_format','이 제공 주장의 확인 형식')
            for method in c.get('delivery',[]):
                mid='access:'+method;self.add_node(mid,'access','API' if method=='api' else '다운로드',{'name':method},
                    {'basis':'access_vocabulary','reference':'availability_assertions'})
                self.add_link(claim_id,mid,'accessed_via','형식·가격·인증과 별개인 접근 방식')
        self.validate()
        self.revision=hashlib.sha256(json.dumps({'nodes':self.nodes,'links':list(self.links.values())},
                            sort_keys=True,ensure_ascii=False).encode()).hexdigest()[:16]

    def add_node(self,nid,kind,label,properties,provenance):
        Node(nid,kind)
        if kind not in OBJECT_TYPES or not isinstance(label,str) or not label or len(label)>300:raise ValueError('invalid semantic node')
        schema=OBJECT_TYPES[kind]['properties']
        if set(properties)!=set(schema) or any(not valid_property(properties[k],v) for k,v in schema.items()):raise ValueError('invalid semantic properties: '+nid)
        value={'id':nid,'kind':kind,'label':label,'properties':properties,'provenance':provenance}
        if nid in self.nodes and self.nodes[nid]!=value:
            raise ValueError('conflicting object identity: '+nid)
        self.nodes[nid]=value

    def add_link(self,source,target,relation,reason,*,inverse=False):
        spec=LINK_TYPES[relation]
        if self.nodes[source]['kind']!=spec['domain'] or self.nodes[target]['kind']!=spec['range']:raise ValueError('link violates domain/range')
        key=(source,target,relation)
        if key in self.links:return
        canonical=(target,source,spec['inverse']) if inverse else key
        value={'id':stable_id('link',canonical),'source':source,'target':target,'relation':relation,
               'canonical_source':canonical[0],'canonical_target':canonical[1],'canonical_relation':canonical[2],
               'label':spec['label'],'reason':reason,'basis':spec['basis'],'derived_inverse':inverse}
        self.links[key]=value
        self.adjacency[source].append(Edge(source,Node(target,self.nodes[target]['kind']),relation))

    def validate(self):
        if len(self.nodes)>1024 or len(self.links)>4096:raise ValueError('ontology index exceeds static bounds')
        for node in self.nodes.values():
            for relation,spec in LINK_TYPES.items():
                if spec['domain']!=node['kind']:continue
                count=sum(e.relation==relation for e in self.adjacency[node['id']])
                if not spec['min_targets']<=count<=spec['max_targets']:raise ValueError('link cardinality: '+relation)
        for key,edge in self.links.items():
            if key!=(edge['source'],edge['target'],edge['relation']):raise ValueError('invalid edge identity')
            spec=LINK_TYPES[edge['relation']]
            if self.nodes[edge['source']]['kind']!=spec['domain'] or self.nodes[edge['target']]['kind']!=spec['range']:raise ValueError('invalid edge type')
            if edge['derived_inverse'] and (edge['canonical_source'],edge['canonical_target'],edge['canonical_relation']) not in self.links:
                raise ValueError('inverse without canonical statement')

    async def query(self,session,plan):
        plan=copy.deepcopy(plan);workflow_ids=plan.get('workflow_ids',[])
        if any(wid not in self.workflows for wid in workflow_ids):raise ValueError('unknown ontology workflow')
        workflow_selection=plan.get('need_selection')=='workflow' or (
            'need_selection' not in plan and not plan['needs'] and bool(workflow_ids))
        if workflow_selection and not workflow_ids and not plan['question']:
            plan['question']='데이터를 어떤 업무에 활용하려고 하나요?'
        if not workflow_selection and not plan['needs'] and not plan['question']:
            plan['question']='필요한 데이터 종류를 한 가지 알려 주세요.'
        request_label=plan['purpose'] if workflow_selection else ' · '.join(self.indicators[n['indicator']]['label'] for n in plan['needs'])+' 자료 탐색'
        request={'id':'request:current','kind':'request','label':request_label or '자료 탐색 요청',
                 'properties':{'purpose':request_label},'provenance':{'basis':'user_intent','reference':'current_request'}}
        extra=[];extra_meta={}
        if not plan['question']:
            for target,relation in [('purpose:'+w,'selects_workflow') for w in workflow_ids]+[
                    ('indicator:'+n['indicator'],'requests_indicator') for n in plan['needs'] if not workflow_selection]:
                node=self.nodes[target];edge=Edge(request['id'],Node(target,node['kind']),relation);extra.append(edge)
                extra_meta[(edge.source,target,relation)]={'source':edge.source,'target':target,'relation':relation,
                    'label':LINK_TYPES[relation]['label'],'reason':'사용자 요청을 해석한 시작점', 'basis':'user_intent','derived_inverse':False}
        explicit=set() if workflow_selection else {n['indicator'] for n in plan['needs']}
        related_keys=[];related_targets=set();related_available=0
        if explicit and not plan['question'] and plan.get('related_mode','include')!='exclude':
            excluded=set(plan.get('related_exclusions',[]))
            for need in plan['needs']:
                for edge in self.adjacency['indicator:'+need['indicator']]:
                    target=edge.target.id.removeprefix('indicator:')
                    if edge.relation not in RELATED_RELATIONS or target in explicit or target in excluded or target in related_targets:continue
                    related_targets.add(target);related_available+=1
                    if len(related_keys)<4:related_keys.append((edge.source,edge.target.id,edge.relation))
        related_key_set=set(related_keys)
        allowed={'selects_workflow','requests_indicator','requires_analysis','uses_indicator','has_documented_offer',
                 'describes_resource','available_at','evidenced_by','covers_area','supports_format','accessed_via','in_catalog_domain'}|RELATED_RELATIONS
        def neighbors(node,limit):
            values=extra if node.id==request['id'] else self.adjacency.get(node.id,())
            def accepted(e):
                if e.relation not in allowed:return False
                if e.relation in RELATED_RELATIONS:return (e.source,e.target.id,e.relation) in related_key_set
                if explicit and e.target.kind=='indicator' and e.target.id.removeprefix('indicator:') not in explicit:return False
                if e.relation=='has_documented_offer' and (e.target.id,e.source,'documents_indicator') not in self.links:return False
                return True
            return (e for e in values if accepted(e))
        visited=await session.traverse_local([Node(request['id'],'request')],neighbors)
        seen={n.id for n in visited.nodes};edges=[]
        for e in visited.edges:
            key=(e.source,e.target.id,e.relation);edges.append(copy.deepcopy(extra_meta[key] if key in extra_meta else self.links[key]))
        reached={n.id.removeprefix('indicator:') for n in visited.nodes if n.kind=='indicator'}
        inferred=[]
        if workflow_selection and not plan['question']:
            prior_needs={n['indicator']:n for n in plan['needs']}
            plan['needs']=[]
            for e in edges:
                if e['relation']=='uses_indicator':
                    iid=e['target'].removeprefix('indicator:')
                    if iid not in inferred:inferred.append(iid)
            if len(inferred)>5:
                plan['question']='필요한 데이터 종류가 5개를 넘어요. 어떤 업무를 먼저 확인할까요?'
            else:
                for iid in inferred:
                    reason=next(e['reason'] for e in edges if e['relation']=='uses_indicator' and e['target']=='indicator:'+iid)
                    need={'indicator':iid,'subject':'','frequency':'','formats':[],'fields':[],
                          'unit':'','measurement_basis':'','clear':[]}
                    need.update(prior_needs.get(iid,{}));need['reason']=reason[:90]
                    plan['needs'].append(need)
        edge_keys={(e['source'],e['target'],e['relation']) for e in edges}
        related=[{'source_indicator':source.removeprefix('indicator:'),'indicator':target.removeprefix('indicator:'),
                  'relation':relation,'reason':self.links[(source,target,relation)]['reason'],
                  'references':list(self.links[(source,target,relation)].get('references',[]))}
                 for source,target,relation in related_keys if (source,target,relation) in edge_keys]
        candidates={iid:[] for iid in reached};paths=[]
        def route(target,prefer_workflow=True):
            def search(skip_direct):
                pending=deque([(request['id'],[request['id']],[])]);seen_route={request['id']}
                while pending:
                    current,nodes,relations=pending.popleft()
                    if current==target:return nodes,relations
                    for e in edges:
                        if e['source']!=current or (skip_direct and e['relation']=='requests_indicator') or e['target'] in seen_route:continue
                        seen_route.add(e['target']);pending.append((e['target'],nodes+[e['target']],relations+[e['relation']]))
                return None
            return (search(True) if prefer_workflow and workflow_ids else None) or search(False)
        for iid in reached:
            for edge in edges:
                if edge['source']!='indicator:'+iid or edge['relation']!='has_documented_offer':continue
                cid=edge['target'];c=self.claims[cid]
                needed=[(cid,'provider:'+c['source_id'],'available_at'),
                        (cid,c['ontology_resource_id'],'describes_resource'),(cid,c['ontology_evidence_id'],'evidenced_by')]
                if not all(key in edge_keys for key in needed):continue
                candidates[iid].append(c)
                prefix=route('indicator:'+iid,prefer_workflow=workflow_selection)
                if prefix:
                    nodes,relations=prefix
                    paths.append({'indicator':iid,'source_id':c['source_id'],'claim_id':cid,'resource_id':c['ontology_resource_id'],
                        'evidence_id':c['ontology_evidence_id'],'node_ids':nodes+[cid,'provider:'+c['source_id']],
                        'relations':relations+['has_documented_offer','available_at'],
                        'rule':'documented_site_candidate','origin':'related_recommendation' if any(r in RELATED_RELATIONS for r in relations) else 'workflow_inference' if 'requires_analysis' in relations else 'explicit_request'})
        payload={'version':SCHEMA_VERSION,'revision':self.revision,'engine':'bounded_typed_graph',
            'status':'warning' if visited.truncated else 'success','summary':'정의된 개념·관계를 실제 탐색한 결과입니다.',
            'roots':[request['id']],'workflow_ids':workflow_ids,'inferred_indicators':inferred,
            'related_indicators':related,'related_limit':4,'related_truncated':related_available>4,
            'nodes':[copy.deepcopy(request if n.id==request['id'] else self.nodes[n.id]) for n in visited.nodes],
            'edges':edges,'paths':paths,'truncated':visited.truncated,'reasons':list(visited.reasons),
            'object_types':{kind:copy.deepcopy(OBJECT_TYPES[kind]) for kind in {n.kind for n in visited.nodes}},
            'link_types':{relation:copy.deepcopy(LINK_TYPES[relation]) for relation in {e['relation'] for e in edges}},
            'rules':copy.deepcopy(INFERENCE_RULES),'usage':{'nodes':len(visited.nodes),'edges':len(visited.edges)},
            'limits':{'depth':session.policy.ontology.max_depth,'nodes':session.policy.ontology.max_nodes,
                      'edges':session.policy.ontology.max_edges,'neighbor_items':session.policy.ontology.max_neighbor_items},
            'comparability':{'status':'not_verified','requirements':list(COMPARABILITY_REQUIREMENTS),
                             'message':'관계가 연결돼도 실제 자료의 결합·비교 가능성은 별도 검증이 필요해요.'},
            'next_actions':['조건을 확인하거나 공식 사이트에서 원본 범위를 확인하세요.'],
            'artifacts':[{'kind':'ontology_definition','version':SCHEMA_VERSION,'revision':self.revision}]}
        return plan,candidates,payload

    def describe(self):
        return {'version':SCHEMA_VERSION,'revision':self.revision,'object_types':copy.deepcopy(OBJECT_TYPES),
                'link_types':copy.deepcopy(LINK_TYPES),'rules':copy.deepcopy(INFERENCE_RULES),
                'concepts':copy.deepcopy(self.vocabulary['concepts']),'workflows':copy.deepcopy(self.vocabulary['workflows']),
                'catalog_domains':copy.deepcopy(self.vocabulary.get('catalog_domains',[])),
                'catalog_bindings':copy.deepcopy(self.vocabulary.get('catalog_bindings',[])),
                'related_links':copy.deepcopy(self.vocabulary.get('related_links',[])),
                'nodes':len(self.nodes),'edges':len(self.links),'assertions':len(self.claims)}


def validate_graph_response(graph,response):
    """Validate paths, typed edges and evidence attachments at the HTTP bridge."""
    if not isinstance(graph,dict) or graph.get('version')!=SCHEMA_VERSION or graph.get('engine')!='bounded_typed_graph':raise ValueError('invalid ontology envelope')
    nodes=graph.get('nodes');edges=graph.get('edges');paths=graph.get('paths')
    if not isinstance(nodes,list) or not 1<=len(nodes)<=192 or not isinstance(edges,list) or len(edges)>384 or not isinstance(paths,list) or len(paths)>160:raise ValueError('unbounded ontology response')
    by_id={}
    for node in nodes:
        if not isinstance(node,dict) or node.get('kind') not in OBJECT_TYPES:raise ValueError('invalid graph node')
        nid=node.get('id');Node(nid,node['kind'])
        if nid in by_id or not isinstance(node.get('label'),str) or len(node['label'])>300:raise ValueError('invalid graph identity')
        props=node.get('properties');schema=OBJECT_TYPES[node['kind']]['properties']
        if not isinstance(props,dict) or set(props)!=set(schema) or any(not valid_property(props[k],kind) for k,kind in schema.items()):raise ValueError('invalid graph properties')
        by_id[nid]=node
    if graph.get('roots')!=['request:current'] or 'request:current' not in by_id or by_id['request:current']['kind']!='request':raise ValueError('invalid graph roots')
    keys=set();adj=defaultdict(list)
    for edge in edges:
        if not isinstance(edge,dict) or edge.get('relation') not in LINK_TYPES:raise ValueError('invalid graph predicate')
        spec=LINK_TYPES[edge['relation']];source=edge.get('source');target=edge.get('target')
        if source not in by_id or target not in by_id or by_id[source]['kind']!=spec['domain'] or by_id[target]['kind']!=spec['range'] or edge.get('basis')!=spec['basis']:raise ValueError('graph domain/range violation')
        key=(source,target,edge['relation'])
        if key in keys:raise ValueError('duplicate graph edge')
        if not isinstance(edge.get('label'),str) or not isinstance(edge.get('reason'),str):raise ValueError('invalid edge text')
        keys.add(key);adj[source].append(target)
    reachable=set(graph['roots']);pending=deque(reachable)
    while pending:
        for target in adj[pending.popleft()]:
            if target not in reachable:reachable.add(target);pending.append(target)
    if reachable!=set(by_id):raise ValueError('unreachable graph observation')
    if graph.get('usage')!={'nodes':len(nodes),'edges':len(edges)}:raise ValueError('invalid graph counts')
    if type(graph.get('truncated')) is not bool or not isinstance(graph.get('reasons'),list):raise ValueError('invalid graph truncation')
    if graph.get('status')!=('warning' if graph['truncated'] else 'success'):raise ValueError('invalid graph status')
    declared=graph.get('related_indicators',[])
    if not isinstance(declared,list) or len(declared)>4 or any(not isinstance(x,dict) for x in declared):raise ValueError('invalid related declarations')
    declared_keys={(x.get('source_indicator'),x.get('indicator'),x.get('relation')) for x in declared}
    if len(declared_keys)!=len(declared):raise ValueError('duplicate related declaration')
    groups=response.get('related_groups',[])
    if {(g['source_indicator'],g['indicator'],g['relation']) for g in groups}!=declared_keys:raise ValueError('related recommendations differ from traversed links')
    edge_by_key={(e['source'],e['target'],e['relation']):e for e in edges}
    for group in groups:
        key=('indicator:'+group['source_indicator'],'indicator:'+group['indicator'],group['relation'])
        edge=edge_by_key.get(key)
        if group['relation'] not in RELATED_RELATIONS or edge is None or group['reason']!=edge['reason'] or group['references']!=edge.get('references',[]):raise ValueError('related recommendation lacks its relation')
        declaration=next(x for x in declared if x.get('indicator')==group['indicator'])
        if any(declaration.get(k)!=group[k] for k in ('source_indicator','indicator','relation','reason','references')):raise ValueError('related declaration changed')
    related_by_indicator={g['indicator']:g for g in groups}
    matched={}
    for group in [*response['groups'],*response.get('related_groups',[])]:
        expected_domains={e['target'] for e in edges if e['source']=='indicator:'+group['indicator'] and e['relation']=='in_catalog_domain'}
        domains=group.get('catalog_domains',[])
        if {d['node_id'] for d in domains}!=expected_domains:raise ValueError('catalog bindings differ from traversed graph')
        for domain in domains:
            node=by_id[domain['node_id']]
            if domain!={'node_id':node['id'],'name':node['label'],**node['properties']}:raise ValueError('catalog classification changed')
        for site in group['sites']:
            for fact in site['evidence']:
                cid=fact.get('ontology_claim_id');rid=fact.get('ontology_resource_id');eid=fact.get('ontology_evidence_id')
                if cid not in by_id or rid not in by_id or eid not in by_id:raise ValueError('site lacks admitted provenance')
                if by_id[cid]['kind']!='assertion' or by_id[rid]['kind']!='dataset' or by_id[eid]['kind']!='evidence':raise ValueError('invalid provenance kinds')
                if canonical_url(fact['evidence_url'])!=by_id[eid]['properties']['url']:raise ValueError('evidence URL mismatch')
                required={(cid,rid,'describes_resource'),(cid,eid,'evidenced_by'),(cid,'provider:'+site['source_id'],'available_at'),
                          ('indicator:'+group['indicator'],cid,'has_documented_offer')}
                if not required<=keys:raise ValueError('incomplete recommendation path')
                matched[(group['indicator'],site['source_id'],cid)]=fact
    explained=set()
    for path in paths:
        if not isinstance(path,dict):raise ValueError('invalid graph path')
        key=(path.get('indicator'),path.get('source_id'),path.get('claim_id'))
        seq=path.get('node_ids');relations=path.get('relations')
        if key not in matched or not isinstance(seq,list) or not 4<=len(seq)<=8 or not isinstance(relations,list) or len(relations)!=len(seq)-1:raise ValueError('invalid recommendation explanation')
        if seq[0]!='request:current' or seq[-1]!='provider:'+path['source_id'] or 'indicator:'+path['indicator'] not in seq or path['claim_id'] not in seq:raise ValueError('invalid explanation endpoints')
        if any((a,b,r) not in keys for a,b,r in zip(seq,seq[1:],relations)):raise ValueError('fabricated explanation edge')
        related_relations=[r for r in relations if r in RELATED_RELATIONS]
        expected_origin='related_recommendation' if related_relations else 'workflow_inference' if 'requires_analysis' in relations else 'explicit_request'
        if path.get('origin')!=expected_origin or len(related_relations)>1:raise ValueError('invalid recommendation origin')
        related_group=related_by_indicator.get(path['indicator'])
        if related_group:
            expected_nodes=['request:current','indicator:'+related_group['source_indicator'],'indicator:'+related_group['indicator'],path['claim_id'],'provider:'+path['source_id']]
            expected_relations=['requests_indicator',related_group['relation'],'has_documented_offer','available_at']
            if seq!=expected_nodes or relations!=expected_relations or path['origin']!='related_recommendation':raise ValueError('related explanation has wrong origin')
        elif related_relations:raise ValueError('direct explanation uses related recommendation')
        if path.get('resource_id')!=matched[key]['ontology_resource_id'] or path.get('evidence_id')!=matched[key]['ontology_evidence_id']:raise ValueError('explanation provenance mismatch')
        explained.add(key)
    if explained!=set(matched):raise ValueError('recommendation missing graph explanation')
    return graph
