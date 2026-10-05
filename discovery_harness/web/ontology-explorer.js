(() => {
  'use strict';
  const $=id=>document.getElementById(id),el=(tag,className,text)=>{const n=document.createElement(tag);if(className)n.className=className;if(text!==undefined)(window.DataieumI18n.unbind(n),n.textContent=String(text));return n;};
  const ui=(tag,cls,source,params={})=>window.DataieumI18n.bind(el(tag,cls),source,params);
  const fmt=n=>typeof n==='number'?window.DataieumI18n.number(n):window.DataieumI18n.text('미확인');
  const displayName=(id,label)=>window.ConceptMap.name(id,label);
  const text=v=>v===null||v===undefined||v===''?'미확인':Array.isArray(v)?v.map(text).join(' · '):typeof v==='object'?JSON.stringify(v,null,2):String(v);
  const safeURL=value=>{try{const u=new URL(value);return ['http:','https:'].includes(u.protocol)&&!u.username&&!u.password?u.href:null;}catch{return null;}};
  const link=(label,url)=>{const u=safeURL(url),a=el(u?'a':'span','',label);if(u){a.href=u;a.target='_blank';a.rel='noopener noreferrer';}return a;};
  let schema,hierarchy,graphMode='hierarchy',domain='',selected='',page=1,pages=1,search='',assetConcept='',catalogRequest,recordRequest,sitesRequest,intentRequest,searchContext=null,countries=[],inspectorOpen=false;
  const params=new URLSearchParams(location.search);let selectionExplicit=Boolean(params.get('indicator'));domain=params.get('domain')||'';selected=params.get('indicator')||'';assetConcept=params.get('view')==='catalogue'?'':selected;
  const status=(message,error=false)=>{window.DataieumI18n.bind($('status'),message);$('status').className=error?'warning':'muted';};
  const showError=(node,error)=>window.DataieumI18n.bind(node,error.i18nSource||'자료를 불러오지 못했어요. 다시 시도해 주세요.',error.i18nParams||{});
  try{countries=JSON.parse(params.get('countries')||'[]');if(!Array.isArray(countries)||countries.length>5||countries.some(c=>typeof c!=='string'||!c.trim()||c.length>60))countries=[];}catch{countries=[];}
  async function get(path,signal){
    const r=await fetch(path,{cache:'no-store',signal,headers:{'X-Dataieum-Chat':'1'}});
    if(r.ok)return r.json();
    const seconds=Math.min(3600,Math.max(1,Number.parseInt(r.headers.get('Retry-After'),10)||1));let detail;try{detail=await r.json();}catch{}
    const source=r.status===429?'같은 IP의 요청 한도에 도달했어요. {seconds}초 후 다시 시도해 주세요.':detail?.code==='unsafe_prompt'?'검색과 관계없는 지시가 포함되어 있어요. 필요한 데이터와 조건만 입력해 주세요.':r.status===503?'요청이 몰렸거나 연결이 준비 중이에요. 잠시 후 다시 시도해 주세요.':r.status===504?'응답 대기 시간이 지났어요. 다시 시도해 주세요.':r.status===400?'입력 조건을 확인해 주세요. 국가는 최대 5개까지 지정할 수 있어요.':'자료를 불러오지 못했어요. 다시 시도해 주세요.';
    throw Object.assign(new Error(source),{i18nSource:source,i18nParams:{seconds}});
  }
  function updateURL(){const p=new URLSearchParams();if(domain)p.set('domain',domain);if(selected)p.set('indicator',selected);if(countries.length)p.set('countries',JSON.stringify(countries));if(selected&&!assetConcept)p.set('view','catalogue');history.replaceState(null,'','/ontology'+(p.size?'?'+p:''));}
  function concepts(){return schema.nodes.filter(n=>n.kind==='indicator'&&n.id!=='indicator:other');}
  const hasSources=id=>hierarchy?.nodes.some(n=>n.id===id&&n.legacy_id);
  const focusCurrent=()=>$('relationship-graph').querySelector('.hierarchy-current,.concept-map-source')?.focus({preventScroll:true});
  function renderConceptList(){
    if(!schema)return;
    const q=$('concept-search').value.trim().toLowerCase();$('concept-list').replaceChildren();
    const allowed=new Set(schema.edges.filter(e=>e.relation==='in_catalog_domain'&&(!domain||e.target==='domain:'+domain)).map(e=>e.source));
    for(const n of concepts()){
      if(domain&&!allowed.has(n.id))continue;
      if(q&&![n.label,...(n.properties.aliases||[])].some(s=>s.toLowerCase().includes(q)))continue;
      const b=el('button','',displayName(n.id.slice(10),n.label));b.type='button';b.setAttribute('aria-pressed',String(n.id==='indicator:'+selected));
      b.addEventListener('click',()=>chooseConcept(n.id.slice(10)));$('concept-list').append(b);
    }
    if(!$('concept-list').children.length)$('concept-list').append(ui('p','muted',"일치하는 정의가 없어요. 아래에서 실제 자료를 검색할 수 있어요.",{}));
  }
  function closeInspector(){inspectorOpen=false;sitesRequest?.abort();sitesRequest=null;$('node-inspector').hidden=true;$('graph-workspace').classList.remove('has-inspector');$('site-cards').replaceChildren();}
  function openInspector(){
    inspectorOpen=true;$('node-inspector').hidden=false;$('graph-workspace').classList.add('has-inspector');
    const root=$('relationship-graph').querySelector('.hierarchy-current,.concept-map-source');root?.setAttribute('aria-pressed','true');root?.focus({preventScroll:true});
    const node=concepts().find(n=>n.id==='indicator:'+selected);(window.DataieumI18n.unbind($('node-heading')),$('node-heading').textContent=displayName(selected,node?.label||selected));(window.DataieumI18n.unbind($('node-description')),$('node-description').textContent=window.ConceptMap.brief(node?.properties.definition||''));loadSites();
  }
  function chooseConcept(id,inspect=false){
    cancelIntent();
    closeInspector();
    // In a multi-country question, follow the node the user is leaving, not
    // the first result's per-indicator country/year/format restrictions.
    const origin=searchContext?.needs.find(n=>n.indicator===selected);
    if(origin&&!searchContext.needs.some(n=>n.indicator===id))searchContext={...searchContext,needs:[origin]};
    searchContext=window.HierarchyMap.context(searchContext,id,selected);selectionExplicit=true;selected=id;assetConcept=id;page=1;search='';$('query').value='';$('record-detail').hidden=true;recordRequest?.abort();$('concept-browser').open=false;updateURL();renderConceptList();renderConcept();if(inspect)openInspector();else focusCurrent();if($('catalog-panel').open)loadCatalog();
  }
  function openCatalog(){if($('catalog-panel').open)loadCatalog();else $('catalog-panel').open=true;}
  function browseCatalog(){
    domain='';assetConcept='';page=1;$('domain').value='';updateURL();openCatalog();$('query').focus();
  }
  function renderConcept(){
    if(!schema||!hierarchy)return;
    const n=schema.nodes.find(n=>n.id==='indicator:'+selected),detail=$('concept-detail'),graph=$('relationship-graph'),list=$('relationship-details');
    graph.replaceChildren();list.replaceChildren();
    if(!n){detail.replaceChildren(ui('h2','',"개념을 선택하세요",{}),ui('p','muted',"개념의 정의와 사전에 검토한 관련 관계를 확인할 수 있어요.",{}));return;}
    $('hierarchy-mode').disabled=$('related-mode').disabled=false;detail.replaceChildren(el('h2','',displayName(selected,n.label)),el('p','hierarchy-definition',n.properties.definition));
    $('hierarchy-mode').setAttribute('aria-pressed',String(graphMode==='hierarchy'));$('related-mode').setAttribute('aria-pressed',String(graphMode==='related'));
    (graphMode==='hierarchy'?window.DataieumI18n.bind($('graph-help'),"상위로 올라가거나 하위를 선택해 범위를 좁혀 보세요.",{}):window.DataieumI18n.bind($('graph-help'),"함께 살펴볼 개념이에요. 상하위 포함 관계와는 구분됩니다.",{}));
    (searchContext?window.DataieumI18n.bind($('graph-scope'),"자료 조건: {value1}",()=>({value1:(appliedConditions(searchContext))})):(countries.length?window.DataieumI18n.bind($('graph-scope'),"자료 국가: {value1}",()=>({value1:(countries.join(' · '))})):(window.DataieumI18n.unbind($('graph-scope')),$('graph-scope').textContent='')));
    list.append(el('h3','',n.label),el('p','',n.properties.definition),
      ui('p','definition-meta',"개념 기준 단위: {value1} · 시간 의미: {value2}",()=>({value1:(text(n.properties.canonical_unit)),value2:({stock:'시점',flow:'기간 흐름',mixed:'자료마다 확인',static:'정적 정의',event:'사건'}[n.properties.temporal_meaning]||text(n.properties.temporal_meaning))})));
    if(n.properties.aliases?.length)list.append(ui('p','definition-meta',"같은 의미의 표현: {value1}",()=>({value1:(n.properties.aliases.join(' · '))})));
    const outgoing=schema.edges.filter(e=>e.source===n.id&&e.relation!=='in_catalog_domain');
    if(graphMode==='hierarchy')window.HierarchyMap.render(graph,{selected,dictionary:hierarchy,onSelect:id=>chooseConcept(id),onInspect:openInspector});
    else renderRelationGraph(graph,n,outgoing);
    for(const edge of hierarchy.edges.filter(e=>e.source===selected||e.target===selected)){const child=hierarchy.nodes.find(n=>n.id===edge.source),parent=hierarchy.nodes.find(n=>n.id===edge.target),item=el('article','relation-item');item.append(el('h3','',parent.label+' → '+child.label),el('p','',edge.reason),ui('p','evidence',"개념 정의 검토 · 실제 자료 분류는 별도 확인",{}));list.append(item);}
    for(const edge of outgoing){
      const target=schema.nodes.find(node=>node.id===edge.target),item=el('article','relation-item');
      const b=el('button','',n.label+' → '+target.label);b.type='button';b.addEventListener('click',()=>chooseConcept(target.id.slice(10)));
      item.append(b,el('p','type-badge',edge.label),el('p','',edge.reason));
      const specification=schema.semantic_relation_types[edge.relation];if(specification)item.append(el('p','evidence',specification.definition));
      for(const ref of edge.references||[])item.append(window.DataieumI18n.bind(link("",ref),"정의 참고 자료 ↗",{}),el('br'));
      list.append(item);
    }
    if(!outgoing.length)list.append(ui('p','muted',"별도로 정의된 관련 개념이 아직 없어요.",{}));
    const domains=schema.edges.filter(e=>e.source===n.id&&e.relation==='in_catalog_domain');
    for(const edge of domains){
      const d=schema.nodes.find(node=>node.id===edge.target),b=ui('button','',"{value1} 카탈로그 분야 보기",()=>({value1:(d.label)}));b.type='button';
      b.addEventListener('click',()=>{domain=d.properties.id;assetConcept='';$('domain').value=domain;page=1;updateURL();renderConceptList();openCatalog();});list.append(b);
    }
  }
  function appliedConditions(plan){
    const labels={countries:'국가',regions:'지역',years:'연도',dates:'기간',frequency:'시간 단위',formats:'형식',fields:'컬럼',geography_level:'지역 단위',additional_requirements:'추가 조건',subject:'대상',unit:'단위',measurement_basis:'측정 방식'};
    const need=plan.needs?.find(n=>n.indicator===selected)||(plan.selected_concept===selected?plan.needs[0]:null),criteria={...plan};
    if(need){for(const key of ['subject','unit','frequency','formats','fields','measurement_basis'])if(need[key]?.length)criteria[key]=need[key];for(const s of need.scope||[])criteria[s.field]=s.values;}
    const parts=[];for(const [key,label] of Object.entries(labels)){const value=criteria[key];if(Array.isArray(value)?value.length:value)parts.push(window.DataieumI18n.text(label)+': '+text(value));}
    if(plan.free_only)parts.push(window.DataieumI18n.text('무료'));if(plan.commercial_only)parts.push(window.DataieumI18n.text('상업 이용'));if(plan.delivery)parts.push(plan.delivery==='api'?'API':window.DataieumI18n.text('다운로드'));
    return parts.join(' · ')||window.DataieumI18n.text('여러 국가');
  }
  function renderSites(data){
    if(!inspectorOpen)return;
    $('site-cards').replaceChildren();$('sites-retry').hidden=true;
    const group=data.groups.find(g=>g.indicator===selected);window.DataieumI18n.bind($('sites-heading'),"{value1} 보는 곳",()=>({value1:(displayName(selected,group?.name||'선택한 데이터'))}));
    window.DataieumI18n.bind($('applied-scope'),'{value}',()=>({value:appliedConditions(data.plan)}));window.DataieumI18n.bind($('graph-scope'),"자료 조건: {value1}",()=>({value1:(appliedConditions(data.plan))}));
    if(!group){(window.DataieumI18n.unbind($('sites-status')),$('sites-status').textContent=data.question||'찾는 데이터 종류를 조금 더 구체적으로 알려 주세요.');return;}
    (group.sites.length?window.DataieumI18n.bind($('sites-status'),"확인된 사이트 {value1}곳",()=>({value1:(group.sites.length)})):window.DataieumI18n.bind($('sites-status'),"이 조건에 맞는 사이트를 아직 확인하지 못했어요.",{}));
    if(group.unverified.length)$('site-cards').append(ui('p','warning',"확인하지 못한 조건: {value1}",()=>({value1:(group.unverified.join(' · '))})));
    for(const site of group.sites){
      const card=el('article','site-card'),formats=[...new Set(site.evidence.flatMap(f=>f.formats||[]))];
      card.append(el('h3','',site.name),ui('p','format-label',"형식: {value1}",()=>({value1:((formats.length?formats.join(' · '):window.DataieumI18n.text('미확인')))})),window.DataieumI18n.bind(link("",site.evidence[0].evidence_url),"자료 안내 열기 ↗",{}));
      const more=el('details');more.append(ui('summary','',"제공 범위·이용조건",{}));
      for(const fact of site.evidence){
        const offer=el('div','site-offer');offer.append(link(fact.title+' ↗',fact.evidence_url),el('p','',fact.summary),ui('p','format-label',"데이터 형식: {value1}",()=>({value1:((fact.formats.length?fact.formats.join(' · '):window.DataieumI18n.text('미확인')))})));
        offer.append(ui('p','',"국가: {value1}",()=>({value1:(text(fact.countries))})),el('p','',fact.access_note),ui('p','evidence',"확인일: {value1}",()=>({value1:(fact.checked_on)})));more.append(offer);
      }
      more.append(window.DataieumI18n.bind(link("",site.url),"{value1} 홈페이지 ↗",()=>({value1:(site.name)})));card.append(more);$('site-cards').append(card);
    }
    if(data.relaxation){const b=el('button','',data.relaxation.label);b.type='button';b.addEventListener('click',()=>{$('intent-query').value=data.relaxation.query;submitIntent();});$('site-cards').append(b);}
    if(data.ontology?.truncated)$('site-cards').append(ui('p','warning',"탐색 한도 내에서 확인한 결과예요. 모든 경로를 확인하지는 못했어요.",{}));
    if(!group.sites.length){const b=ui('button','',"관계 검토 표본 보기",{});b.type='button';b.addEventListener('click',()=>{openCatalog();$('catalog-panel').scrollIntoView({block:'start'});});$('site-cards').append(b);}
  }
  async function loadSites(){
    if(!inspectorOpen)return;
    if(!hasSources(selected)){sitesRequest?.abort();sitesRequest=null;$('site-cards').replaceChildren();$('sites-retry').hidden=true;window.DataieumI18n.bind($('sites-heading'),"자료 제공처",{});(searchContext?window.DataieumI18n.bind($('applied-scope'),'{value}',()=>({value:appliedConditions(searchContext)})):window.DataieumI18n.bind($('applied-scope'),'{value}',()=>({value:countries.join(' · ')})));window.DataieumI18n.bind($('sites-status'),"이 개념에 연결할 공식 자료는 아직 검토되지 않았어요. 상위 개념의 자료를 자동으로 물려주지 않아요.",{});return;}
    sitesRequest?.abort();const request=new AbortController();sitesRequest=request;const timer=setTimeout(()=>request.abort(),20000);
    $('sites-retry').hidden=true;$('site-cards').replaceChildren();window.DataieumI18n.bind($('sites-heading'),"{value1} 보는 곳",()=>({value1:(displayName(selected,concepts().find(n=>n.id==='indicator:'+selected)?.label||'선택한 데이터'))}));window.DataieumI18n.bind($('sites-status'),"사이트를 확인하는 중…",{});(countries.length?window.DataieumI18n.bind($('applied-scope'),"국가: {value1}",()=>({value1:(countries.join(' · '))})):window.DataieumI18n.bind($('applied-scope'),"여러 국가",{}));
    try{const p=new URLSearchParams({indicator:selected});if(searchContext)p.set('context',JSON.stringify(searchContext));else p.set('countries',JSON.stringify(countries));const data=await get('/api/ontology/sites?'+p,request.signal);if(sitesRequest!==request)return;searchContext=data.plan;renderSites(data);}
    catch(e){if(sitesRequest!==request)return;(e.name==='AbortError'?window.DataieumI18n.bind($('sites-status'),"응답 대기 시간이 지났어요. 다시 시도해 주세요.",{}):showError($('sites-status'),e));$('sites-retry').hidden=false;}
    finally{clearTimeout(timer);}
  }
  function cancelIntent(){if(intentRequest){intentRequest.abort();intentRequest=null;window.DataieumI18n.bind($('intent-status'),"질문 해석을 취소했어요.",{});}$('intent-submit').disabled=false;$('intent-cancel').hidden=true;}
  async function submitIntent(){
    cancelIntent();closeInspector();const request=new AbortController();intentRequest=request;const timer=setTimeout(()=>request.abort(),20000);
    $('intent-submit').disabled=true;$('intent-cancel').hidden=false;window.DataieumI18n.bind($('intent-status'),"질문에서 데이터 개념과 조건을 찾는 중…",{});$('site-cards').replaceChildren();window.DataieumI18n.bind($('sites-status'),"새 질문의 해석을 기다리는 중…",{});$('intent-options').replaceChildren();
    try{
      const p=new URLSearchParams({q:$('intent-query').value.trim()});if(selectionExplicit){p.set('selected',selected);if(!searchContext)p.set('countries',JSON.stringify(countries));}if(searchContext)p.set('context',JSON.stringify(searchContext));
      const data=await get('/api/chat?'+p,request.signal);if(intentRequest!==request)return;searchContext=data.plan;
      if(data.state==='clarify'){$('hierarchy-mode').disabled=$('related-mode').disabled=true;(window.DataieumI18n.unbind($('intent-status')),$('intent-status').textContent=data.question);$('relationship-graph').replaceChildren();$('concept-detail').replaceChildren(ui('h2','',"검색 조건 확인",{}));(window.DataieumI18n.unbind($('graph-scope')),$('graph-scope').textContent='');$('relationship-details').replaceChildren();$('intent-query').value='';$('intent-query').focus();return;}
      countries=data.scope.countries;$('countries').value=countries.join(', ');if(!data.plan.selected_concept&&data.groups.every(g=>g.indicator==='other')){$('concept-detail').replaceChildren(ui('h2','',"연결된 개념을 아직 확인하지 못했어요.",{}));$('relationship-graph').replaceChildren();(window.DataieumI18n.unbind($('graph-scope')),$('graph-scope').textContent='');$('relationship-details').replaceChildren();$('hierarchy-mode').disabled=$('related-mode').disabled=true;window.DataieumI18n.bind($('intent-status'),"아래 자료 카탈로그에서 제목·설명으로 직접 찾아볼 수 있어요.",{});selected=assetConcept='';selectionExplicit=false;updateURL();return;}window.DataieumI18n.bind($('intent-status'),"관련 데이터를 눌러 이어서 살펴보세요.",{});
      if(data.groups.length>1)for(const group of data.groups){const b=el('button','',displayName(group.indicator,group.name));b.type='button';b.addEventListener('click',()=>{closeInspector();searchContext=data.plan;selected=assetConcept=group.indicator;page=1;updateURL();renderConceptList();renderConcept();if($('catalog-panel').open)loadCatalog();});$('intent-options').append(b);}
      if(data.groups.length){selected=assetConcept=data.plan.selected_concept||data.groups[0].indicator;selectionExplicit=Boolean(data.plan.selected_concept);page=1;updateURL();renderConceptList();renderConcept();if($('catalog-panel').open)loadCatalog();}
    }catch(e){if(intentRequest!==request)return;(e.name==='AbortError'?window.DataieumI18n.bind($('intent-status'),"20초 안에 해석하지 못했어요. 다시 시도해 주세요.",{}):showError($('intent-status'),e));window.DataieumI18n.bind($('sites-status'),"질문을 다시 보내거나 노드를 선택해 주세요.",{});}
    finally{clearTimeout(timer);if(intentRequest===request){intentRequest=null;$('intent-submit').disabled=false;$('intent-cancel').hidden=true;}}
  }
  function renderRelationGraph(parent,source,edges){
    window.ConceptMap.render(parent,{source:{id:source.id.slice(10),label:source.label},
      relations:edges.map(edge=>({id:edge.target.slice(10),label:schema.nodes.find(n=>n.id===edge.target).label,reason:edge.reason})),
      onSelect:node=>{if(node.id===selected)openInspector();else chooseConcept(node.id,true);}});
  }
  function renderModels(){
    $('object-models').replaceChildren();
    for(const m of schema.object_models){
      const item=el('details'),summary=el('summary','',m.label);item.append(summary,el('p','',m.definition));
      const attrs=el('div','table-wrap'),table=el('table'),head=el('tr');for(const label of ['속성','의미','유형','연결에 필요한가'])head.append(ui('th','',label));table.append(head);
      for(const a of m.attributes||[]){const tr=el('tr');for(const v of [a.name,a.meaning,a.data_type,a.required_for_join?'관계·하위유형별 조건부 필요':'별도 확인'])tr.append(el('td','',text(v)));table.append(tr);}attrs.append(table);item.append(attrs);
      for(const r of m.relationships||[]){const target=[...schema.object_models,...(schema.target_type_definitions||[])].find(t=>t.id===r.target_type),p=el('article','relation-item');p.append(el('h3','',r.label||r.predicate),el('p','',r.meaning),ui('p','definition-meta',"대상 객체: {value1} · 관계 수: {value2}",()=>({value1:(target?.label||r.target_type),value2:(r.cardinality)})),ui('p','evidence',"연결 조건: {value1}",()=>({value1:(text(r.join_requirements))})),ui('p','evidence',"제약: {value1}",()=>({value1:(text(r.limitations))})));item.append(p);}
      if(m.source_mapping)item.append(ui('p','evidence',"현재 카탈로그에서 확인: {value1}",()=>({value1:(text(m.source_mapping.catalogue_fields))})),ui('p','warning',"원본에서 추가로 필요한 필드: {value1}",()=>({value1:(text(m.source_mapping.missing_raw_fields))})));
      $('object-models').append(item);
    }
  }
  function renderAssetGraph(parent,data,record){
    const ns='http://www.w3.org/2000/svg',svg=(tag,attrs={},label)=>{const n=document.createElementNS(ns,tag);for(const [k,v] of Object.entries(attrs))n.setAttribute(k,v);if(label!==undefined)(window.DataieumI18n.unbind(n),n.textContent=label);return n;};
    const entries=data.edges.filter(e=>e.source===record.node_id).map(e=>({label:e.label,target:data.nodes.find(n=>n.id===e.target),recordId:null}));
    entries.push(...data.dataset_relationships.map(r=>({label:r.label,target:{label:r.target_dataset,kind:'catalog_record'},recordId:r.target_dataset})));
    if(!entries.length)return;
    const height=Math.max(180,entries.length*74+24),canvas=svg('svg',{viewBox:`0 0 960 ${height}`,height,role:'img','aria-label':'현재 자료에서 실제 메타데이터 객체와 검토된 자료로 이어지는 관계'}),cy=height/2;
    const box=(x,y,label,kind,recordId)=>{const g=svg('g',{transform:`translate(${x},${y})`,class:'graph-node'});g.append(svg('rect',{width:270,height:50,rx:5}),svg('text',{x:12,y:19,class:'graph-label'},data.object_types[kind]?.label||kind),svg('text',{x:12,y:39},label.length>25?label.slice(0,24)+'…':label),svg('title',{},label));if(recordId){g.setAttribute('role','button');g.setAttribute('tabindex','0');g.setAttribute('aria-label',label+' 자료 보기');const activate=()=>loadRecord(recordId);g.addEventListener('click',activate);g.addEventListener('keydown',e=>{if(['Enter',' '].includes(e.key)){e.preventDefault();activate();}});}canvas.append(g);};
    for(const [i,item] of entries.entries()){const y=12+i*74;canvas.append(svg('path',{d:`M 280 ${cy} C 470 ${cy} 485 ${y+25} 665 ${y+25}`,class:'graph-edge'}),svg('text',{x:475,y:y+14,'text-anchor':'middle',class:'graph-label'},item.label));box(665,y,item.target.label,item.target.kind,item.recordId);}
    box(10,cy-25,record.title||record.id,'catalog_record',null);const wrap=el('div','relation-graph');wrap.append(canvas);parent.append(wrap);
  }
  async function loadCatalog(){
    catalogRequest?.abort();const request=new AbortController();catalogRequest=request;
    window.DataieumI18n.bind($('catalog-status'),"현재 DB의 자료를 불러오는 중…",{});$('previous').disabled=$('next').disabled=true;
    if(assetConcept&&!hasSources(assetConcept)){$('records').replaceChildren();window.DataieumI18n.bind($('catalog-status'),"이 개념으로 개별 검토한 자료가 아직 없어요.",{});(window.DataieumI18n.unbind($('page-state')),$('page-state').textContent='');$('all-records').hidden=false;return;}
    const query=new URLSearchParams({page:String(page)});if(assetConcept)query.set('indicator',assetConcept);else if(domain)query.set('concept',domain);if(search)query.set('q',search);
    $('all-records').hidden=!assetConcept;
    try{
      const data=await get('/api/ontology/catalog?'+query,request.signal);if(catalogRequest!==request)return;
      page=data.page;pages=data.pages;$('records').replaceChildren();
      window.DataieumI18n.bind($('catalog-status'),"{value1}{value2}개 · 추천 사이트 목록이나 전체 자료의 검증 완료 수가 아닙니다.",()=>({value1:((assetConcept?window.DataieumI18n.text('이 개념으로 개별 검토한 표본 '):window.DataieumI18n.text('분류·검색에 해당하는 등록 자료 '))),value2:(fmt(data.total))}));
      for(const record of data.records){
        const row=el('article','record-row'),body=el('div'),provider=schema.sources.find(s=>s.id===record.source_id);
        body.append(el('h3','',record.title||record.id),el('p','',(provider?.name||record.source_id)+' · '+record.id));
        const interpretations=record.interpretations.filter(a=>a.current_evidence_matches);
        body.append((interpretations.length?ui('p',interpretations.length?'':'muted',"분석가 해석 {value1}건 · 메타데이터 기준",()=>({value1:(interpretations.length)})):ui('p',interpretations.length?'':'muted',"객체 관계는 원본 메타데이터에서 연결 · 개별 의미 해석은 미검토",{})));
        const button=ui('button','',"자료 설명·원문 보기",{});button.type='button';button.addEventListener('click',()=>loadRecord(record.id));row.append(body,button);$('records').append(row);
      }
      if(!data.records.length)$('records').append((assetConcept?ui('p','muted',"이 개념으로 개별 검토한 자료가 아직 없어요. 분야 전체 자료는 별도로 탐색할 수 있어요.",{}):ui('p','muted',"이 조건의 등록 자료가 없어요.",{})));
      (window.DataieumI18n.unbind($('page-state')),$('page-state').textContent=fmt(page)+' / '+fmt(pages));$('previous').disabled=page<=1;$('next').disabled=page>=pages;
    }catch(e){if(e.name!=='AbortError')showError($('catalog-status'),e);}
  }
  async function loadRecord(id){
    recordRequest?.abort();const request=new AbortController();recordRequest=request;const target=$('record-detail');target.hidden=false;
    target.replaceChildren(ui('p','muted',"자료의 객체·관계를 확인하는 중…",{}));target.scrollIntoView({block:'start'});
    try{
      const data=await get('/api/ontology/record?'+new URLSearchParams({id}),request.signal);if(recordRequest!==request)return;
      const record=data.records[0];if(!record){target.replaceChildren(ui('p','',"등록 자료를 찾지 못했어요.",{}));return;}
      const node=data.nodes.find(n=>n.id===record.node_id);target.replaceChildren(ui('p','eyebrow',"실제 등록 자료",{}),el('h2','',record.title||record.id),ui('p','muted',"등록 메타데이터 기준 · 실제 범위와 이용 조건은 공식 페이지에서 확인하세요.",{}));
      const back=ui('button','',"검색 결과로 돌아가기",{});back.type='button';back.addEventListener('click',()=>{target.hidden=true;openCatalog();$('query').focus();});target.append(back);
      const metadataPanel=el('section','metadata-panel');target.append(metadataPanel);
      loadMetadata(id,metadataPanel,request);
      const advanced=el('details');advanced.append(ui('summary','',"객체 관계와 검토 근거 자세히 보기",{}),el('p','muted',data.scope));target.append(advanced);
      renderAssetGraph(advanced,data,record);
      const fields=el('dl');for(const [key,label] of [['id','카탈로그 식별자'],['source_id','제공처 ID'],['native_id','원본 식별자 표기'],['publisher','발행자 표기'],['region','지역 표기'],['period','기간 표기'],['reference_years','기준연도 추출값'],['year_basis','연도 추출 근거'],['format','형식 표기']])fields.append(ui('dt','',label),el('dd','',text(node.properties[key])));advanced.append(fields);
      advanced.append(ui('h3','',"이 자료에서 확인되는 객체 관계",{}));
      const relations=el('div','record-relations');
      for(const edge of data.edges.filter(e=>e.source===record.node_id)){
        const end=data.nodes.find(n=>n.id===edge.target),item=el('article','record-relation');item.append(el('strong','',edge.label+' → '+end.label));
        if(end.properties.url)item.append(window.DataieumI18n.bind(link("",end.properties.url),"등록 원문 ↗",{}));
        if(end.properties.catalogue_id){const button=ui('button','',"참조한 자료 열기",{});button.type='button';button.addEventListener('click',()=>loadRecord(end.properties.catalogue_id));item.append(button);}
        item.append(ui('p','evidence',"근거 필드: {value1}",()=>({value1:(edge.provenance.source_field)})));
        if(edge.provenance.evidence)item.append(el('p','evidence',text(edge.provenance.evidence)));
        if(edge.properties.status)item.append(ui('p','evidence',"원본 상태: {value1} · 방법: {value2}",()=>({value1:(edge.properties.status),value2:(text(edge.properties.method))})));
        item.append(el('p','muted',data.relation_types[edge.relation].meaning));relations.append(item);
      }advanced.append(relations);
      if(data.truncated)advanced.append(ui('p','warning',"화면의 관계 표시 한도에 도달했어요. 원본 필드의 모든 의미를 확인했다는 뜻은 아니에요.",{}));
      advanced.append(ui('h3','',"데이터 분석가의 개별 해석",{}));
      if(!record.interpretations.length)advanced.append(ui('p','muted',"이 자료의 개별 의미 해석은 아직 검토되지 않았어요. 위의 원본 메타데이터 관계만 확인할 수 있어요.",{}));
      for(const a of record.interpretations){const article=el('article','relation-item');article.append((!a.current_evidence_matches?ui('p','type-badge',"근거 변경 · 재검토 필요",{}):(a.status==='unresolved'?ui('p','type-badge',"의미 해석 보류",{}):ui('p','type-badge',"메타데이터 해석 검토",{}))),el('p','',a.interpretation),ui('p','evidence',"검토 분야: {value1}",()=>({value1:(schema.nodes.find(n=>n.id==='domain:'+a.domain)?.label||a.domain)})),ui('p','evidence',"집계 수준: {value1}",()=>({value1:(text(a.granularity))})),ui('p','evidence',"확인된 원본 연결 키: {value1}",()=>({value1:((a.join_keys?.length?text(a.join_keys):window.DataieumI18n.text('없음 · 추가 확인 필요')))})),el('p','warning',text(a.limitations)));advanced.append(article);}
      advanced.append(ui('h3','',"다른 실제 자료와의 업무 관계",{}));
      if(!data.dataset_relationships.length)advanced.append(ui('p','muted',"다른 자료와의 개별 업무 관계는 아직 정의되지 않았어요. 같은 분류라는 이유로 관계를 만들지 않습니다.",{}));
      for(const relation of data.dataset_relationships){const item=el('article','relation-item'),b=el('button','',relation.label||relation.predicate);b.type='button';b.addEventListener('click',()=>loadRecord(relation.target_dataset));item.append(b,el('p','',relation.meaning),ui('p','evidence',"대상 자료: {value1}",()=>({value1:(relation.target_dataset)})),ui('p','warning',"물리적 연결: 미검증 · 필요한 키: {value1}",()=>({value1:(text(relation.required_keys))})),el('p','evidence',text(relation.limitations)));advanced.append(item);}
      target.focus({preventScroll:true});
    }catch(e){if(e.name!=='AbortError')target.replaceChildren(el('p','warning',e.message));}
  }
  async function loadMetadata(id,panel,request){
    panel.replaceChildren(ui('p','muted',"공식 메타데이터를 확인하는 중…",{}));
    try{
      const data=await get('/api/ontology/metadata?'+new URLSearchParams({id}),request.signal);
      if(recordRequest!==request||!panel.isConnected)return;
      const m=data.metadata;if(!m){panel.replaceChildren(ui('p','muted',"등록 메타데이터를 찾지 못했어요.",{}));return;}
      panel.replaceChildren(ui('h3','',"자료 설명",{}));if(m.dataset_url)panel.append(window.DataieumI18n.bind(link("",m.dataset_url),"공식 자료 페이지 열기 ↗",{}));
      const fields=el('dl'),labels={title:'제목',description:'설명',classification_paths:'공식 분류 경로',survey_name:'조사명',tags:'공식 태그'};
      for(const [key,label] of Object.entries(labels)){
        const value=m.fields[key],dd=el('dd','',value||'등록 메타데이터에 없음');
        if(m.preview_truncated_fields.includes(key))dd.append(ui('span','muted'," (일부 표시 · 전체 내용은 원문 확인)",{}));
        fields.append(ui('dt','',label),dd);
      }
      panel.append(fields);
      if(m.provenance.survey_name?.method==='quoted_kosis_survey')panel.append(ui('p','evidence',"조사명은 KOSIS 원본 발행자 표기의 「조사명」에서 추출했어요.",{}));
      if(m.status==='missing_title')panel.append(ui('p','warning',"제목이 없어 의미 입력을 만들 수 없어요.",{}));
      if(m.truncated_fields.length)panel.append(ui('p','warning',"긴 메타데이터의 일부가 입력 한도를 넘었어요. 원문 확인이 필요해요.",{}));
    }catch(e){
      if(recordRequest!==request||e.name==='AbortError')return;
      const retry=ui('button','',"메타데이터 다시 확인",{});retry.type='button';retry.addEventListener('click',()=>loadMetadata(id,panel,request));
      panel.replaceChildren(ui('p','warning',"자료 설명을 불러오지 못했어요. 다른 정보는 계속 확인할 수 있어요.",{}),retry);
    }
  }
  async function initialize(){
    $('retry').hidden=true;status('온톨로지를 불러오는 중…');
    try{
      [schema,hierarchy]=await Promise.all([get('/api/ontology/schema'),get('/api/ontology/hierarchy')]);
      for(const node of hierarchy.nodes){const existing=schema.nodes.find(n=>n.id==='indicator:'+node.id);if(existing)continue;schema.nodes.push({id:'indicator:'+node.id,kind:'indicator',label:node.label,properties:{definition:node.definition,aliases:node.aliases,canonical_unit:node.measurement?.unit,temporal_meaning:node.measurement?.temporal_meaning}});}
      if(!selected||selected==='other'||!schema.nodes.some(n=>n.id==='indicator:'+selected))selected=assetConcept='population';$('summary').replaceChildren();
      for(const [label,value] of [['등록 자료',schema.summary.records],['카탈로그 분야',schema.summary.domains],['정의된 개념',schema.summary.concepts],['업무 객체 유형',schema.object_models.length],['분석가 해석 기록',schema.summary.asset_interpretations]]){const box=el('div','metric');box.append(el('strong','',fmt(value)),ui('span','',label));$('summary').append(box);}
      $('domain').replaceChildren(ui('option','',"전체 분야",{}));$('domain').firstElementChild.value='';
      const domains=schema.nodes.filter(n=>n.kind==='domain');if(domain&&!domains.some(n=>n.properties.id===domain))domain='';
      for(const d of domains){const option=ui('option','','{name} · {count}',()=>({name:d.label,count:fmt(d.properties.record_count)}));option.value=d.properties.id;$('domain').append(option);}$('domain').value=domain;
      status('');(window.DataieumI18n.unbind($('schema-scope')),$('schema-scope').textContent=schema.scope);$('countries').value=countries.join(', ');renderConceptList();renderConcept();renderModels();
      if(params.get('view')==='catalogue'){assetConcept='';search=(params.get('q')||'').slice(0,200);$('query').value=search;openCatalog();if(!params.get('record'))$('catalog-panel').scrollIntoView({block:'start'});}
      if(params.get('record'))await loadRecord(params.get('record'));
    }catch(e){showError($('status'),e);$('status').className='warning';$('retry').hidden=false;}
  }
  $('domain').addEventListener('change',()=>{domain=$('domain').value;assetConcept='';page=1;updateURL();renderConceptList();if($('catalog-panel').open)loadCatalog();});
  for(const [id,mode] of [['hierarchy-mode','hierarchy'],['related-mode','related']])$(id).addEventListener('click',()=>{closeInspector();graphMode=mode;renderConcept();});
  $('sites-retry').addEventListener('click',loadSites);
  $('close-inspector').addEventListener('click',()=>{closeInspector();focusCurrent();});
  $('catalog-panel').addEventListener('toggle',()=>{if($('catalog-panel').open&&schema)loadCatalog();});
  $('coverage-form').addEventListener('submit',e=>{e.preventDefault();const values=[...new Set($('countries').value.split(',').map(v=>v.trim()).filter(Boolean).map(v=>v==='한국'?'대한민국':v))];if(values.length>5||values.some(v=>v.length>60)){window.DataieumI18n.bind($('sites-status'),"국가는 최대 5개, 각 60자까지 입력해 주세요.",{});return;}cancelIntent();countries=values;if(searchContext){searchContext.countries=values;for(const n of searchContext.needs)n.scope=(n.scope||[]).filter(s=>s.field!=='countries');}$('countries').value=countries.join(', ');updateURL();loadSites();});
  $('intent-form').addEventListener('submit',e=>{e.preventDefault();submitIntent();});$('intent-cancel').addEventListener('click',()=>{cancelIntent();loadSites();});
  const reset=ui('button','',"조건 초기화",{});reset.type='button';reset.addEventListener('click',()=>{cancelIntent();closeInspector();searchContext=null;selectionExplicit=false;countries=[];$('countries').value='';$('intent-query').value='';window.DataieumI18n.bind($('intent-status'),"검색 조건을 초기화했어요.",{});$('intent-options').replaceChildren();updateURL();renderConcept();});$('intent-form').querySelector('.search-row').append(reset);
  $('all-records').addEventListener('click',()=>{assetConcept='';page=1;updateURL();loadCatalog();});
  $('browse-catalog').addEventListener('click',browseCatalog);
  $('concept-search').addEventListener('input',renderConceptList);
  $('catalog-search').addEventListener('submit',e=>{e.preventDefault();search=$('query').value.trim();page=1;loadCatalog();});
  $('previous').addEventListener('click',()=>{if(page>1){page--;loadCatalog();}});$('next').addEventListener('click',()=>{if(page<pages){page++;loadCatalog();}});
  $('retry').addEventListener('click',initialize);initialize();
})();
