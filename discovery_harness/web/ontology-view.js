/* Draw only real admitted graph paths. Text and properties never become HTML. */
window.OntologyView = (() => {
  let serial=0;
  const ns='http://www.w3.org/2000/svg';
  const provenance={user_intent:'사용자 요청 해석',curated_domain_rule:'분석가가 정의한 개념·업무 관계',
    curated_domain_definition:'분석가가 정의한 개념',official_documentation:'공식 문서에서 확인',
    catalog_metadata:'등록 카탈로그 정보',format_vocabulary:'형식 정의',access_vocabulary:'접근 방식 정의'};
  const names={definition:'정의',entity_type:'측정 대상',measure_kind:'측정 종류',aliases:'같은 의미의 표현',
    unit_dimension:'단위 차원',canonical_unit:'개념의 기준 단위',observed_unit:'실제 자료에서 확인한 단위',
    measurement_basis:'관측·추정·예보',temporal_meaning:'시간 의미',spatial_semantics:'공간 의미',
    not_equivalent_to:'동일시하면 안 되는 개념',comparability_requirements:'비교 전에 확인할 조건',
    countries:'자료 대상 국가',regions:'명시된 지역',years:'확인된 기준연도',dates:'확인된 날짜 범위',
    frequencies:'집계 주기',geography_levels:'지역 단위',formats:'표현 형식',delivery:'접근 방식',fields:'확인된 컬럼',
    subjects:'확인된 세부 대상',free:'무료 이용',commercial:'상업적 이용',sample_verified:'실제 파일·응답 표본 검증',
    description:'업무 정의',limitations:'해석 제한',reason:'분석에 필요한 이유',catalogue_country:'카탈로그의 사이트 국가',
    url:'공식 링크',checked_on:'문서 확인일',boundary_version:'경계 버전',identity_basis:'자료 식별 기준',
    reference_urls:'자료의 공식 안내',level:'근거/지역 유형',purpose:'요청 목적',role:'사이트 역할',name:'이름',
    catalog_id:'카탈로그 분류 ID',record_count:'전체 분류 자료 수',count_basis:'자료 수의 범위'};
  const values={count:'개수',rate:'비율',amount:'측정량',duration:'기간',position:'위치',category:'분야 분류',
    index:'지수',event:'사건',composite:'복합 측정',stock:'특정 시점의 상태',flow:'기간 동안의 흐름',
    static:'정적 정의',mixed:'지표마다 다름',observed:'관측·실측',estimated:'추정',forecast:'예보',simulation:'모의 계산',
    api:'API',download:'다운로드',official_documentation:'공식 안내 문서',country:'국가',region:'지역',registered_access_site:'자료 접근 사이트'};
  const svg=(name,attrs={},text)=>{const node=document.createElementNS(ns,name);for(const [key,value] of Object.entries(attrs))node.setAttribute(key,String(value));if(text!==undefined)node.textContent=text;return node;};
  const textValue=value=>value===null||value===undefined?'미확인':Array.isArray(value)?(value.length?value.map(x=>values[x]||x).join(' · '):'미확인 / 별도 명시 없음'):typeof value==='boolean'?(value?'예':'아니요'):values[value]||String(value);
  function render(parent,data,{el,safeURL}) {
    const graph=data.ontology;if(!graph||data.state==='clarify')return;
    const byId=new Map(graph.nodes.map(n=>[n.id,n]));
    const visible=new Set(graph.nodes.filter(n=>['request','purpose','analysis','indicator','domain'].includes(n.kind)).map(n=>n.id));
    for(const path of graph.paths){path.node_ids.forEach(id=>visible.add(id));visible.add(path.resource_id);visible.add(path.evidence_id);}
    const nodes=graph.nodes.filter(n=>visible.has(n.id));
    const edges=graph.edges.filter(e=>visible.has(e.source)&&visible.has(e.target));
    const relatedIds=new Set((graph.related_indicators||[]).map(item=>'indicator:'+item.indicator));
    const directIds=new Set(edges.filter(e=>e.relation==='requests_indicator').map(e=>e.target));
    const kindLabel=node=>node.kind==='indicator'?(relatedIds.has(node.id)?'함께 볼 관련 지표':directIds.has(node.id)?'직접 요청 지표':'업무에 필요한 지표'):(graph.object_types[node.kind]?.label||node.kind);
    const details=el('details','ontology-view');
    details.append(el('summary','ontology-summary','온톨로지 그래프 · 개념과 추천 근거 보기'));
    const intro=el('p','dataset-meta',graph.paths.length?'조건을 통과한 추천 경로예요. 개념이나 관계를 선택하면 의미와 근거를 볼 수 있어요.':'확인된 제공 경로가 없어 업무·데이터 개념 관계만 표시해요.');
    details.append(intro);
    if(relatedIds.size)details.append(el('p','dataset-meta','직접 요청 지표 아래에 함께 볼 관련 지표를 구분했어요. 관계가 있어도 같은 지표이거나 바로 결합할 수 있다는 뜻은 아니에요.'));
    if(graph.truncated)details.append(el('p','unverified','탐색 한도에 도달해 일부 관계를 확인하지 못했어요.'));
    const layout={request:0,purpose:1,analysis:2,indicator:3,assertion:5,dataset:6,provider:6,evidence:7};
    const rows=new Map();for(const n of nodes){const rank=relatedIds.has(n.id)?4:layout[n.kind]??6;if(!rows.has(rank))rows.set(rank,[]);rows.get(rank).push(n);}
    const ranks=[...rows.keys()].sort((a,b)=>a-b),maxColumns=Math.max(...[...rows.values()].map(xs=>xs.length));
    const width=Math.max(640,maxColumns*166+24),height=ranks.length*112+36,pos=new Map();
    ranks.forEach((rank,row)=>{const xs=rows.get(rank);xs.forEach((n,i)=>pos.set(n.id,{x:width/2+(i-(xs.length-1)/2)*166,y:28+row*112}));});
    const scroller=el('div','ontology-scroll');scroller.tabIndex=0;scroller.setAttribute('aria-label','관계 그래프. 내부에서 상하좌우로 이동할 수 있습니다.');
    const canvas=svg('svg',{width,height,viewBox:`0 0 ${width} ${height}`,role:'group','aria-label':'실제 추천에 사용한 온톨로지 관계'});
    const defs=svg('defs'),arrowId='ontology-arrow-'+(++serial),marker=svg('marker',{id:arrowId,viewBox:'0 0 10 10',refX:9,refY:5,markerWidth:5,markerHeight:5,orient:'auto-start-reverse'});
    marker.append(svg('path',{d:'M 0 0 L 10 5 L 0 10 z',class:'ontology-arrow'}));defs.append(marker);canvas.append(defs);
    const inspector=el('section','ontology-inspector');inspector.setAttribute('aria-live','polite');inspector.append(el('p','dataset-meta','개념을 선택하면 정의·단위·지역과 시간 기준을 확인할 수 있어요.'));
    const property=(list,label,value)=>{list.append(el('dt','',label),el('dd','',textValue(value)));};
    const showNode=node=>{
      inspector.replaceChildren(el('h3','',node.label),el('p','dataset-meta',kindLabel(node)),
        el('p','dataset-meta',provenance[node.provenance?.basis]||'근거 유형 미확인'));
      const list=el('dl','ontology-properties');
      for(const [key,value] of Object.entries(node.properties)){
        if(!names[key])continue;
        if(['url','reference_urls'].includes(key)){
          const dt=el('dt','',names[key]),dd=el('dd');
          for(const item of Array.isArray(value)?value:[value]){const url=safeURL(item);if(url){const a=el('a','','공식 원문 ↗');a.href=url;a.target='_blank';a.rel='noopener noreferrer';dd.append(a,el('br'));}}
          list.append(dt,dd);continue;
        }
        property(list,names[key],key==='not_equivalent_to'?value.map(id=>byId.get('indicator:'+id)?.label||id):
          key==='sample_verified'&&!value?'미검증':value);
      }
      inspector.append(list);
      if(node.kind==='indicator')inspector.append(el('p','dataset-meta','개념의 기준 단위는 실제 제공 자료의 단위를 검증했다는 뜻이 아니에요.'));
      if(node.kind==='provider')inspector.append(el('p','dataset-meta','이 링크는 접근 사이트예요. 자료 생산기관이나 대상 국가를 뜻하지 않아요.'));
    };
    const showEdge=edge=>{
      const spec=graph.link_types[edge.relation];inspector.replaceChildren(el('h3','',edge.label),
        el('p','answer',`${byId.get(edge.source).label} → ${byId.get(edge.target).label}`),
        el('p','answer',spec.definition),el('p','answer',edge.reason),
        el('p','dataset-meta',provenance[edge.basis]||edge.basis),
        el('p','dataset-meta',`관계 제약: ${graph.object_types[spec.domain]?.label||spec.domain} → ${graph.object_types[spec.range]?.label||spec.range} · 연결 대상 ${spec.min_targets}~${spec.max_targets}개 · 자동 전이 없음`));
      const references=el('p','dataset-meta');let count=0;
      for(const reference of edge.references||[]){
        const url=safeURL(reference);if(!url)continue;
        const anchor=el('a','','관계 검토 자료 '+(++count)+' ↗');anchor.href=url;anchor.target='_blank';anchor.rel='noopener noreferrer';references.append(anchor,el('br'));
      }
      if(count)inspector.append(references);
    };
    const activate=(node,callback)=>{node.addEventListener('click',callback);node.addEventListener('keydown',e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();callback();}});};
    for(const edge of edges){
      const from=pos.get(edge.source),to=pos.get(edge.target);if(!from||!to)continue;
      const sy=from.y+52,ty=to.y,mid=(sy+ty)/2;
      canvas.append(svg('path',{d:`M ${from.x} ${sy} C ${from.x} ${mid}, ${to.x} ${mid}, ${to.x} ${ty}`,class:'ontology-edge','marker-end':`url(#${arrowId})`}));
      const label=svg('text',{x:(from.x+to.x)/2,y:mid-3,'text-anchor':'middle',class:'ontology-edge-label',role:'button',tabindex:0,'aria-label':edge.label+' 관계 설명'},edge.label);
      activate(label,()=>showEdge(edge));canvas.append(label);
    }
    for(const node of nodes){
      const point=pos.get(node.id),group=svg('g',{transform:`translate(${point.x-74} ${point.y})`,class:'ontology-node '+node.kind,
        role:'button',tabindex:0,'aria-label':node.label+' 개념 상세'});
      group.append(svg('rect',{width:148,height:52,rx:6}));
      const label=node.label.length>17?node.label.slice(0,16)+'…':node.label;
      group.append(svg('title',{},node.label),svg('text',{x:74,y:21,'text-anchor':'middle',class:'ontology-node-label'},label),
        svg('text',{x:74,y:40,'text-anchor':'middle',class:'ontology-node-kind'},kindLabel(node)));
      activate(group,()=>{canvas.querySelectorAll('.ontology-node').forEach(n=>n.classList.remove('selected'));group.classList.add('selected');showNode(node);});canvas.append(group);
    }
    scroller.append(canvas);details.append(scroller,el('p','dataset-meta','그래프 안에서 상하좌우로 이동할 수 있어요.'),inspector);
    const comparison=el('details','ontology-comparability');comparison.append(el('summary','','이 자료들을 바로 결합할 수 있나요?'),el('p','answer',graph.comparability.message));
    const requirements=el('ul');for(const condition of graph.comparability.requirements)requirements.append(el('li','',condition));comparison.append(requirements);details.append(comparison);
    parent.append(details);
  }
  return {render};
})();
