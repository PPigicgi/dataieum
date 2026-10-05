/* Show server-accepted sites and evidence; keep retrieval scores internal. */
(() => {
const i18n = window.DataieumI18n;
const bindings = new Map();
let pruneScheduled = false;
const watch = (node, render) => {
  bindings.set(node, render); render();
  if (!pruneScheduled) {
    pruneScheduled = true;
    setTimeout(() => {
      pruneScheduled = false;
      for (const element of bindings.keys()) if (element.isConnected === false) bindings.delete(element);
    }, 0);
  }
  return node;
};
i18n?.subscribe(() => { for (const [node, render] of bindings) { if (node.isConnected === false) bindings.delete(node); else render(); } });
const field = (node, type, id, key, original) => watch(node, () => {
  const value = i18n?.field(type, id, key, original) || {text: original, translated: false, status: 'original'};
  node.textContent = value.text; node.dataset.translationStatus = value.status;
  if (value.translated) node.setAttribute('title', original); else node.removeAttribute?.('title');
});
const copy = (source, params = {}) => ({source, params});
const text = (source,params={}) => i18n?.text(source,params) || source.replace(/\{(\w+)\}/g,(match,key)=>params[key]??match);
function metadataInfo(el, card, fields, showNotice = true) {
  const rows = fields.filter(item => item.original);
  const notice = el('p', 'dataset-meta metadata-language-note');
  const original = el('details', 'metadata-original');
  if(!showNotice)original.open=true;
  original.append(el('summary', '', copy('원문 보기')));
  const entries = rows.map(item => {
    const row = el('p', 'metadata-original-field');
    row.append(el('strong', '', copy(item.label)), el('span', '', ' · ' + item.original));
    original.append(row); return {item, row};
  });
  if(showNotice)card.append(notice);
  card.append(original);
  watch(showNotice ? notice : original, () => {
    let datasetOriginal = 0, datasetTranslated = 0, translated = 0;
    for (const {item, row} of entries) {
      const hasTranslation = i18n?.field(item.type, item.id, item.key, item.original)?.translated === true;
      row.hidden = !hasTranslation;
      if (hasTranslation) translated++;
      if (item.type === 'dataset') hasTranslation ? datasetTranslated++ : datasetOriginal++;
    }
    original.hidden = translated === 0;
    notice.hidden = datasetOriginal === 0;
    notice.textContent = text(datasetTranslated ? '번역되지 않은 항목은 제공처 원문으로 표시돼요.' : '자료의 제목과 설명은 제공처 원문으로 표시돼요.');
  });
}
window.SiteResults = {
  renderRelated(container,data,{el,safeURL}) {
    if(data?.kind!=='related'||!Array.isArray(data.items)||data.items.length>3)throw new Error('invalid related result');
    container.replaceChildren();container.setAttribute('aria-busy','false');
    container.hidden=!data.items.length;
    if(container.hidden)return 0;
    const heading=el('h3','site-results-section-heading');
    heading.setAttribute('tabindex','-1');
    watch(heading,()=>{heading.textContent=text('이런 자료는 어떨까요?');});container.append(heading);
    const list=el('ul','site-results-list related-results-list');container.append(list);
    let count=0;
    for(const item of data.items){
      const site=this.selectSites(item.result)[0],fact=site?.matches?.[0]?.evidence?.[0];
      if(!fact)continue;
      const card=el('li','site-result'),label=el('p','related-concept-label');
      watch(label,()=>{label.textContent=text(item.label);});card.append(label);
      const title=el('h4','site-result-title'),url=safeURL(fact.evidence_url),link=el(url?'a':'span','');
      field(link,'dataset',fact.dataset_id,'title',fact.title);
      if(url){link.href=url;link.target='_blank';link.rel='noopener noreferrer';}
      title.append(link);card.append(title);
      card.append(field(el('p','site-result-provider'),'source',site.source_id,'name',site.name));
      list.append(card);
      window.DataieumCooperation?.bind(card,{datasetId:fact.dataset_id,hints:{formats:fact.formats||[]}});
      count++;
      window.DataieumIntros?.bind(card,{id:fact.dataset_id,title:fact.title,description:''},link,null);
    }
    container.hidden=count===0;return count;
  },
  selectSites(data) {
    if(data.state==='clarify')return [];
    const judged=['relevance_then_cosine','relevance_then_hybrid_rrf'].includes(data.retrieval?.selection?.method);
    const sites=new Map(),candidates=[];
    const score=fact=>Number.isFinite(fact.rrf_score)&&fact.rrf_score>0&&fact.rrf_score<=2/61?fact.rrf_score:Number.isFinite(fact.cosine)&&Math.abs(fact.cosine)<=1?fact.cosine:-Infinity;
    const key=(site,fact)=>fact.dataset_id||site.source_id+'|'+fact.evidence_url+'|'+fact.title;
    for(const group of (data.groups||[]).slice(0,5))for(const site of (group.sites||[]).slice(0,40)){
      for(const fact of (site.evidence||[]).slice(0,40))if(site.source_id&&fact?.title&&(!judged||['direct','related'].includes(fact.relevance_tier)))candidates.push({site,fact,group,key:key(site,fact)});
    }
    candidates.sort((a,b)=>(judged?(a.fact.relevance_tier==='direct'?0:1)-(b.fact.relevance_tier==='direct'?0:1):0)||score(b.fact)-score(a.fact));
    if(judged){
      const seen=new Set(),result=[];
      for(const {site,fact,group,key:identity} of candidates){
        if(seen.has(identity))continue;seen.add(identity);
        result.push({source_id:site.source_id,name:site.name,url:site.url,tier:fact.relevance_tier,
          matches:[{indicator:group.indicator,name:group.name,evidence:[fact]}]});
        if(result.length===10)break;
      }
      return result;
    }
    const accepted=new Set();for(const item of candidates){if(accepted.size===10)break;accepted.add(item.key);}
    for(const group of (data.groups||[]).slice(0,5)){
      for(const site of (group.sites||[]).slice(0,40)){
        const evidence=(site.evidence||[]).slice(0,40).filter(fact=>fact&&accepted.has(key(site,fact))).sort((a,b)=>score(b)-score(a));
        if(!site.source_id||!evidence.length)continue;
        let card=sites.get(site.source_id);
        if(!card){card={source_id:site.source_id,name:site.name,url:site.url,matches:[],order:sites.size};sites.set(site.source_id,card);}
        if(!card.matches.some(m=>m.indicator===group.indicator))card.matches.push({indicator:group.indicator,name:group.name,evidence});
      }
    }
    const siteScore=site=>Math.max(-Infinity,...site.matches.flatMap(m=>m.evidence).map(score));
    return [...sites.values()].sort((a,b)=>siteScore(b)-siteScore(a)||b.matches.length-a.matches.length||a.order-b.order).slice(0,10);
  },
  render(answer,data,{el,safeURL,submit,selectContext}) {
    const rawEl=el;
    el=(tag, cls, value)=>{
      const node=rawEl(tag,cls);
      if(value && typeof value==='object') {
        if(i18n)i18n.bind(node,value.source,value.params);
        else {const params=typeof value.params==='function'?value.params():value.params;node.textContent=value.source.replace(/\{(\w+)\}/g,(match,key)=>params[key]??match);}
      } else if(value!==undefined)node.textContent=value;
      return node;
    };
    const plan=data.plan;
    const judged=['relevance_then_cosine','relevance_then_hybrid_rrf'].includes(data.retrieval?.selection?.method);
    const semanticEmpty=judged&&data.retrieval.selection.empty_reason==='meaning_unverified';
    if(!plan||!Array.isArray(data.groups)||!Array.isArray(plan.needs))throw new Error('invalid site result');
    answer.replaceChildren();selectContext?.(plan);
    const link=(label,value,className='')=>{const url=safeURL(value),node=el(url?'a':'span',className,label);if(url){node.href=url;node.target='_blank';node.rel='noopener noreferrer';}return node;};
    const scopedConditions=()=>[...new Set((plan.needs.length?plan.needs:[{}]).map(need=>{
      const scope={countries:plan.countries,regions:plan.regions,years:plan.years,years_mode:plan.years_mode};
      for(const entry of need.scope||[])if(['countries','regions','years'].includes(entry.field)){
        scope[entry.field]=entry.values||[];if(entry.field==='years')scope.years_mode=entry.mode;
      }
      return [scope.countries.length?scope.countries.map(v=>text(v)).join(' · '):text('여러 국가'),
        ...scope.regions.map(v=>text(v)),scope.years.length?text('{years}년',{years:scope.years.join(scope.years_mode==='range'?'~':', ')}):''].filter(Boolean).join(' · ');
    }))].join(' / ');
    const conditions=()=>[scopedConditions(),plan.dates.join('~'),plan.geography_level,
      plan.frequency,plan.formats.join(' / '),plan.delivery==='api'?'API':plan.delivery==='download'?text('다운로드'):'',
      plan.free_only?text('무료'):'',plan.commercial_only?text('상업적 이용'):''].filter(Boolean).join(' · ');
    const scopeLine=el('p','dataset-meta search-scope');answer.append(scopeLine);
    watch(scopeLine,()=>{scopeLine.textContent=(judged||['cosine_ranked','hybrid_rrf'].includes(data.retrieval?.selection?.method)?text('요청 조건: '):'')+conditions();});
    if(data.state==='clarify'){answer.append(el('p','answer',data.question));return;}
    const sites=this.selectSites(data);
    const factKey=(site,fact)=>fact.dataset_id||site.source_id+'|'+fact.evidence_url+'|'+fact.title;
    const datasetCount=new Set(sites.flatMap(site=>site.matches.flatMap(match=>match.evidence.map(fact=>factKey(site,fact))))).size;
    const distinctSummary=fact=>fact.summary&&fact.summary.trim().replace(/\s+/g,' ')!==fact.title.trim().replace(/\s+/g,' ');
    answer.append(el('h2','site-results-heading', sites.length?copy('자료 {datasets}개 · 제공 사이트 {sites}곳',{datasets:datasetCount,sites:new Set(sites.map(s=>s.source_id)).size}):copy(semanticEmpty?'요청한 내용에 맞는 자료를 아직 확인하지 못했어요.':'조건에 맞는 사이트를 아직 확인하지 못했어요.')));
    if(sites.length){
      const toolbar=el('div','site-results-toolbar');answer.append(toolbar);
      answer.append(el('p','dataset-meta results-language-note',copy('번역이 없는 자료 제목은 원문으로 표시돼요.')));
      let list,activeTier;
      if(judged&&!sites.some(site=>site.tier==='direct'))answer.append(el('p','answer',copy('요청에 직접 맞는 자료는 아직 확인하지 못했어요. 아래는 함께 살펴볼 수 있는 자료예요.')));
      for(const site of sites){
        const tier=site.tier||'legacy';
        if(tier!==activeTier){
          activeTier=tier;
          const label=tier==='direct'?'요청에 맞는 자료':tier==='related'?'함께 볼 자료':'제공 사이트별 관련 자료';
          if(judged)answer.append(el('h3','site-results-section-heading',copy(label)));
          list=el('ol','site-results-list');i18n?i18n.bind(list,label,{},'aria-label'):list.setAttribute('aria-label',label);
          if(judged&&tier==='related')list.setAttribute('start',String(1+sites.filter(s=>s.tier==='direct').length));
          answer.append(list);
        }
        const facts=site.matches.flatMap(m=>m.evidence).sort((a,b)=>(Number.isFinite(b.cosine)?b.cosine:-Infinity)-(Number.isFinite(a.cosine)?a.cosine:-Infinity)),primary=facts[0],formats=[...new Set(primary.formats||[])];
        const card=el('li','site-result'),title=el('h3','site-result-title');title.append(field(link(primary.title,primary.evidence_url),'dataset',primary.dataset_id,'title',primary.title));card.append(title);
        const meta=el('div','site-result-meta');
        meta.append(field(el('p','site-result-provider'),'source',site.source_id,'name',site.name));
        meta.append(el('p','site-result-format',copy('데이터 형식: {formats}',()=>({formats:formats.length?formats.join(' · '):text('미확인')}))));
        card.append(meta);
        const secondary=el('details','site-result-secondary');secondary.append(el('summary','',copy('원문·출처 보기')));
        metadataInfo(el,secondary,[{type:'dataset',id:primary.dataset_id,key:'title',original:window.DataieumIntros?'':primary.title,label:'제목'},
          {type:'dataset',id:primary.dataset_id,key:'description',original:distinctSummary(primary)?primary.summary:'',label:'설명'},
          {type:'source',id:site.source_id,key:'name',original:site.name,label:'제공 사이트'}],false);
        secondary.append(link(copy('사이트 홈 ↗'),site.url));
        const missingLabels=fact=>[...new Set((fact.unverified_conditions||[]).map(token=>({countries:'대상 국가',regions:'지역',years:'대상 연도',years_range:'대상 기간',dates:'날짜',formats:'형식',formats_any:'형식',frequency:'시간 단위',fields:'컬럼',delivery:'제공 방식',free_only:'무료 이용',commercial_only:'상업적 이용',geography_level:'지역 단위',unit:'측정 단위',measurement_basis:'측정 방식',additional_requirements:'추가 조건'})[String(token).split(':')[0]]||'조건'))];
        const missing=missingLabels(primary);if(missing.length)card.append(el('p','site-result-format',copy('{conditions} 미확인',()=>({conditions:missing.map(text).join(' · ')}))));
        const actions=el('div','site-result-actions');actions.append(link(copy('자료 페이지 열기 ↗'),primary.evidence_url,'site-result-primary'),secondary);card.append(actions);
        window.DataieumCooperation?.bind(actions,{datasetId:primary.dataset_id,hints:window.DataieumCooperation.searchHints(plan,site.matches.find(m=>m.evidence.includes(primary))?.indicator,primary)});
        const siteDatasetCount=new Set(facts.map(fact=>factKey(site,fact))).size;
        const evidence=el('details','site-result-evidence');evidence.append(el('summary','',copy('이 사이트의 자료 {count}개 · 세부 정보',{count:siteDatasetCount})));
        for(const match of site.matches){
          const section=el('section');const heading=el('h4','');watch(heading,()=>{heading.textContent=i18n?.topic(match.indicator,match.name)||match.name;});section.append(heading);
          for(const fact of match.evidence){
            const item=el('div','site-result-fact');const title=field(link(fact.title,fact.evidence_url),'dataset',fact.dataset_id,'title',fact.title);item.append(title);
            if(distinctSummary(fact))item.append(field(el('p',''),'dataset',fact.dataset_id,'description',fact.summary));
            metadataInfo(el,item,[{type:'dataset',id:fact.dataset_id,key:'title',original:fact.title,label:'제목'},
              {type:'dataset',id:fact.dataset_id,key:'description',original:distinctSummary(fact)?fact.summary:'',label:'설명'}]);
            if(fact.formats?.length)item.append(el('p','',copy('데이터 형식: {formats}',{formats:[...new Set(fact.formats)].join(' · ')})));
            const missing=missingLabels(fact);if(missing.length)item.append(el('p','',copy('{conditions} 미확인',()=>({conditions:missing.map(text).join(' · ')}))));
            if(fact.countries?.length)item.append(el('p','',copy('확인된 대상 국가: {countries}',{countries:fact.countries.join(' · ')})));
            if(fact.access_note)item.append(el('p','',fact.access_note));
            if(fact.checked_on)item.append(el('p','dataset-meta',copy(data.retrieval?'메타데이터 수집일: {date}':'제공 근거 확인일: {date}',{date:fact.checked_on})));
            section.append(item);
            window.DataieumCooperation?.bind(item,{datasetId:fact.dataset_id,hints:window.DataieumCooperation.searchHints(plan,match.indicator,fact)});
            window.DataieumIntros?.bind(item,{id:fact.dataset_id,title:fact.title,description:distinctSummary(fact)?fact.summary:''},title,distinctSummary(fact)?item.querySelector('p'):null);
          }evidence.append(section);
        }if(!judged)card.append(evidence);list.append(card);
        window.DataieumIntros?.bind(card,{id:primary.dataset_id,title:primary.title,description:distinctSummary(primary)?primary.summary:''},title.querySelector('a'),card.querySelector('.site-result-description'));
      }
    }
    const gaps=[...new Set(data.groups.flatMap(g=>(g.unverified||[]).map(gap=>(g.indicator==='other'?'요청한 자료':g.name)+': '+gap)))];
    if(data.retrieval?.selection?.coverage_gaps?.length){
      answer.append(el('p','unverified',copy('아직 확인하지 못한 조건 — {conditions}',()=>({conditions:data.retrieval.selection.coverage_gaps.map(g=>
        [...g.countries.map(v=>text(v)),...g.regions.map(v=>text(v)),g.years.length?text('{years}년',{years:g.years.join(g.years_mode==='range'?'~':', ')}):''].filter(Boolean).join(' · ')).join(' / ')}))));
    }else if(gaps.length)answer.append(el('p','unverified',copy('아직 확인하지 못한 조건 — {conditions}',{conditions:gaps.join(' / ')})));
    if(!sites.length){
      answer.append(el('p','answer', copy(semanticEmpty?'검색 후보는 있었지만, 요청한 내용과 관련성을 뒷받침하는 근거가 부족했어요.':'현재 등록된 제공 정보만으로 요청 조건을 확인하지 못했어요. 실제 자료가 없다는 뜻은 아니에요.')));
      answer.append(el('p','answer', copy(semanticEmpty?'찾는 지표나 자료 종류를 더 구체적으로 적거나 카탈로그에서 직접 찾아보세요.':'조건을 바꾸거나 카탈로그에서 자료 이름으로 직접 찾아볼 수 있어요.')));
    }
    if(!sites.length || data.state==='partial'){
      const criteria=el('details','site-results-more');criteria.append(el('summary','', copy('어떤 기준으로 확인하나요?')));
      criteria.append(el('p','', copy(['cosine_ranked','hybrid_rrf'].includes(data.retrieval?.selection?.method) ? '질문과 관련된 자료를 찾고, 지정한 국가·지역·기간·형식 등의 조건을 적용해요.' : data.retrieval ? '질문과 의미가 가까운 자료를 찾고, Agent가 등록된 제목·설명·분류에서 요청한 자료와 조건의 근거를 확인해요. 근거가 부족한 후보는 제외해요.' :
        '요청한 종류의 자료를 제공한다는 공식 근거가 있고, 지정한 국가·지역·기간·형식 등의 조건을 확인할 수 있는 사이트를 보여드려요.')));
      criteria.append(el('p','', copy(data.retrieval?.selection?.coverage_policy ? '지정한 국가·지역·대상 연도를 확인할 수 없는 자료는 제외해요. 다른 조건은 정보가 없으면 미확인으로 표시해요.' : ['cosine_ranked','hybrid_rrf'].includes(data.retrieval?.selection?.method) ? '조건과 다르다고 명시된 자료는 제외하고, 정보가 없는 조건은 미확인으로 표시해요.' : '조건에 맞지 않는 경우뿐 아니라, 제공 정보에 해당 조건이 기록되어 있지 않은 경우에도 제외될 수 있어요. 인터넷 전체를 실시간으로 검색한 결과는 아니에요.')));
      const scope=data.evidence_scope;
      if(Number.isInteger(scope?.sites)&&Number.isInteger(scope?.registered_sites))criteria.append(el('p','dataset-meta', data.retrieval ?
        copy('등록된 {registered}개 사이트 중 이번 검색 후보에 포함된 {sites}곳의 메타데이터를 검토했어요.',{registered:scope.registered_sites,sites:scope.sites}) :
        copy('현재 등록된 {registered}개 사이트 중 제공 근거가 정리된 {sites}곳의 정보를 사용해요.',{registered:scope.registered_sites,sites:scope.sites})));
      answer.append(criteria);
    }
    if(data.relaxation){const button=el('button','relaxation',copy(data.relaxation.label));
      if(data.relaxation.need_indicator)watch(button,()=>{
        const id=data.relaxation.need_indicator;
        const name=window.DataieumLocaleData?.topics?.[id]?.original?.name||data.groups.find(group=>group.indicator===id)?.name;
        const prefix=name&&name!==id?(i18n?.topic(id,name)||name)+': ':'';
        button.textContent=prefix+text(data.relaxation.label);
      });
      button.type='button';button.addEventListener('click',()=>submit(data.relaxation.query,plan,null,button.textContent));answer.append(button);}
    const more=el('details','site-results-more');more.append(el('summary','', copy('다른 자료 직접 찾기')));
    const browse=el('a','', copy('카탈로그에서 검색'));browse.href='/catalogue';more.append(browse);answer.append(more);
    if(data.ontology?.truncated)answer.append(el('p','unverified', copy('탐색 한도 안에서 확인된 결과예요. 일부 제공 근거는 확인하지 못했어요.')));
  }
};

})();
