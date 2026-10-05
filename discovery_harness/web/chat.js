(() => {
  'use strict';
  const i18n = window.DataieumI18n;
  const copy = (source, params = {}) => ({source, params});
  const ui = (node, value, attribute = null) => {
    const {source, params} = typeof value === 'object' ? value : copy(value);
    if (i18n) i18n.bind(node, source, params, attribute);
    else {
      const values = typeof params === 'function' ? params() : params;
      const text = source.replace(/\{(\w+)\}/g, (match, key) => values[key] ?? match);
      if (attribute) node.setAttribute(attribute, text); else node.textContent = text;
    }
    return node;
  };
  const $ = (id) => document.getElementById(id);
  const messages = $('messages'), input = $('question'), status = $('status');
  ui(status, '국가를 적으면 그 범위로, 생략하면 여러 나라 자료를 찾아요.');
  ui($('connection-status'), '연결 확인 중');
  ui($('chat-panel-title'), '대화와 검색 결과');
  ui($('stop'), '■ 중지');
  const embeddedChat = document.body.dataset.embeddedChat === 'true';
  const topicGraph = embeddedChat ? null : window.TopicGraph?.mount();
  let embeddedPhase = null, embeddedExpanded = false, lastResult = null, conceptDetail = null;
  function notifyParent(action, detail = {}) {
    if (embeddedChat && window.parent !== window) {
      window.parent?.postMessage({type: 'dataieum-chat', version: 1, action, ...detail}, location.origin);
    }
  }
  function setEmbeddedPhase(phase, announce = false) {
    const changed = embeddedPhase !== phase;
    embeddedPhase = phase;
    document.body.dataset.chatPhase = phase;
    $('chat-panel').hidden = phase !== 'conversation' && !embeddedExpanded;
    if (changed || announce) notifyParent('phase', {phase});
  }
  function setEmbeddedLayout(expanded) {
    embeddedExpanded = expanded;
    document.body.dataset.chatExpanded = String(expanded);
    $('chat-panel').hidden = embeddedPhase !== 'conversation' && !expanded;
    const resize = $('resize-chat-panel');
    ui(resize, expanded ? '작게 보기' : '크게 보기');
    const label = expanded ? '채팅 원래 크기로' : '채팅 크게 보기';
    ui(resize, label, 'title'); ui(resize, label, 'aria-label');
    resize.setAttribute('aria-pressed', String(expanded));
  }
  if (embeddedChat) {
    document.querySelector('.topbar').hidden = true;
    $('graph-stage').hidden = true;
    $('close-chat-panel').hidden = false;
    const heading = $('chat-panel').querySelector('.panel-heading');
    heading.className += ' embedded-chat-heading';
    const actions = document.createElement('div');
    actions.className = 'embedded-chat-actions';
    const resize = document.createElement('button');
    resize.id = 'resize-chat-panel';resize.type = 'button';resize.className = 'quiet-button';
    resize.addEventListener('click', () => notifyParent('toggle-size'));
    actions.append($('connection-status'), $('new-chat'), resize, $('close-chat-panel'));
    heading.append(actions);
    document.querySelector('main').prepend(heading);
    const placeLanguageControls = () => {
      const controls = document.querySelector('.dataieum-language');
      if (controls) heading.append(controls);
    };
    placeLanguageControls();
    document.addEventListener('DOMContentLoaded', placeLanguageControls, {once: true});
    ui($('chat-panel-title'), '데이터 채팅');
    ui($('chat-panel-title'), '더블클릭하면 큰 화면 · 다시 더블클릭하면 원래 크기', 'title');
    heading.addEventListener('dblclick', event => {
      if (event.target.closest('button')) return;
      event.preventDefault();notifyParent('toggle-size');
    });
    setEmbeddedLayout(false);
    setEmbeddedPhase('composer');
    document.addEventListener('keydown', event => {
      if (event.key === 'Escape' && !event.isComposing) { event.preventDefault(); notifyParent(embeddedExpanded ? 'toggle-size' : 'close'); }
    });
    document.addEventListener('click', event => {
      const link = event.target.closest('a');
      const href = link?.getAttribute('href');
      if (href && !href.startsWith('#') && !link.target) {
        link.target = '_blank'; link.rel = 'noopener noreferrer';
      }
      if (link?.closest('.site-result-title') || link?.matches('.site-result-primary')) notifyParent('official-opened');
    });
  }
  function showChatPanel(open, restoreFocus = false) {
    if (embeddedChat) { setEmbeddedPhase(open ? 'conversation' : 'composer'); return; }
    $('chat-panel').hidden = !open;
    $('chat-toggle').setAttribute('aria-expanded', String(open));
    if (open && window.matchMedia('(max-width: 767px)').matches) topicGraph?.closeInspector();
    if (restoreFocus) (embeddedChat ? input : $('chat-toggle')).focus({preventScroll: true});
  }
  $('chat-toggle').addEventListener('click', () => showChatPanel($('chat-panel').hidden));
  $('close-chat-panel').addEventListener('click', () => embeddedChat ? notifyParent('close') : showChatPanel(false, true));
  $('chat-panel').addEventListener('keydown', event => { if (!embeddedChat && event.key === 'Escape') showChatPanel(false, true); });
  window.addEventListener('topic-inspector-open', () => {
    if (window.matchMedia('(max-width: 767px)').matches) showChatPanel(false);
  });
  const sizeComposer = () => {
    const compact = messages.children.length > 0 && !input.value && document.activeElement !== input;
    document.body.dataset.composerState = compact ? 'compact' : 'expanded';
    input.style.height = 'auto';
    input.style.height = compact ? '24px' : Math.min(112, input.scrollHeight) + 'px';
  };
  input.addEventListener('input', sizeComposer);
  input.addEventListener('focus', sizeComposer);
  input.addEventListener('blur', sizeComposer);
  const fitViewport = () => {
    const height = window.visualViewport?.height || window.innerHeight;
    document.documentElement.style.setProperty('--app-height', Math.round(height) + 'px');
    document.body.classList.toggle('compact-viewport', height < 550);
  };
  window.visualViewport?.addEventListener('resize', fitViewport);
  window.visualViewport?.addEventListener('scroll', fitViewport);
  window.addEventListener('resize', fitViewport);
  fitViewport();
  if (typeof ResizeObserver === 'function') {
    new ResizeObserver(entries => {
      document.documentElement.style.setProperty('--composer-height', Math.ceil(entries[0].target.getBoundingClientRect().height) + 12 + 'px');
    }).observe(document.querySelector('.composer-area'));
  }
  const initialQuery=new URLSearchParams(location.search).get('q');
  if(initialQuery && new TextEncoder().encode(initialQuery).length<=2048)input.value=initialQuery;
  sizeComposer();
  const names = new Map(), sources = new Map(), sourceCountries = new Map();
  let controller = null;
  let monitorPromise = null, pendingJob = null, jobAnswer = null, navigating = false, restorationIssue = false;
  const storageKey = 'dataieum.pending-chat.v1';
  const terminalStates = new Set(['completed', 'failed', 'cancelled', 'expired']);
  let searchContext = null;
  let checkingConnection = false;
  let connectionState = {state: 'checking', ready: false};
  function renderConnection() {
    const value = connectionState;
    // Service-wide busy is still ready; only this tab's unfinished job is processing.
    const available = value.ready === true && ['ready', 'busy'].includes(value.state);
    ui($('connection-status'), available ? (pendingJob ? '검색 처리 중' : '검색 준비됨') :
      value.state === 'embedding_not_configured' ? '검색 연결 설정 필요' :
      value.state === 'authentication_required' ? '검색 연결 복구 필요' :
      value.state === 'vector_preparing' ? '검색 자료 준비 중' :
      value.state === 'service_recovering' ? '검색 서비스 복구 중' :
      value.state === 'catalog_preparing' ? '자료 준비 중' :
      value.state === 'checking' ? '연결 확인 중' : '연결 끊김');
  }
  async function refreshConnection() {
    if (checkingConnection) return;
    checkingConnection = true;
    try {
      const response = await fetch('/api/chat/status', {cache: 'no-store', signal: AbortSignal.timeout(3500)});
      if (!response.ok) throw new Error('offline');
      const value = await response.json();
      connectionState = value; renderConnection();
      if (!pendingJob && !restorationIssue && !messages.children.length) {
        ui(status, value.ready ? '국가를 적으면 그 범위로, 생략하면 여러 나라 자료를 찾아요.' :
          value.state === 'service_recovering' ? '검색 서비스 상태를 확인하고 있어요. 잠시 후 연결 상태를 다시 확인해 주세요.' :
          '채팅 서비스에 아직 연결되지 않았어요. 오른쪽 위에서 연결 상태를 다시 확인할 수 있어요.');
      }
    } catch {
      connectionState = {state: 'offline', ready: false}; renderConnection();
    } finally { checkingConnection = false; }
  }
  const number = new Intl.NumberFormat('ko-KR');
  const el = (tag, className, text) => {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) { if (text && typeof text === 'object') ui(node, text); else node.textContent = text; }
    return node;
  };
  const scrollToEnd = () => { $('conversation').scrollTop = $('conversation').scrollHeight; };
  function busy(value) {
    renderConnection();
    $('send').hidden = value; $('stop').hidden = !value;
    $('new-chat').disabled = value;
    document.querySelectorAll('[data-prompt], .retry, .relaxation, .concept-map-node, .hierarchy-node, .graph-modes button').forEach(b => { b.disabled = value; });
    document.querySelectorAll('.inspect-search').forEach(button => { button.disabled = value || button.dataset.supported !== 'true'; });
    messages.setAttribute('aria-busy', String(value));
    $('stop').disabled = !!controller && !!pendingJob?.cancel_requested;
    ui($('stop'), pendingJob?.cancel_requested ? '취소 상태 확인' : '■ 중지');
  }
  function safeURL(value) {
    try {
      const url = new URL(value);
      return ['https:', 'http:'].includes(url.protocol) && !url.username && !url.password ? url.href : null;
    } catch { return null; }
  }
  function renderResults(answer, data) {
    if (data.version === 2) {
      window.SiteResults.render(answer, data, {el, safeURL, submit, selectContext:value=>{searchContext=value;}});
      searchContext = data.plan;
      topicGraph?.showResult(data);
      if (embeddedChat) publishResult(data);
      return;
    }
    if (!Number.isInteger(data.total) || !Array.isArray(data.datasets) ||
        !Array.isArray(data.scope?.countries) || data.scope.basis !== 'provider_country') throw new Error('invalid');
    answer.replaceChildren();
    const name = names.get(data.concept_id) || (data.concept_id === 'unknown' ? '주제 미확인' : data.concept_id);
    const countries = data.scope.countries;
    answer.append(el('p', 'dataset-meta', copy('제공 국가: {countries} / 주제: {topic}', () => ({countries: countries.length ? countries.join(' · ') : i18n?.text('전체 · 여러 나라') || '전체 · 여러 나라', topic: name}))));
    if (!data.total || !data.datasets.length || data.concept_id === 'unknown') {
      const message = data.concept_id === 'unknown' ? '주제를 찾지 못했어요. 인구, 날씨, 병원처럼 필요한 데이터 주제를 알려 주세요.' :
        data.scope.unavailable_countries?.length ? copy('{countries} 제공처는 현재 카탈로그에 등록되어 있지 않아요. 다른 국가 자료로 대체하지 않았어요.', {countries: data.scope.unavailable_countries.join(' · ')}) :
        '지정한 국가와 주제에 연결된 자료가 없어요. 국가나 주제를 바꿔서 찾아보세요.';
      answer.append(el('p', 'answer', typeof message === 'object' ? message : copy(message)));
      return;
    }
    const open=el('a','relaxation', copy('관계 그래프에서 살펴보기'));
    open.href='/ontology?'+new URLSearchParams({indicator:data.concept_id});
    answer.append(el('p','answer', copy('관계 탐색 화면에서 이 개념의 연결을 확인할 수 있어요.')),open);
  }
  const bytes = value => new TextEncoder().encode(value).length;
  const record = value => !!value && typeof value === 'object' && !Array.isArray(value);
  const validIndicator = value => typeof value === 'string' && /^[a-z0-9_]{1,128}$/.test(value);
  // /api/chat/jobs accepts exactly the explicit routes in site_search.INDICATORS
  // except "other". Taxonomy nodes outside that contract remain inspection-only.
  const searchableIndicators = new Set(('population fertility_rate death_rate net_migration elderly_share households business_count ' +
    'business_sales foot_traffic temperature precipitation traffic_volume hospital_locations medical_specialties opening_hours ' +
    'pharmacies health_indicators hospital_beds health_access life_expectancy boundaries facilities housing employment income prices ' +
    'energy environment education agriculture culture finance trade welfare digital law science safety water nature geology government ' +
    'development diplomacy transport geography basemap spatial_services weather economy social research_data statistics').split(' '));
  function publishResult(data) {
    if (!['clarify', 'results', 'partial', 'unverified'].includes(data.state) || !record(data.plan) ||
        !Array.isArray(data.plan.needs) || data.plan.needs.length > 5 || !Array.isArray(data.groups) || data.groups.length > 5 ||
        !data.plan.needs.every(need => record(need) && validIndicator(need.indicator)) ||
        !data.groups.every(group => record(group) && validIndicator(group.indicator) && Array.isArray(group.sites)) ||
        data.plan.selected_concept && !validIndicator(data.plan.selected_concept)) return;
    lastResult = data;
    const text=(value,max)=>typeof value==='string'?value.slice(0,max):'';
    const list=(value,max)=>Array.isArray(value)?value.slice(0,max):[];
    const topic=(value,related=false)=>({id:text(value?.id,80),label:text(value?.label,160),definition:text(value?.definition,2000),parent_id:text(value?.parent_id,80),
      ...(related?{via_topic_id:text(value?.via_topic_id,80),topic_cosine:value?.topic_cosine}:{query_cosine:value?.query_cosine})});
    const exploration=data.exploration;
    const topicContext=value=>record(value)&&['high','low','unclassified'].includes(value.classification?.level)&&Number.isFinite(value.classification?.max_similarity)?
      {version:text(value.version,100),classification:{level:value.classification.level,max_similarity:value.classification.max_similarity},
        topics:list(value.topics,15).map(item=>({id:text(item?.id,80),label:text(item?.label,160),similarity:item?.similarity,level:item?.level}))}:null;
    const selected=window.SiteResults.selectSites(data),groups=new Map();let remaining=10;const records=new Set();
    for(const site of selected)for(const match of site.matches){
      const evidence=[];
      for(const fact of match.evidence){
        if(!remaining||typeof fact.dataset_id!=='string'||records.has(fact.dataset_id)||!Number.isFinite(fact.cosine)||Math.abs(fact.cosine)>1)continue;
        records.add(fact.dataset_id);remaining--;
        evidence.push({dataset_id:text(fact.dataset_id,512),title:text(fact.title,240),summary:text(fact.summary,1500),evidence_url:safeURL(fact.evidence_url)||'',cosine:fact.cosine,topic_context:topicContext(fact.topic_context),
          formats:list(fact.formats,8).map(value=>text(value,40)),topic_matches:list(fact.topic_matches,15).map(value=>({id:text(value?.id,80),label:text(value?.label,160),
            definition:text(value?.definition,2000),origin:value?.origin,via_topic_id:text(value?.via_topic_id,80),dataset_topic_cosine:value?.dataset_topic_cosine}))});
      }
      if(!evidence.length)continue;
      if(!groups.has(match.indicator))groups.set(match.indicator,{indicator:match.indicator,name:text(match.name,160),sites:[]});
      groups.get(match.indicator).sites.push({source_id:text(site.source_id,200),name:text(site.name,160),url:safeURL(site.url)||'',evidence});
    }
    notifyParent('result', {result: {state: data.state, plan: {selected_concept: data.plan.selected_concept || '',
      needs: data.plan.needs.map(need => ({indicator: need.indicator}))},
      groups:[...groups.values()],exploration:{status:exploration?.status||'unavailable',version:text(String(exploration?.version||''),80),
        query_topics:list(exploration?.query_topics,5).map(value=>topic(value)),related_topics:list(exploration?.related_topics,10).map(value=>topic(value,true)),
        relations:list(exploration?.relations,30).map(value=>({source:text(value?.source,80),target:text(value?.target,80),relation:text(value?.relation,40),basis:text(value?.basis,60),cosine:value?.cosine}))}}});
  }
  function clearInspection() {
    if (conceptDetail) { conceptDetail.hidden = true; conceptDetail.replaceChildren(); }
  }
  function inspectConcept(concept) {
    if (!record(concept) || !validIndicator(concept.indicator) ||
        typeof concept.label !== 'string' || !concept.label.trim() || bytes(concept.label) > 512 ||
        typeof concept.definition !== 'string' || bytes(concept.definition) > 8192 ||
        concept.relation !== undefined && (typeof concept.relation !== 'string' || bytes(concept.relation) > 4096)) return;
    if (!conceptDetail) {
      conceptDetail = el('section', 'embedded-concept-detail'); conceptDetail.id = 'embedded-concept-detail';
      conceptDetail.setAttribute('aria-labelledby', 'embedded-concept-title');
      $('conversation').prepend(conceptDetail);
    }
    conceptDetail.replaceChildren(); conceptDetail.hidden = false;
    const title = el('h3', '', copy('{value}', () => ({value: i18n?.topic(concept.indicator, concept.label) || concept.label}))); title.id = 'embedded-concept-title';
    const heading = el('div', 'embedded-concept-heading');
    const close = el('button', 'quiet-button', copy('정보 닫기')); close.type = 'button';
    close.addEventListener('click', () => { clearInspection(); if (!messages.children.length) showChatPanel(false); input.focus({preventScroll: true}); });
    heading.append(title, close); conceptDetail.append(heading, el('p', 'embedded-concept-definition', copy('{value}', () => ({value: i18n?.topic(concept.indicator, concept.definition, 'definition') || concept.definition}))));
    if (concept.relation) conceptDetail.append(el('p', 'dataset-meta', concept.relation));
    const groups = lastResult?.groups.filter(group => group.indicator === concept.indicator) || [];
    const matched = lastResult && {...lastResult, groups, relaxation: null};
    if (matched && window.SiteResults.selectSites(matched).length) {
      const sites = el('div', 'embedded-concept-sites');
      // Inspection is separate from the turn log and never selects a new context.
      window.SiteResults.render(sites, matched, {el, safeURL, submit}); conceptDetail.append(sites);
    } else {
      conceptDetail.append(el('p', 'answer', copy('현재 검색 결과에서 이 개념의 제공 근거가 확인된 사이트는 없어요.')));
      const supported = searchableIndicators.has(concept.indicator);
      if (!supported) conceptDetail.append(el('p', 'dataset-meta', copy('이 개념의 직접 검색은 아직 지원하지 않아요. 다른 개념으로 바꾸어 검색하지 않았어요.')));
      const search = el('button', 'inspect-search relaxation', copy('이 개념의 자료 찾기')); search.type = 'button';
      search.dataset.supported = String(supported); search.disabled = !supported || !!pendingJob || !!controller;
      search.addEventListener('click', () => {
        if (!searchableIndicators.has(concept.indicator)) return;
        const label = names.get(concept.indicator) || concept.label;
        submit(label + ' 자료 찾기', searchContext, concept.indicator, label + ' 자료 찾기');
      });
      conceptDetail.append(search);
    }
    $('welcome').hidden = true; showChatPanel(true); $('conversation').scrollTop = 0;
    close.focus({preventScroll: true});
  }
  if (embeddedChat) window.addEventListener('message', event => {
    if (event.source !== window.parent || event.origin !== location.origin || !record(event.data)) return;
    const data = event.data;
    if (data.type !== 'dataieum-chat' || data.version !== 1 || !['open', 'inspect', 'layout', 'locale', 'guide-highlight', 'guide-example'].includes(data.action) ||
        !Object.keys(data).every(key => ['type', 'version', 'action', data.action === 'guide-highlight' ? 'target' : data.action === 'guide-example' ? 'text' : data.action === 'locale' ? 'locale' : data.action === 'layout' ? 'expanded' : 'concept'].includes(key))) return;
    try { if (bytes(JSON.stringify(data)) > 16384) return; } catch { return; }
    if (data.action === 'guide-highlight') {
      if (Object.keys(data).length === 4 && ['', 'input', 'results', 'source'].includes(data.target)) document.body.dataset.guideTarget = data.target;
    }
    else if (data.action === 'guide-example') {
      // The walkthrough only fills the composer; the person still decides to send.
      if (Object.keys(data).length === 4 && typeof data.text === 'string' && data.text.trim() && bytes(data.text) <= 200 && !input.disabled) {
        input.value = data.text; input.dispatchEvent(new Event('input')); input.focus({preventScroll: true});
      }
    }
    else if (data.action === 'locale') { if (['ko', 'en'].includes(data.locale) && Object.keys(data).length === 4) i18n?.setLocale(data.locale); }
    else if (data.action === 'open') { setEmbeddedPhase(embeddedPhase, true); input.focus({preventScroll: true}); }
    else if (data.action === 'layout') { if (typeof data.expanded === 'boolean') setEmbeddedLayout(data.expanded); }
    else inspectConcept(data.concept);
  });
  function validStoredJob(value) {
    if (!record(value) || value.version !== 1 || typeof value.request_token !== 'string' || !/^[a-f0-9]{64}$/.test(value.request_token) ||
        !(value.job_id === null || typeof value.job_id === 'string' && /^[a-f0-9]{32}$/.test(value.job_id)) ||
        !Number.isSafeInteger(value.created_at) || value.created_at <= 0 ||
        typeof value.cancel_requested !== 'boolean' || !record(value.payload) ||
        typeof value.displayQuery !== 'string' || !value.displayQuery.trim() || bytes(value.displayQuery) > 4096 ||
        !Object.keys(value).every(key => ['version', 'request_token', 'job_id', 'payload', 'displayQuery', 'cancel_requested', 'created_at'].includes(key))) return false;
    const payload = value.payload;
    return typeof payload.query === 'string' && !!payload.query.trim() && bytes(payload.query) <= 2048 &&
      (payload.context === null || record(payload.context)) &&
      (payload.indicator === undefined || typeof payload.indicator === 'string' && /^[a-z0-9_]{1,128}$/.test(payload.indicator)) &&
      Object.keys(payload).every(key => ['query', 'context', 'indicator'].includes(key)) && bytes(JSON.stringify(payload)) <= 32768;
  }
  function savePending() {
    try { sessionStorage.setItem(storageKey, JSON.stringify(pendingJob)); return true; }
    catch { return false; }
  }
  function clearPending() {
    pendingJob = null;
    try { sessionStorage.removeItem(storageKey); } catch {}
    ui($('search-progress-status'), '');
  }
  function showJob(message, error = false) {
    jobAnswer.replaceChildren(el('p', error ? 'answer error' : 'pending search-progress', typeof message === 'object' ? message : copy(message)));
    ui($('search-progress-status'), message);
  }
  function createTurn(job) {
    restorationIssue = false;
    lastResult = null; clearInspection(); notifyParent('search-start');
    $('welcome').hidden = true;
    const turn = el('section', 'turn');
    turn.append(el('p', 'user-message', job.displayQuery), el('div', 'assistant-label', copy('데이터이음')));
    jobAnswer = el('div'); turn.append(jobAnswer); messages.append(turn);
    // Only the one unfinished request is kept in this tab's session storage.
    while (messages.children.length > 8) messages.firstElementChild.remove();
    $('chat-history-count').textContent = String(messages.children.length);
    showChatPanel(true); sizeComposer();
    busy(true); scrollToEnd();
  }
  function pauseJob(unavailable = false, conflict = false) {
    const message = conflict ? '저장된 요청 정보가 기존 접수 정보와 맞지 않아 상태를 확인할 수 없어요. 자동으로 다시 검색하거나 서버 요청을 취소하지 않았어요.' :
      unavailable ? '보관 기간이 지났거나 접근 정보를 확인할 수 없어 이 요청을 불러올 수 없어요. 자동으로 다시 검색하지 않았어요.' :
      pendingJob.cancel_requested ? '취소 여부를 확인하지 못했어요. 요청이 아직 처리 중일 수 있어요. 연결을 확인한 뒤 취소 상태를 이어서 확인해 주세요.' :
      '연결이 끊겨 상태 확인을 멈췄어요. 접수된 요청은 계속 처리될 수 있어요. 같은 요청을 이어서 확인할 수 있어요.';
    showJob(message, true);
    const resume = el('button', 'job-resume retry', copy(pendingJob.cancel_requested ? '취소 상태 이어서 확인' : '요청 이어서 확인'));
    resume.type = 'button'; resume.addEventListener('click', () => { if (!controller) monitor(); });
    jobAnswer.append(resume);
    if (unavailable || conflict) {
      const close = el('button', 'job-resume retry', copy('이 요청 닫고 새 검색')); close.type = 'button';
      close.addEventListener('click', () => {
        if (controller) return;
        clearPending(); busy(false);
        notifyParent('request-closed');
        jobAnswer.replaceChildren(el('p', 'answer', copy('이 화면의 요청을 닫았어요. 서버의 취소가 확인된 것은 아니에요.')));
        ui(status, '새 질문을 입력해 주세요.'); input.focus();
      });
      jobAnswer.append(close);
    }
    ui(status, message);
    // Keep new searches blocked until the existing request is resolved or explicitly closed.
    messages.setAttribute('aria-busy', 'false');
  }
  function failureMessage(code) {
    return code === 'authentication_required' ? '검색 서비스 연결을 복구해야 해요. 운영자의 재연결이 끝난 뒤 다시 검색해 주세요.' :
      code === 'unsafe_prompt' ? '검색과 관계없는 지시가 포함되어 있어요. 필요한 데이터와 조건만 입력해 주세요.' :
      code === 'job_owner_limit' ? '같은 IP에서 대기하거나 처리 중인 요청이 너무 많아요. 기존 요청이 끝난 뒤 보내 주세요.' :
      code === 'job_queue_full' ? '접수 가능한 요청 수에 도달했어요. 잠시 후 다시 보내 주세요.' :
      code === 'job_receipt_capacity' ? '전체 서비스의 임시 접수 한도에 도달해 요청을 접수하지 못했어요. 잠시 후 다시 보내 주세요.' :
      code === 'queue_expired' ? '대기 가능한 시간이 지나 검색을 시작하지 못했어요. 원하시면 새로 요청해 주세요.' :
      code === 'model_initialization_timeout' ? '검색 연결을 준비하는 데 시간이 오래 걸려 검색을 시작하지 못했어요. 잠시 후 다시 시도해 주세요.' :
      ['chat_busy', 'dependency_busy', 'capacity_exceeded', 'server_overloaded'].includes(code) ?
      '검색 처리 단계가 혼잡해 요청을 완료하지 못했어요. 잠시 후 다시 시도해 주세요.' :
      ['execution_timeout', 'dependency_timeout', 'chat_timeout'].includes(code) ?
      '검색 처리 시간이 한도를 넘었어요. 자동으로 재실행하지 않았어요. 원하시면 다시 요청해 주세요.' :
      code === 'vector_unavailable' ? '의미 검색 연결을 사용할 수 없어 요청을 완료하지 못했어요.' :
      ['worker_restarted', 'stopped'].includes(code) ? '검색 서비스가 중단되어 요청을 완료하지 못했어요. 자동으로 재실행하지 않았어요.' :
      code === 'chat_unavailable' ? '검색 서비스에 연결하지 못했어요. 잠시 후 다시 시도해 주세요.' :
      ['chat_invalid_response', 'invalid_job_result'].includes(code) ? '검색 결과를 확인하는 중 오류가 발생했어요. 잠시 후 다시 시도해 주세요.' :
      '검색 처리 중 오류가 발생했어요. 입력한 조건의 문제로 확인된 것은 아니에요. 잠시 후 다시 시도해 주세요.';
  }
  function finishJob(value) {
    const job = pendingJob;
    clearPending();
    if (value.status === 'completed') {
      try {
        renderResults(jobAnswer, value.result);
        if(value.result.version===2 && value.result.state!=='clarify' && value.result.plan?.related_mode!=='exclude') {
          startRelated(job,jobAnswer);
        }
        ui(status, value.result.state === 'clarify' ? '필요한 조건을 알려 주시면 다시 찾아드릴게요.' :
          value.result.state === 'unverified' ? '조건을 바꾸거나 카탈로그에서 직접 찾아보세요.' :
          '자료 페이지를 열거나, 원하는 조건을 이어서 말해 주세요.');
      } catch {
        jobAnswer.replaceChildren(el('p', 'answer error', copy('완료된 응답의 형식을 확인하지 못했어요. 자동으로 다시 검색하지 않았어요.')));
        ui(status, '응답을 표시하지 못했어요. 새 질문을 입력할 수 있어요.');
      }
    } else {
      const cancelled = value.status === 'cancelled';
      const code = typeof value.error === 'string' ? value.error : value.error_code;
      if (code === 'authentication_required' && !job.job_id && !input.value) input.value = job.displayQuery || job.payload.query;
      const message = cancelled ? '요청 취소가 확인됐어요.' : failureMessage(code);
      jobAnswer.replaceChildren(el('p', cancelled ? 'answer' : 'answer error', copy(message)));
      if (!cancelled && code !== 'unsafe_prompt') {
        const retry = el('button', 'retry', copy('새 요청으로 다시 검색')); retry.type = 'button';
        retry.addEventListener('click', () => submit(job.payload.query, job.payload.context, job.payload.indicator, job.displayQuery));
        jobAnswer.append(retry);
      }
      ui(status, cancelled ? '다른 질문을 입력해 주세요.' : message);
    }
    busy(false); showChatPanel(true);
    notifyParent('search-end', {status: value.status});
    // Make room for the answer without interrupting a draft or another window.
    if (!input.value && document.hasFocus() && [input, $('send'), $('stop')].includes(document.activeElement)) {
      jobAnswer.setAttribute('tabindex', '-1'); jobAnswer.focus({preventScroll: true});
    }
    sizeComposer(); jobAnswer.scrollIntoView({block: 'start'});
  }
  function waitForPoll(milliseconds, signal) {
    return new Promise((resolve, reject) => {
      const finish = () => { signal.removeEventListener('abort', abort); resolve(); };
      const timer = setTimeout(finish, milliseconds);
      const abort = () => { clearTimeout(timer); signal.removeEventListener('abort', abort); reject(new DOMException('Aborted', 'AbortError')); };
      signal.addEventListener('abort', abort, {once: true});
      if (signal.aborted) abort();
    });
  }
  let relatedRun=null, relatedSequence=0;
  function cancelRelated() {
    const run=relatedRun;if(!run)return;
    relatedRun=null;run.cancelled=true;run.controller.abort();
    if(run.receipt)deleteRelated(run.receipt);
    run.container.hidden=true;
    run.jump.hidden=true;
  }
  function deleteRelated(receipt) {
    fetch('/api/chat/jobs/'+receipt.job_id,{method:'DELETE',keepalive:true,
      headers:{'X-Dataieum-Chat':'1','X-Dataieum-Job-Token':receipt.request_token},
      signal:AbortSignal.timeout(5000)}).catch(()=>{});
  }
  function startRelated(parent,answer) {
    cancelRelated();
    const container=el('section','related-results');container.setAttribute('aria-busy','true');
    container.id='related-results-'+(++relatedSequence);
    const jump=el('button','related-results-jump');jump.type='button';jump.hidden=true;
    jump.setAttribute('aria-controls',container.id);
    answer.querySelector('.site-results-toolbar')?.append(jump);
    jump.addEventListener('click',()=>{
      if(container.hidden)return;
      container.scrollIntoView({block:'start',behavior:window.matchMedia('(prefers-reduced-motion: reduce)').matches?'auto':'smooth'});
      container.querySelector('.site-results-section-heading')?.focus({preventScroll:true});
    });
    // Only the small status line is live. Do not reread ten existing cards.
    const loading=ui(el('p','dataset-meta'),'관련 개념의 자료도 살펴보고 있어요…');
    loading.setAttribute('role','status');container.append(loading);answer.append(container);
    const run={controller:new AbortController(),container,jump,receipt:null,cancelled:false};relatedRun=run;
    (async()=>{
      const timer=setTimeout(()=>run.controller.abort(),28000);
      try {
        // Let POST finish even on navigation so a late receipt can be cancelled.
        const response=await fetch('/api/chat/related',{method:'POST',cache:'no-store',signal:AbortSignal.timeout(12000),
          headers:{'Content-Type':'application/json','X-Dataieum-Chat':'1','X-Dataieum-Job-Token':parent.request_token},
          body:JSON.stringify({parent_job_id:parent.job_id})});
        if(!response.ok)throw new Error('related_unavailable');
        let value=await response.json();
        if(!/^[a-f0-9]{32}$/.test(value.job_id)||!/^[a-f0-9]{64}$/.test(value.request_token))throw new Error('invalid_related_receipt');
        run.receipt={job_id:value.job_id,request_token:value.request_token};
        if(run.cancelled||run.controller.signal.aborted){deleteRelated(run.receipt);return;}
        while(['queued','running'].includes(value.status)){
          const res=await fetch('/api/chat/jobs/'+value.job_id,{signal:run.controller.signal,cache:'no-store',
            headers:{'X-Dataieum-Chat':'1','X-Dataieum-Job-Token':run.receipt.request_token,'X-Dataieum-Job-State':value.status}});
          if(!res.ok)throw new Error('related_unavailable');
          value=await res.json();
          if(value.job_id!==run.receipt.job_id)throw new Error('invalid_related_receipt');
        }
        if(relatedRun!==run||run.cancelled)return;
        if(value.status!=='completed')throw new Error('related_unavailable');
        const count=window.SiteResults.renderRelated(container,value.result,{el,safeURL});
        jump.hidden=!count;
        if(count)ui(jump,copy('관련 자료 {count}개 ↓',{count}));
      } catch {
        if(run.receipt)deleteRelated(run.receipt);
        container.hidden=true;
        jump.hidden=true;
      } finally {
        clearTimeout(timer);container.setAttribute('aria-busy','false');
        if(relatedRun===run)relatedRun=null;
      }
    })();
  }
  async function jobRequest(method, signal, previousState = null) {
    const request = new AbortController();
    const abort = () => request.abort();
    signal.addEventListener('abort', abort, {once: true});
    if (signal.aborted) abort();
    // Server wait includes its first DB reads; allow bounded final DB cleanup
    // and response transfer after the 10-second status observation window.
    const timeout = setTimeout(abort, method === 'GET' && previousState ? 17000 : 12000);
    const headers = {'X-Dataieum-Chat': '1'};
    const options = {method, signal: request.signal, cache: 'no-store', headers};
    let url = '/api/chat/jobs';
    if (method === 'POST') {
      headers['Content-Type'] = 'application/json';
      options.body = JSON.stringify({...pendingJob.payload, request_token: pendingJob.request_token});
    } else {
      url += '/' + pendingJob.job_id;
      headers['X-Dataieum-Job-Token'] = pendingJob.request_token;
      if (method === 'GET' && ['queued','running'].includes(previousState)) headers['X-Dataieum-Job-State'] = previousState;
    }
    try {
      const response = await fetch(url, options);
      let value;
      try { value = await response.json(); } catch { throw new Error('invalid_job_response'); }
      if (!response.ok) {
        const error = new Error('http'); error.status = response.status;
        error.code = typeof value.code === 'string' ? value.code : value.error;
        throw error;
      }
      if (!record(value) || !/^[a-f0-9]{32}$/.test(value.job_id) ||
          !['queued', 'running', ...terminalStates].includes(value.status) ||
          pendingJob.job_id && value.job_id !== pendingJob.job_id) throw new Error('invalid_job_response');
      return value;
    } finally { clearTimeout(timeout); signal.removeEventListener('abort', abort); }
  }
  function monitor() {
    if (controller || !pendingJob || navigating) return monitorPromise;
    controller = new AbortController();
    const request = controller;
    busy(true);
    showJob(pendingJob.cancel_requested ? '접수 상태를 확인하고 취소를 요청하고 있어요…' :
      pendingJob.job_id ? '접수된 요청의 상태를 확인하고 있어요…' : '요청을 접수하고 있어요…');
    ui(status, '대기 중에도 이 탭을 새로고침하면 요청을 이어서 확인할 수 있어요.');
    monitorPromise = (async () => {
      let failures = 0, deleteSent = false, paused = false, previousState = null;
      try {
        while (pendingJob && !request.signal.aborted) {
          try {
            // A missing receipt must not turn an old, possibly completed request into a new model run.
            const age = Date.now() - pendingJob.created_at;
            if (!pendingJob.job_id && (age > 2 * 60 * 60 * 1000 || age < -5 * 60 * 1000)) {
              paused = true; pauseJob(true); return;
            }
            const method = !pendingJob.job_id ? 'POST' : pendingJob.cancel_requested && !deleteSent ? 'DELETE' : 'GET';
            const value = await jobRequest(method, request.signal, previousState);
            previousState = value.wait_supported === true ? value.status : null;
            if (request.signal.aborted) return;
            pendingJob.job_id = value.job_id;
            if (value.cancel_requested === true) pendingJob.cancel_requested = true;
            if (method === 'DELETE') deleteSent = true;
            const stored = savePending();
            failures = 0;
            if (terminalStates.has(value.status)) { finishJob(value); return; }
            if (pendingJob.cancel_requested && !deleteSent) continue;
            const message = pendingJob.cancel_requested ? '취소를 요청했어요. 실행 중인 작업이 정리되는지 확인하고 있어요…' :
              value.status === 'queued' && value.blocked_reason === 'authentication_required' ?
                '검색 서비스 연결을 복구해야 해서 대기 중이에요. 접수한 요청은 유지되며, 대기 시간 안에 연결이 복구되면 이어서 검색해요.' :
              value.status === 'queued' ? Number.isInteger(value.position) && value.position > 0 ?
                copy('요청이 접수됐어요. 현재 대기 순서 {position}번째예요.', {position: number.format(value.position)}) : '요청이 접수되어 순서를 기다리고 있어요.' :
                '검색을 시작했어요. 요청한 조건에 맞는 자료와 사이트를 확인하고 있어요…';
            showJob(message); busy(true);
            ui(status, stored ? '순서가 되면 검색하고 결과를 보여드려요. 중지 버튼으로 취소할 수 있어요.' :
              '이 브라우저에 복구 정보를 저장하지 못했어요. 결과를 확인할 때까지 이 탭을 유지해 주세요.');
            const hint = Number.isFinite(value.poll_after_ms) ? value.poll_after_ms : 3000;
            if (value.wait_supported !== true) await waitForPoll(Math.min(10000, Math.max(3000, hint)) + Math.random() * 500, request.signal);
          } catch (error) {
            if (request.signal.aborted) return;
            const rejected = !pendingJob.job_id && ['job_owner_limit', 'job_queue_full', 'job_receipt_capacity', 'invalid_job_token',
              'invalid_job_payload', 'invalid_job_owner', 'unsafe_prompt', 'authentication_required'].includes(error.code);
            if (rejected) { finishJob({status: 'failed', error: error.code}); return; }
            const unavailable = error.status === 404 && error.code === 'job_not_found' ||
              error.status === 410 && error.code === 'job_expired';
            if (unavailable || error.status === 409 || ++failures >= 3) {
              paused = true; pauseJob(unavailable, error.status === 409); return;
            }
            showJob(copy('연결 상태를 다시 확인하고 있어요… ({attempt}/3)', {attempt: failures}));
            await waitForPoll(3000 + Math.random() * 500, request.signal);
          }
        }
      } catch (error) {
        if (!request.signal.aborted && pendingJob) { paused = true; pauseJob(); }
      } finally {
        if (controller === request) controller = null;
        busy(!!pendingJob);
        if (paused) {
          messages.setAttribute('aria-busy', 'false');
          jobAnswer.querySelectorAll('.job-resume').forEach(button => { button.disabled = false; });
        }
        if (!navigating) refreshConnection();
      }
    })();
    return monitorPromise;
  }
  async function submit(query, contextSnapshot = searchContext, selectedIndicator = null, displayQuery = query) {
    if (pendingJob || controller) {
      ui(status, '기존 요청을 완료하거나 취소한 뒤 새 질문을 보내 주세요.'); return;
    }
    query = query.trim();
    if (!query) { input.focus(); return; }
    if (bytes(query) > 2048) { ui(status, '질문을 600자 이내로 줄여 주세요.'); input.focus(); return; }
    cancelRelated();
    try {
      const token = Array.from(crypto.getRandomValues(new Uint8Array(32)), value => value.toString(16).padStart(2, '0')).join('');
      const payload = {query, context: contextSnapshot ? JSON.parse(JSON.stringify(contextSnapshot)) : null};
      if (selectedIndicator) payload.indicator = selectedIndicator;
      const job = {version: 1, request_token: token, job_id: null, payload, displayQuery, cancel_requested: false, created_at: Date.now()};
      if (!validStoredJob(job)) throw new Error('invalid');
      pendingJob = job;
      if (!savePending()) { pendingJob = null; throw new Error('storage'); }
      if (!selectedIndicator) input.value = '';
      createTurn(job); monitor();
    } catch {
      ui(status, '질문 또는 요청 복구 정보를 저장하지 못했어요. 질문을 줄이거나 브라우저 저장 설정을 확인해 주세요. 아직 접수하지 않았어요.');
    }
  }
  async function cancelPending() {
    if (!pendingJob || pendingJob.cancel_requested && controller) return;
    pendingJob.cancel_requested = true; savePending();
    const previous = monitorPromise;
    controller?.abort('cancel');
    if (previous) await previous;
    if (pendingJob && !navigating) monitor();
  }
  $('chat-form').addEventListener('submit', event => { event.preventDefault(); submit(input.value); });
  input.addEventListener('keydown', event => {
    if (event.key === 'Enter' && !event.shiftKey && !event.isComposing) { event.preventDefault(); submit(input.value); }
  });
  $('stop').addEventListener('click', cancelPending);
  document.querySelectorAll('[data-prompt]').forEach(button => {
    button.addEventListener('click', () => submit(button.dataset.prompt));
  });
  $('new-chat').addEventListener('click', () => {
    if (pendingJob || controller) return;
    cancelRelated();
    messages.replaceChildren(); searchContext = null; lastResult = null; clearInspection(); restorationIssue = false; $('welcome').hidden = false; input.value = '';
    notifyParent('reset');
    $('chat-history-count').textContent = ''; showChatPanel(false); topicGraph?.reset(); sizeComposer();
    ui(status, '국가를 적으면 그 범위로, 생략하면 여러 나라 자료를 찾아요.'); input.focus();
  });
  // Navigation disconnects polling only: the durable server job remains queued/running.
  window.addEventListener('pagehide', () => { navigating = true; controller?.abort('navigation'); cancelRelated(); });
  window.addEventListener('pageshow', async event => {
    navigating = false;
    if (event.persisted && pendingJob) { if (monitorPromise) await monitorPromise; monitor(); }
  });
  $('connection-status').addEventListener('click', refreshConnection);
  window.addEventListener('focus', refreshConnection);
  try {
    const stored = sessionStorage.getItem(storageKey);
    if (stored) {
      if (bytes(stored) > 65536) throw new Error('invalid');
      const value = JSON.parse(stored);
      if (!validStoredJob(value)) throw new Error('invalid');
      pendingJob = value; createTurn(value); monitor();
    }
  } catch {
    restorationIssue = true;
    try { sessionStorage.removeItem(storageKey); } catch {}
    ui(status, '저장된 요청 정보를 확인할 수 없어 자동으로 연결하지 않았어요. 기존 요청의 취소 여부는 확인되지 않았어요.');
  }
  // Locale changes only update bound copy; never replay or republish a search.
  if (embeddedChat) i18n?.subscribe(locale => notifyParent('locale', {locale}));
  refreshConnection();
  fetch('/api/bootstrap', {signal: AbortSignal.timeout(5000), cache: 'no-store'})
    .then(r => r.ok ? r.json() : null).then(data => {
      for (const item of data?.concepts || []) names.set(item.id, item.name);
      for (const item of data?.sources || []) {
        sources.set(item.id, item.name); sourceCountries.set(item.id, item.country);
      }
    }).catch(() => {});
})();
