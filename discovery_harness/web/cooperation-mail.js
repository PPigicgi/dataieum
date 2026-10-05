/* Fixed mail forms: server owns member identity, catalogue, recipient and final text. */
(() => {
'use strict';
const PURPOSES = {
  analysis: ['업무 분석', '업무에 필요한 자료를 분석하려고 합니다.', '자료를 비교하고 분석 결과를 업무에 참고하려고 합니다.'],
  research: ['연구·논문', '연구에 필요한 자료를 살펴보려고 합니다.', '자료를 분석해 연구나 논문 작성에 참고하려고 합니다.'],
  service: ['서비스 개발', '서비스 개발에 필요한 자료를 검토하려고 합니다.', '자료의 항목과 내용을 검토해 개발 중인 서비스에 활용하려고 합니다.'],
  education: ['교육·학습', '교육과 학습에 필요한 자료를 살펴보려고 합니다.', '자료를 읽고 분석해 교육이나 학습에 활용하려고 합니다.']
};
const STATUS = {draft:'미리보기', queued:'발송 대기', sending:'발송 처리 중', retrying:'재시도 중', accepted:'발송됨',
  delivered:'수신 서버에 전달됨', delayed:'수신 서버 전달 지연', failed:'발송 실패', unknown:'발송 여부 확인 필요',
  bounced:'반송됨', complained:'수신 거부 신고', cancelled:'발송 취소'};
// Visual tone only; the status text above stays the source of meaning.
const TONES = {draft:'muted', queued:'accent', sending:'accent', retrying:'accent', accepted:'ok', delivered:'ok',
  delayed:'warn', unknown:'warn', failed:'bad', bounced:'bad', complained:'bad', cancelled:'muted'};
const TYPE_NOTES = {usage_inquiry:'이용 조건과 출처 표기 방법을 묻습니다.', data_provision:'필요한 범위의 자료를 보내 달라고 요청합니다.'};
const STEPS = [['data','자료'],['write','작성'],['review','확인'],['sent','보냄']];
const t = value => window.DataieumI18n?.text(value) || value;
const strings = values => Array.isArray(values) ? values.filter(v => typeof v === 'string' || typeof v === 'number').map(String).slice(0,30) : [];
function searchHints(plan, indicator, fact={}) {
  const hints={years:plan.years,regions:plan.regions,countries:plan.countries,dates:plan.dates,formats:fact.formats||[]};
  const need=(plan.needs||[]).find(item=>item.indicator===indicator);
  for(const scope of need?.scope||[])if(['years','regions','countries'].includes(scope.field))hints[scope.field]=scope.values||[];
  return hints;
}
function createDraft(datasetId, hints={}) {
  const years=strings(hints.years), regions=strings(hints.regions), countries=strings(hints.countries), items=strings(hints.items),dates=strings(hints.dates);
  const scope_hint=[years.length ? '기간: '+years.join(', ') : '', regions.length ? '지역: '+regions.join(', ') : '',
    countries.length ? '국가: '+countries.join(', ') : '', dates.length ? '날짜: '+dates.join('~') : '', items.length ? '항목: '+items.join(', ') : ''].filter(Boolean).join(' / ').slice(0,2000);
  return {dataset_id:datasetId, request_type:'usage_inquiry', purpose:'', usage_description:'', commercial_use:'',
    requested_scope:'', preferred_format:'', purpose_key:'', scope_hint, formats:strings(hints.formats)};
}
function choosePurpose(draft, key) {
  if(!PURPOSES[key]) return;
  draft.purpose_key=key; draft.purpose=PURPOSES[key][1]; draft.usage_description=PURPOSES[key][2];
}
function requestInput(draft) {
  if(!draft.purpose.trim() || !draft.usage_description.trim()) throw new Error('활용 목적을 선택해 주세요.');
  if(!['commercial','noncommercial','undecided'].includes(draft.commercial_use)) throw new Error('상업적 이용 여부를 선택해 주세요.');
  if(draft.request_type==='data_provision' && !draft.requested_scope.trim()) throw new Error('요청할 자료의 범위를 선택해 주세요.');
  return Object.fromEntries(['dataset_id','request_type','purpose','usage_description','commercial_use','requested_scope','preferred_format'].map(k=>[k,draft[k]]));
}
function previewInput(draft, preview) {
  const input=requestInput(draft);if(preview?.status==='draft')input.request_id=preview.request_id;return input;
}
function updateAccount(target, account) {
  if(target.member?.member_id!==account.member?.member_id)target.preview=null;
  Object.assign(target,{member:account.member,csrf:account.csrf,enabled:account.enabled,
    auth_method:account.auth_method||target.auth_method||'email',gmail_connected:account.gmail_connected===true});
}
const state={member:null, csrf:null, enabled:false, draft:null, options:null, preview:null, pending:false, generation:0};
let dialog, steps, tabs, content, alertBox, opener, poll, entry, viewGeneration=0;
function el(tag, cls='', value) {
  const node=document.createElement(tag); if(cls)node.className=cls;
  if(value!==undefined) node.textContent=value;
  return node;
}
function copy(tag, cls, value) {const node=el(tag,cls,t(value)); window.DataieumI18n?.bind(node,value); return node;}
function button(label, action, cls='coop-button') {
  const node=copy('button',cls,label); node.type='button'; node.addEventListener('click',()=>run(action)); return node;
}
function submit(label) {const node=copy('button','coop-button coop-primary',label);node.type='submit';return node;}
function note(value) {return copy('p','coop-note',value);}
function footer(...items) {const bar=el('div','coop-actions coop-footer');bar.append(...items);return bar;}
function error(value) {alertBox.textContent=value; alertBox.hidden=false; alertBox.focus();}
async function run(action) {
  if(state.pending)return;
  state.pending=true; dialog?.setAttribute('aria-busy','true'); alertBox.hidden=true;
  try {await action();} catch(e) {
    if(e.code==='google_reconnect_required'){state.gmail_connected=false;showLogin();}
    error(e.message || t('잠시 후 다시 시도해 주세요.'));
  }
  finally {state.pending=false; dialog?.setAttribute('aria-busy','false');}
}
async function api(path, {method='GET', body}={}) {
  const headers={}; if(method!=='GET'){headers['Content-Type']='application/json';headers['X-Dataieum-CSRF']=state.csrf || '';}
  const response=await fetch(path,{method,headers,credentials:'same-origin',body:body===undefined?undefined:JSON.stringify(body)});
  let value; try {value=await response.json();} catch {throw new Error(t('서버 응답을 확인할 수 없습니다. 잠시 후 다시 시도해 주세요.'));}
  if(!response.ok){const failure=new Error(value.error || t('요청을 처리할 수 없습니다.'));failure.code=value.code;throw failure;}
  return value;
}
function mount() {
  if(dialog)return;
  dialog=el('dialog','coop-dialog'); dialog.setAttribute('aria-labelledby','coop-title');
  const top=el('div','coop-top'), header=el('div','coop-header'), title=copy('h2','','데이터 요청 메일'); title.id='coop-title';
  header.append(title,button('닫기',()=>dialog.close(),'coop-button coop-close'));
  steps=el('ol','coop-steps');steps.setAttribute('aria-label',t('진행 단계'));steps.hidden=true;
  tabs=el('nav','coop-tabs');tabs.setAttribute('aria-label',t('메일 메뉴'));tabs.hidden=true;
  top.append(header,steps,tabs);dialog.append(top);
  alertBox=el('p','coop-alert');alertBox.hidden=true;alertBox.tabIndex=-1;alertBox.setAttribute('role','alert');dialog.append(alertBox);
  content=el('div','coop-content');dialog.append(content);document.body.append(dialog);
  dialog.addEventListener('close',()=>{clearTimeout(poll);state.generation++;opener?.focus();});
}
// The step thread mirrors the map's connecting lines: traveled segments are solid, the rest dashed.
function showSteps(step, tone) {
  const index=STEPS.findIndex(([key])=>key===step);steps.hidden=index<0;
  steps.replaceChildren(...(index<0?[]:STEPS.map(([key,label],i)=>{
    const item=el('li','coop-step');item.setAttribute('data-state',i<index?'done':i===index?'current':'todo');
    if(i===index){item.setAttribute('aria-current','step');if(tone)item.setAttribute('data-tone',tone);}
    item.append(el('span','coop-step-dot'),copy('span','coop-step-label',label));return item;
  })));
}
function showTabs(active) {
  tabs.replaceChildren();tabs.hidden=!state.member;
  if(!state.member)return;
  const items=[];if(state.draft)items.push(['write','메일 작성',showForm]);
  items.push(['history','내 요청',()=>showHistory()],['profile','회원정보',showProfile]);
  for(const [key,label,action] of items){
    const tab=button(label,action,'coop-tab'+(key===active?' is-active':''));
    if(key===active)tab.setAttribute('aria-current','page');tabs.append(tab);
  }
}
function page(title, {step=null, tone=null, tab=null, subject=null}={}) {
  viewGeneration++;clearTimeout(poll); content.replaceChildren();
  showSteps(step,tone);showTabs(tab);
  if(subject)datasetCard(content,subject);
  const heading=copy('h3','coop-heading',title);heading.tabIndex=-1;content.append(heading);heading.focus();
  return content;
}
function datasetCard(parent, options) {
  if(!options?.dataset?.title)return;
  const card=el('div','coop-subject');card.append(el('strong','coop-dataset',options.dataset.title));
  const meta=el('p','coop-subject-meta');
  if(options.institution)meta.append(el('span','',options.institution));
  link(meta,'자료 페이지',options.dataset.url);
  if(meta.children.length)card.append(meta);
  parent.append(card);
}
function facts(parent, values) {
  const list=el('ul','coop-facts');for(const value of values)list.append(copy('li','',value));parent.append(list);return list;
}
function field(parent, label, value, change, {type='text', max=200, required=false, multiline=false, autocomplete, hint}={}) {
  const wrap=el('label','coop-field'), caption=el('span','coop-field-label');caption.append(copy('span','',label));
  if(hint)caption.append(copy('small','',hint));wrap.append(caption);const input=el(multiline?'textarea':'input');
  if(!multiline)input.type=type;input.value=value;input.maxLength=max;input.required=required;
  if(autocomplete)input.autocomplete=autocomplete;
  input.addEventListener('input',()=>change(input.value));wrap.append(input);parent.append(wrap);return input;
}
// variant: cards (label + description), chips (short labels), segmented (one row of equal choices).
function choices(parent, legend, entries, value, change, variant='cards') {
  const group=el('fieldset','coop-choices coop-choices-'+variant);group.append(copy('legend','',legend));
  const list=el('div','coop-choice-list'), name='coop-'+Math.random().toString(36).slice(2);
  for(const [key,label,description] of entries){
    const wrap=el('label','coop-choice'), radio=el('input');radio.type='radio';radio.name=name;radio.value=key;radio.checked=key===value;
    const words=el('span','coop-choice-text');words.append(copy('strong','',label));if(description)words.append(copy('small','',description));
    radio.addEventListener('change',()=>{if(radio.checked)change(key);});wrap.append(radio,words);list.append(wrap);
  }
  group.append(list);parent.append(group);return group;
}
async function account(generation=state.generation) {
  const me=await api('/api/account/me');if(generation!==state.generation)return me;updateAccount(state,me);
  if(entry)entry.textContent=t(state.member?'내 요청':'가입·로그인');
  return me;
}
function link(parent, label, url, cls='coop-link') {
  try {const parsed=new URL(url);if(!['https:','http:'].includes(parsed.protocol))return;
    const node=copy('a',cls,label);node.href=parsed.href;node.target='_blank';node.rel='noopener noreferrer';parent.append(node);return node;
  } catch {}
}
function showContactReference(parent, options) {
  const reference=options?.contact_reference;
  const labels={
    data_inquiry:['자료 이용 문의','등록된 자료 문의 주소입니다. 자동 발송은 준비 중입니다.'],
    portal_support:['사이트 지원 문의','사이트 이용·지원 문의용 주소입니다. 자료의 이용 조건은 해당 제공기관에 확인해 주세요.'],
    manual_inquiry:['자료 제공 사전 문의','자료 신청은 공식 문의 페이지의 절차를 먼저 확인해 주세요.']
  };
  if(!reference || !labels[reference.kind] || typeof reference.email!=='string')return false;
  const [label,description]=labels[reference.kind], box=el('div','coop-reference'), details=el('dl');
  details.append(copy('dt','',label),el('dd','',reference.email));box.append(details,note(description));parent.append(box);return true;
}
function officialPage(parent, url) {
  const bar=el('div','coop-actions');link(bar,'공식 문의 페이지 열기',url,'coop-button coop-primary');
  if(bar.children.length)parent.append(bar);
}
async function open({datasetId, hints={}}={}) {
  try {if(window.parent!==window && window.parent.DataieumCooperation){return window.parent.DataieumCooperation.open({datasetId,hints});}} catch {}
  mount();opener=document.activeElement; if(!dialog.open)dialog.showModal();
  if(datasetId && state.draft?.dataset_id!==datasetId){state.draft=createDraft(datasetId,hints);state.preview=null;state.options=null;}
  const generation=++state.generation;
  await run(async()=>{
    const loading=page('자료 정보를 확인하고 있어요',{step:state.draft?'data':null});loading.append(el('div','coop-loading'));
    const calls=[account()];if(state.draft)calls.push(api('/api/cooperation/options?dataset_id='+encodeURIComponent(state.draft.dataset_id)));
    const results=await Promise.all(calls);if(generation!==state.generation)return;
    if(state.draft)state.options=results[1];
    if(!state.enabled){
      const box=page('메일 발송을 준비 중입니다',{step:state.options?'data':null,subject:state.options});
      box.append(note(state.auth_method==='google'?'본인 Gmail로 보내는 기능을 준비 중입니다. 계정 연결 설정이 완료되면 이 창에서 메일을 작성하고 보낼 수 있습니다.':'데이터이음 주소로 발송하고, 답장은 본인 이메일로 받는 기능을 준비 중입니다.'));
      showContactReference(box,state.options);if(state.options?.contact_url)officialPage(box,state.options.contact_url);return;
    }
    if(!state.member && !datasetId)showLogin();
    else if(state.draft)showForm();else if(!state.member)showLogin();else await showHistory();
  });
}
function showLogin() {
  if(state.auth_method==='google'){showGoogleLogin();return;}
  const box=page('이메일로 가입·로그인',{step:state.draft?'write':null,subject:state.options});
  box.append(note('메일은 데이터이음 주소로 발송하고, 답장은 인증한 이메일로 받습니다. 비밀번호 없이 인증번호로 로그인합니다.'));
  const form=el('form','coop-form'), values={email:state.member?.email||'',name:state.member?.name||'',organization:state.member?.organization||'개인'};
  let loginOnly=false;
  const profile=el('div','coop-form');
  choices(form,'계정 선택',[['signup','처음 가입'],['login','기존 회원 로그인']],state.member?'login':'signup',key=>{
    loginOnly=key==='login';profile.hidden=loginOnly;nameInput.required=!loginOnly;
  },'segmented');
  field(form,'이메일',values.email,v=>values.email=v,{type:'email',max:254,required:true,autocomplete:'email'});
  const nameInput=field(profile,'이름',values.name,v=>values.name=v,{max:100,required:true,autocomplete:'name'});
  field(profile,'소속',values.organization,v=>values.organization=v,{max:200,autocomplete:'organization',hint:'개인은 개인'});form.append(profile);
  if(state.member){loginOnly=true;profile.hidden=true;nameInput.required=false;}
  form.append(note('기존 회원의 이름과 소속은 로그인 입력으로 바뀌지 않습니다. 회원정보에서 수정할 수 있습니다.'));
  const send=submit('인증번호 받기'),sendBar=el('div','coop-actions');sendBar.append(send);form.append(sendBar);box.append(form);
  let challenge, nextSend=0;
  const codeArea=el('div','coop-form');box.append(codeArea);
  form.addEventListener('submit',event=>{event.preventDefault();run(async()=>{
    if(Date.now()<nextSend)throw new Error(t('인증번호 재전송은 60초 후 가능합니다.'));
    challenge=await api('/api/account/code/start',{method:'POST',body:{...values,name:loginOnly?'':values.name}});nextSend=Date.now()+60000;
    send.textContent=t('인증번호 다시 받기');send.className='coop-button';codeArea.replaceChildren();
    codeArea.append(note('메일함의 인증번호 6자리를 입력해 주세요. 10분 동안 유효하며 스팸함도 확인해 주세요.'));
    let code='';const verifyForm=el('form','coop-form coop-code');
    const input=field(verifyForm,'인증번호',code,v=>code=v,{max:6,required:true,autocomplete:'one-time-code'});input.inputMode='numeric';input.pattern='[0-9]{6}';
    verifyForm.append(footer(submit('인증하고 계속하기')));codeArea.append(verifyForm);input.focus();
    verifyForm.addEventListener('submit',event=>{event.preventDefault();run(async()=>{
      const me=await api('/api/account/code/verify',{method:'POST',body:{challenge_id:challenge.challenge_id,code}});
      updateAccount(state,me);if(entry)entry.textContent=t('내 요청');
      if(state.draft)showForm();else await showHistory();
    });});
  });});
}
function showGoogleLogin() {
  const box=page('내 Gmail로 메일 보내기',{step:state.draft?'write':null,subject:state.options});
  facts(box,['선택한 자료의 담당기관에 본인 Gmail로 보냅니다.','기관의 답장도 같은 Gmail 메일함으로 받습니다.',
    '메일 발송 권한만 요청하며 받은편지함은 읽지 않습니다.','연결이 만료되면 다시 연결해 주세요.']);
  box.append(footer(button('Google 계정 연결',async()=>{
    // Open synchronously during the click so popup blockers do not lose the user gesture.
    const popup=window.open('about:blank','_blank');if(popup)popup.opener=null;
    const generation=state.generation, previousCsrf=state.csrf;
    let result;
    try {result=await api('/api/account/google/start',{method:'POST',body:{}});}
    catch(e){popup?.close();throw e;}
    if(generation!==state.generation){popup?.close();return;}
    const url=new URL(result.url);
    if(url.origin!=='https://accounts.google.com' || url.pathname!=='/o/oauth2/v2/auth'){
      popup?.close();throw new Error(t('Google 연결 주소를 확인할 수 없습니다.'));
    }
    const waiting=page('Google 연결을 기다리고 있어요',{step:state.draft?'write':null});
    const waitingGeneration=viewGeneration;
    const pulse=el('p','coop-waiting');pulse.append(copy('span','','열린 Google 창에서 계정을 선택하고 발송 권한을 허용해 주세요.'));waiting.append(pulse);
    waiting.append(note('연결을 마치면 이 창이 자동으로 다음 단계로 넘어갑니다.'));
    if(popup)popup.location.href=url.href;
    else {waiting.append(note('새 창이 차단되었습니다. 아래 링크로 연결해 주세요.'));link(waiting,'Google 연결 창 열기',url.href,'coop-button coop-primary');}
    const deadline=Date.now()+600000;
    async function checkConnection() {
      if(!dialog.open || generation!==state.generation || waitingGeneration!==viewGeneration || Date.now()>deadline)return false;
      // Poll without minting cookies: a response begun before OAuth rotation must not overwrite the new session.
      const me=await api('/api/account/google/status');
      if(!dialog.open || generation!==state.generation || waitingGeneration!==viewGeneration)return false;
      updateAccount(state,me);if(entry)entry.textContent=t(state.member?'내 요청':'가입·로그인');
      if(me.gmail_connected && me.csrf!==previousCsrf){
        state.preview=null;if(state.draft)showForm();else await showHistory();return true;
      }
      return false;
    }
    waiting.append(footer(button('다시 연결하기',showGoogleLogin),button('연결 확인',async()=>{if(!await checkConnection())throw new Error(t('아직 연결되지 않았습니다. Google 창에서 연결을 마쳐 주세요.'));},'coop-button coop-primary')));
    const schedule=()=>{poll=setTimeout(async()=>{
      if(!dialog.open || generation!==state.generation || waitingGeneration!==viewGeneration || Date.now()>deadline)return;
      try {if(await checkConnection())return;}catch { /* A transient read failure is retried, never a send. */ }
      if(dialog.open && generation===state.generation && waitingGeneration===viewGeneration)schedule();
    },2000);};schedule();
  },'coop-button coop-primary')));
}
function showForm() {
  if(!state.draft){return state.member?showHistory():showLogin();}
  const draft=state.draft, options=state.options;
  if(!options){throw new Error(t('자료를 다시 선택해 주세요.'));}
  if(!options.available){
    const box=page('문의처 안내',{step:'data',subject:options});
    if(!showContactReference(box,options))box.append(note('공식 문의 페이지에서 담당기관과 신청 절차를 확인해 주세요.'));
    officialPage(box,options.contact_url);return;
  }
  if(!state.member || (state.auth_method==='google'&&!state.gmail_connected)){showLogin();return;}
  const box=page('어떤 메일을 보낼까요?',{step:'write',tab:'write',subject:options});
  const sender=el('p','coop-sender');sender.append(copy('span','','보내는 사람'),el('strong','',state.member.name),el('span','',state.member.organization),el('span','',state.member.email));
  box.append(sender);
  if(!options.types[draft.request_type])draft.request_type=Object.keys(options.types)[0];
  const form=el('form','coop-form');box.append(form);
  let scopeSection;
  choices(form,'메일 종류',Object.entries(options.types).map(([key,label])=>[key,label,TYPE_NOTES[key]]),draft.request_type,key=>{draft.request_type=key;scopeSection.hidden=key!=='data_provision';});
  const purposeChoices=Object.entries(PURPOSES).map(([key,[label]])=>[key,label]);
  purposeChoices.push(['custom','직접 입력']);
  const sentences=el('div','coop-sentences');
  const purpose=field(sentences,'사용 목적',draft.purpose,v=>draft.purpose=v,{max:2000,multiline:true,hint:'메일에 그대로 들어갑니다'});
  const usage=field(sentences,'사용 방식',draft.usage_description,v=>draft.usage_description=v,{max:2000,multiline:true});
  choices(form,'활용 목적',purposeChoices,draft.purpose_key,key=>{
    draft.purpose_key=key;if(key==='custom'){purpose.focus();}else{choosePurpose(draft,key);purpose.value=draft.purpose;usage.value=draft.usage_description;}
  },'chips');form.append(sentences);
  choices(form,'상업적 이용 여부',[['noncommercial','비상업적'],['commercial','상업적'],['undecided','미정']],draft.commercial_use,key=>draft.commercial_use=key,'segmented');
  scopeSection=el('section','coop-form');scopeSection.hidden=draft.request_type!=='data_provision';form.append(scopeSection);
  const scopeChoices=[];if(draft.scope_hint)scopeChoices.push([draft.scope_hint,'검색한 조건으로 요청',draft.scope_hint]);
  scopeChoices.push(['선택한 자료의 제공 가능 범위 전체','제공 가능한 범위 전체','기관에서 제공 가능한 범위를 문의합니다.'],['custom','범위 직접 입력','기간·지역·항목 등을 적습니다.']);
  const customScope=el('details','coop-edits');customScope.append(copy('summary','','요청 범위 확인 및 수정'));
  const scopeInput=field(customScope,'요청 범위',draft.requested_scope,v=>draft.requested_scope=v,{max:2000,multiline:true});
  choices(scopeSection,'요청할 자료 범위',scopeChoices,draft.requested_scope,key=>{if(key==='custom'){customScope.open=true;scopeInput.focus();}else{draft.requested_scope=key;scopeInput.value=key;}});scopeSection.append(customScope);
  const formatWrap=el('label','coop-field'),formatLabel=el('span','coop-field-label');
  formatLabel.append(copy('span','','희망 형식'),copy('small','','선택, 실제 형식은 기관이 안내'));formatWrap.append(formatLabel);
  const format=el('select');for(const value of [...new Set(['',...(options.dataset.format?[options.dataset.format]:[]),...draft.formats,'CSV','Excel','API'])]){
    const option=el('option','',value||t('협의 가능'));option.value=value;option.selected=draft.preferred_format===value;format.append(option);
  }format.addEventListener('change',()=>draft.preferred_format=format.value);formatWrap.append(format);scopeSection.append(formatWrap);
  if(options.template_status==='draft')form.append(note('양식 작성 중입니다. 미리보기는 가능하며 실제 발송은 아직 할 수 없습니다.'));
  form.append(footer(submit('메일 미리보기')));
  form.addEventListener('submit',event=>{event.preventDefault();run(async()=>{
    const input=previewInput(draft,state.preview);
    state.preview=await api('/api/cooperation/previews',{method:'POST',body:input});showPreview();
  });});
}
function mailDetails(box, value) {
  if(value.redacted){box.append(note('보관 기간이 지나 메일 본문과 당시 개인정보를 삭제했습니다.'));return;}
  const payload=value.payload, letter=el('article','coop-letter'), list=el('dl','coop-letter-head');
  letter.setAttribute('aria-label',t('보낼 메일'));
  for(const [label,text] of [['보내는 주소',payload.from],['수신 기관',value.institution],['받는 주소',payload.to.join(', ')],['답장받을 주소',payload.reply_to],['제목',payload.subject]]){
    const row=el('div','coop-letter-row');row.append(copy('dt','',label),el('dd','',text));list.append(row);
  }letter.append(list,el('pre','coop-mail-text',payload.text));box.append(letter);
}
function showPreview() {
  const value=state.preview,box=page('보낼 내용을 확인해 주세요',{step:'review',tab:'write'});mailDetails(box,value);
  box.append(note('이름·소속·이메일과 요청 내용을 위 기관에 전달합니다. 기관 답장은 이메일 메일함에서 확인할 수 있습니다.'));
  const consent=el('label','coop-consent'),check=el('input');check.type='checkbox';
  consent.append(check,copy('span','','메일 내용과 개인정보 전달을 확인하고 동의합니다.'));box.append(consent);
  const google=state.auth_method==='google';
  const send=button(google?'내 Gmail로 보내기':'이 내용으로 보내기',async()=>{
    if(!check.checked)return;const result=await api('/api/cooperation/requests/'+value.request_id+'/send',{method:'POST',body:{version:value.version,consent:true}});
    state.preview=result;await showRequest(result.request_id);
  },'coop-button coop-primary');send.disabled=true;check.addEventListener('change',()=>send.disabled=!check.checked || !value.sendable);
  if(google)box.append(note('위에 표시된 본인 Gmail 계정으로 한 번만 전송합니다.'));
  if(!value.sendable)box.append(note('아직 확정되지 않은 양식입니다. 실제 발송은 비활성화되어 있습니다.'));
  box.append(footer(button('수정하기',showForm),send));
}
async function showRequest(id) {
  const value=await api('/api/cooperation/requests/'+id),tone=TONES[value.status]||'muted';
  const box=page('내 요청 상태',{step:value.status==='draft'?'review':'sent',tone,tab:'history'});
  const status=copy('p','coop-status',STATUS[value.status]||value.status);status.setAttribute('data-tone',tone);
  box.append(status,el('p','coop-note',t('요청 번호')+': '+id));
  box.append(note('발송·전달 상태는 데이터 이용 허가를 의미하지 않습니다. 답장은 인증한 이메일의 메일함에서 확인해 주세요.'));
  if(state.auth_method==='google'&&value.status==='accepted')box.append(note('Gmail에서 발송을 확인했습니다. 수신 여부와 답장은 Gmail에서 확인해 주세요.'));
  if(state.auth_method==='google'&&value.status==='failed')box.append(note('Gmail에서 발송을 거절했습니다. 계정의 발송 권한과 사용 한도를 확인해 주세요.'));
  if(value.status==='unknown')box.append(note('메일이 이미 발송되었을 수 있습니다. 중복 발송을 피하기 위해 자동 재전송을 중단했습니다.'));
  mailDetails(box,value);box.append(footer(button('상태 새로고침',()=>showRequest(id))));
  if(['queued','sending','retrying'].includes(value.status)){
    const generation=state.generation;poll=setTimeout(()=>{if(dialog.open && generation===state.generation)run(()=>showRequest(id));},5000);
  }
}
async function showHistory(pageNumber=1) {
  const data=await api('/api/cooperation/requests?page='+pageNumber),box=page('내 요청',{tab:'history'});box.append(note('요청 본문과 당시 개인정보는 30일 동안 보관합니다. 기관 답장은 이메일에서 확인해 주세요.'));
  if(!data.requests.length)box.append(note('아직 보낸 요청이 없습니다. 검색 결과에서 자료를 선택해 주세요.'));
  const list=el('div','coop-history');
  for(const request of data.requests){
    const row=el('div','coop-history-row'),pill=copy('span','coop-pill',STATUS[request.status]||request.status);
    pill.setAttribute('data-tone',TONES[request.status]||'muted');
    row.append(el('span','coop-history-title',request.dataset?.title||t('보관 기간이 지난 요청')),pill,button('내용 보기',()=>showRequest(request.request_id),'coop-button coop-small'));list.append(row);
  }
  if(data.requests.length)box.append(list);
  const actions=el('div','coop-actions');if(pageNumber>1)actions.append(button('이전',()=>showHistory(pageNumber-1)));
  if(data.has_more)actions.append(button('다음',()=>showHistory(pageNumber+1)));if(actions.children.length)box.append(actions);
}
function showProfile() {
  const box=page('회원정보',{tab:'profile'}),form=el('form','coop-form');let name=state.member.name,organization=state.member.organization;
  box.append(el('p','coop-account',state.member.email),note('이메일은 인증된 회신 주소입니다. 이름과 소속은 직접 입력한 정보이며 기관 재직 인증을 뜻하지 않습니다.'));
  field(form,'이름',name,v=>name=v,{max:100,required:true});field(form,'소속',organization,v=>organization=v,{max:200,required:true});
  const logout=button('로그아웃',async()=>{await api('/api/account/logout',{method:'POST',body:{}});updateAccount(state,{member:null,csrf:null,enabled:state.enabled});await account();showLogin();});
  const saveBar=el('div','coop-actions');saveBar.append(logout,submit('회원정보 저장'));form.append(saveBar);box.append(form);
  form.addEventListener('submit',event=>{event.preventDefault();run(async()=>{const result=await api('/api/account/me',{method:'PATCH',body:{name,organization}});state.member=result.member;state.preview=null;showProfile();});});
  const details=el('details','coop-edits coop-danger');details.append(copy('summary','','회원 탈퇴'));
  details.append(note(state.auth_method==='google'?'프로필과 이 사이트의 Gmail 연결을 삭제합니다. 이미 전송된 메일은 회수할 수 없습니다. 탈퇴에는 10분 이내 Google 연결이 필요합니다.':'프로필·로그인 세션을 삭제하고 대기 중인 메일을 취소합니다. 이미 발송되었거나 발송 중인 메일은 회수할 수 없습니다. 탈퇴에는 10분 이내 이메일 인증이 필요합니다.'));
  const consent=el('label','coop-consent'),check=el('input');check.type='checkbox';consent.append(check,copy('span','','계정 삭제에 동의합니다.'));details.append(consent);
  const remove=button('회원 탈퇴하기',async()=>{if(!check.checked)return;await api('/api/account/me',{method:'DELETE',body:{consent:true}});state.preview=null;await account();showLogin();},'coop-button coop-destructive');
  remove.disabled=true;check.addEventListener('change',()=>remove.disabled=!check.checked);
  const actions=el('div','coop-actions');
  actions.append(state.auth_method==='google'?button('Google 계정 다시 연결',showGoogleLogin):button('이메일로 다시 인증',showLogin),remove);
  details.append(actions);box.append(details);
}
function bind(card,{datasetId,hints={}}) {
  if(typeof datasetId!=='string'||!datasetId||datasetId.length>2048)return;
  const control=copy('button','quiet-button coop-result-button','메일 보내기');control.type='button';
  control.addEventListener('click',()=>open({datasetId,hints}));card.append(control);return control;
}
function init() {
  if(document.body?.dataset.embeddedChat==='true')return;
  const header=document.querySelector('.header-actions')||document.querySelector('.topbar');
  if(!header)return;
  entry=copy('button','quiet-button','가입·로그인');entry.type='button';entry.addEventListener('click',()=>open());header.append(entry);
}
window.DataieumCooperation={open,bind,createDraft,choosePurpose,requestInput,searchHints,previewInput,updateAccount};
if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',init);else init();
})();
