/* Read title and region translations; originals remain in storage. */
(() => {
  'use strict';
  const entries = new Set(), cache = new Map(), attempted = new Map();
  let pending = false, scheduled = false, stopped = false, controller = null;
  const lang = () => window.DataieumI18n?.getLocale() === 'en' ? 'en' : 'ko';
  const copy = (ko,en) => lang()==='en'?en:ko;
  const normalize = value => String(value||'').replace(/\s+/g,' ').trim();
  const signature = record => JSON.stringify([record.fingerprint||'',record.region||'',record.country||'',record.coverage||'',record.spatial||'']);
  const key = entry => JSON.stringify([entry.id,normalize(entry.originalTitle),normalize(entry.originalDescription),entry.sourceSignature]);
  function prune(){
    for(const entry of entries)if(!entry.host.isConnected){entries.delete(entry);observer.unobserve(entry.host);}
    while(cache.size>150)cache.delete(cache.keys().next().value);
    while(attempted.size>150)attempted.delete(attempted.keys().next().value);
  }
  function saved(entry){const value=cache.get(key(entry));return value&&value.until>Date.now()?value.row:null;}
  function paint(entry){
    if(!entry.host.isConnected)return;
    const row=saved(entry), localized=row?.available&&row.brief!==true?row[lang()]:null;
    const region=row?.region?.[lang()]||'';
    entry.title.textContent=localized?.title||entry.originalTitle;
    entry.title.setAttribute('lang',localized?lang():'');
    entry.summary.textContent=entry.regionField?'':region;
    entry.summary.hidden=!entry.summary.textContent;
    if(entry.regionField){entry.regionField.value.textContent=region;entry.regionField.row.hidden=!region;}
    entry.original.hidden=!entry.originalTitle;
    entry.original.querySelector('summary').textContent=copy('원문 보기','View original');
    entry.host.dataset.introState=localized?'ready':'original';
  }
  function schedule(){if(scheduled||stopped)return;scheduled=true;setTimeout(()=>{scheduled=false;void flush();},60);}
  const observer=new IntersectionObserver(changes=>{
    for(const change of changes){const entry=[...entries].find(e=>e.host===change.target);if(entry){entry.visible=change.isIntersecting;if(entry.visible)paint(entry);}}
    schedule();
  },{rootMargin:'60px'});
  async function flush(){
    prune();if(pending||stopped)return;
    const batch=[];
    for(const entry of entries){
      if(entry.visible&&!saved(entry)&&!attempted.has(key(entry))&&!batch.some(e=>e.id===entry.id)){
        const query=new URLSearchParams({ids:JSON.stringify([...batch.map(e=>e.id),entry.id])}).toString();
        if(query.length>8192){if(batch.length)break;attempted.set(key(entry),'oversized');continue;}
        batch.push(entry);
      }
      if(batch.length===5)break;
    }
    if(!batch.length)return;
    pending=true;controller=new AbortController();const active=controller;
    const timer=setTimeout(()=>active.abort(),3000);
    for(const e of batch)attempted.set(key(e),'loading');
    for(const e of entries)paint(e);
    try{
      const query=new URLSearchParams({ids:JSON.stringify(batch.map(e=>e.id))});
      const response=await fetch('/api/dataset-intros?'+query,{headers:{'X-Dataieum-Chat':'1'},signal:active.signal});
      if(!response.ok)throw new Error('Introduction unavailable');
      const text=await response.text();if(text.length>65536)throw new Error('Oversized introductions');
      const data=JSON.parse(text);
      if(!Array.isArray(data.items)||data.items.length!==batch.length||new Set(data.items.map(r=>r.id)).size!==batch.length||
          data.items.some(r=>!batch.some(e=>e.id===r.id)||typeof r.available!=='boolean'||
            (r.brief!==undefined&&typeof r.brief!=='boolean')||
            (r.region!==undefined&&(!r.region||typeof r.region!=='object'||['ko','en'].some(l=>
              typeof r.region[l]!=='string'||r.region[l].length>160||/[\u0000-\u001f\ufffd<>]/.test(r.region[l])||
              (r.region[l]&&(l==='ko'?!/[가-힣]/.test(r.region[l]):!/[A-Za-z]/.test(r.region[l])||/[가-힣]/.test(r.region[l]))))))||['ko','en'].some(l=>
            !r[l]||typeof r[l].title!=='string'||typeof r[l].summary!=='string'||r[l].title.length>512||r[l].summary.length>240||
            /[\u0000-\u001f\ufffd]/.test(r[l].title+r[l].summary)||
            (r.brief===true&&r[l].summary&&(l==='ko'?!/[가-힣]/.test(r[l].summary):!/[A-Za-z]/.test(r[l].summary)||/[가-힣]/.test(r[l].summary)))||
            (r.available&&(!r[l].title.trim()||
              (l==='ko'?!/[가-힣]/.test(r[l].title):!/[A-Za-z]/.test(r[l].title)||/[가-힣]/.test(r[l].title)))))))throw new Error('Invalid introductions');
      for(const row of data.items){const e=batch.find(e=>e.id===row.id);cache.set(key(e),{row,until:Date.now()+600000});attempted.delete(key(e));}
    }catch{
      for(const e of batch)attempted.set(key(e),'failed');
      // Keep the original title on storage errors; never request generation.
    }finally{
      clearTimeout(timer);pending=false;controller=null;prune();for(const e of entries)paint(e);schedule();
    }
  }
  function bind(host,record,title,summary){
    if(!host||!record?.id||!record.title||!title)return;
    for(const previous of entries)if(previous.host===host){
      if(previous.title===title&&previous.id===record.id&&previous.originalTitle===record.title&&previous.originalDescription===(record.description||'')&&previous.sourceSignature===signature(record))return;
      entries.delete(previous);observer.unobserve(host);
      previous.original.remove();if(previous.ownsSummary)previous.summary.remove();
    }
    host.dataset.introBound='true';
    const ownsSummary=!summary;
    if(!summary){summary=document.createElement('p');(title.closest('h3,button')||title).after(summary);}
    summary.classList.add('dataset-intro-summary');
    const original=document.createElement('details');original.className='metadata-original dataset-intro-original';original.hidden=true;
    const caption=document.createElement('summary'),sourceTitle=document.createElement('p');
    sourceTitle.textContent=record.title;
    original.append(caption,sourceTitle);
    const term=[...host.querySelectorAll('dt')].find(e=>['지역','Region'].includes(normalize(e.textContent)));
    const regionField=term?.nextElementSibling?{row:term.parentElement,value:term.nextElementSibling}:null;
    const entry={host,id:record.id,title,summary,original,regionField,ownsSummary,originalTitle:record.title,originalDescription:record.description||'',sourceSignature:signature(record),visible:false};
    original.addEventListener('click',event=>event.stopPropagation());
    host.querySelectorAll('.metadata-original-label').forEach(e=>e.remove());
    const secondary=host.querySelector('.site-result-secondary');
    if(secondary){original.open=true;secondary.append(original);}else (summary.closest('button')||summary).after(original);
    entries.add(entry);observer.observe(host);paint(entry);schedule();
  }
  window.DataieumIntros={bind};
  window.addEventListener('dataieum:locale-change',()=>queueMicrotask(()=>{prune();for(const e of entries)paint(e);}));
  window.addEventListener('pagehide',()=>{stopped=true;controller?.abort();observer.disconnect();});
  window.addEventListener('pageshow',()=>{stopped=false;for(const e of entries)observer.observe(e.host);schedule();});
})();
