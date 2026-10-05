/* Display-only localization. Never changes queries, records, graph IDs or jobs. */
(() => {
  'use strict';
  const data=window.DataieumLocaleData||{messages:{},topics:{},metadata:[]};
  const hub=document.querySelector('meta[name="dataieum-brand"]')?.content==='datahub';
  const storageKey='dataieum.locale',valid=value=>value==='ko'||value==='en';
  let locale='ko';
  try{const saved=localStorage.getItem(storageKey);if(valid(saved))locale=saved;}catch{}
  const listeners=new Set(),bindings=new Map();let pruneTimer=null;
  const attributes=new Set(['placeholder','title','aria-label','aria-description']);
  const interpolate=(value,params)=>String(value).replace(/\{([a-zA-Z_][\w]*)\}/g,(match,key)=>Object.hasOwn(params,key)?String(params[key]):match);
  function text(source,params={}){
    const value=String(source??''),canonical=hub?value.replaceAll('데이터 허브','데이터이음').replaceAll('Data Hub','Dataieum'):value;
    const entry=Object.hasOwn(data.messages,canonical)?data.messages[canonical]:null;
    const translated=entry?.[locale]??canonical;
    const branded=hub?translated.replaceAll('데이터이음',locale==='en'?'Data Hub':'데이터 허브').replaceAll('Dataieum','Data Hub'):translated;
    return interpolate(branded,typeof params==='function'?params():params||{});
  }
  const number=value=>typeof value==='number'&&Number.isFinite(value)?new Intl.NumberFormat(locale==='en'?'en-US':'ko-KR').format(value):'—';
  function topic(id,original,field='name'){
    const entry=Object.hasOwn(data.topics,id)?data.topics[id]:null;
    return entry?.original?.[field]===original?entry[locale]?.[field]??original:original;
  }
  function field(entityType,id,name,original){
    const raw=String(original??'');
    const entry=data.metadata.find(row=>row.entityType===entityType&&row.id===id&&row.field===name&&row.targetLanguage===locale&&row.original===raw);
    return {text:entry?.text??raw,original:raw,translated:!!entry,status:entry?'reviewed':'original'};
  }
  function applyBinding(element,binding){
    const value=text(binding.source,binding.params);
    if(binding.attribute)element.setAttribute(binding.attribute,value);else element.textContent=value;
  }
  function bind(element,source,params={},attribute=null){
    if(!element)return element;
    if(attribute!==null&&!attributes.has(attribute))throw new TypeError('Only display attributes may be translated');
    const values=bindings.get(element)||new Map();
    const binding={source,params,attribute};values.set(attribute,binding);bindings.set(element,values);
    if(pruneTimer===null&&typeof window.setTimeout==='function')pruneTimer=window.setTimeout(()=>{
      pruneTimer=null;for(const node of bindings.keys())if(!node.isConnected)bindings.delete(node);
    },0);
    applyBinding(element,binding);return element;
  }
  function localize(root=document){
    const selectors=['[data-i18n]',...Array.from(attributes,attribute=>'[data-i18n-'+attribute+']')].join(',');
    const nodes=[...(root.matches?.(selectors)?[root]:[]),...root.querySelectorAll(selectors)];
    for(const element of nodes){
      let params={};try{params=JSON.parse(element.getAttribute('data-i18n-params')||'{}');}catch{}
      if(!params||typeof params!=='object'||Array.isArray(params))params={};
      if(element.hasAttribute('data-i18n'))bind(element,element.getAttribute('data-i18n'),params);
      for(const attribute of attributes)if(element.hasAttribute('data-i18n-'+attribute))bind(element,element.getAttribute('data-i18n-'+attribute),params,attribute);
    }
  }
  function unbind(element,attribute=null){
    const values=bindings.get(element);if(!values)return;
    values.delete(attribute);if(!values.size)bindings.delete(element);
  }
  const titleSource=document.title;
  let controls=null;
  function refresh(){
    document.documentElement.lang=locale;document.documentElement.dataset.locale=locale;
    document.title=text(titleSource);
    for(const [element,values] of bindings){
      if(!element.isConnected){bindings.delete(element);continue;}
      for(const binding of values.values())applyBinding(element,binding);
    }
    if(controls){
      controls.setAttribute('aria-label',text('화면 언어'));
      for(const button of controls.querySelectorAll('button'))button.setAttribute('aria-pressed',String(button.lang===locale));
    }
  }
  function setLocale(value){
    if(!valid(value))return false;
    if(value===locale)return true;
    locale=value;try{localStorage.setItem(storageKey,value);}catch{}
    refresh();
    for(const callback of [...listeners])callback(locale);
    window.dispatchEvent(new CustomEvent('dataieum:locale-change',{detail:{locale}}));
    return true;
  }
  function mount(){
    const embedded=document.body.dataset.embeddedChat==='true';
    document.body.dataset.i18nPage=location.pathname.startsWith('/chat')?'chat':location.pathname.startsWith('/ontology')?'ontology':'catalogue';
    {
      controls=document.createElement('div');controls.className='dataieum-language';controls.setAttribute('role','group');
      for(const [value,label] of [['ko','한국어'],['en','English']]){
        const button=document.createElement('button');button.type='button';button.lang=value;button.textContent=label;
        button.addEventListener('click',()=>setLocale(value));controls.append(button);
      }
      document.body.append(controls);
    }
    localize();refresh();
  }
  window.DataieumI18n={text,number,topic,field,bind,unbind,localize,getLocale:()=>locale,setLocale,
    subscribe(callback){listeners.add(callback);return ()=>listeners.delete(callback);}};
  document.documentElement.lang=locale;document.documentElement.dataset.locale=locale;
  document.title=text(titleSource);
  window.addEventListener('storage',event=>{if(event.key===storageKey&&valid(event.newValue))setLocale(event.newValue);});
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',mount,{once:true});else mount();
})();
