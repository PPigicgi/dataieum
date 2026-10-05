/* Only direct parents/children are displayed. Every navigation is one step. */
window.HierarchyMap = {
  context(plan,id,origin) {
    if(!plan)return null;
    const next=structuredClone(plan),need=next.needs.find(n=>n.indicator===origin);
    if(plan.selected_concept!==id)delete next.semantic_query;
    if(need)next.needs=[need];next.selected_concept=id;return next;
  },
  async load() {
    if(this.dictionary)return this.dictionary;
    if(this.pending)return this.pending;
    this.pending=(async()=>{const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),8000);try{const r=await fetch('/api/ontology/hierarchy',{signal:controller.signal});if(!r.ok)throw Error('hierarchy unavailable');const value=await r.json();this.dictionary=value;return value;}finally{clearTimeout(timer);this.pending=null;}})();
    return this.pending;
  },
  render(parent, {selected, dictionary, onSelect, onInspect}) {
    parent.replaceChildren();
    const el=(tag,cls,text)=>{const n=document.createElement(tag);n.className=cls;if(text)(window.DataieumI18n.unbind(n),n.textContent=text);return n;};
  const ui=(tag,cls,source,params={})=>window.DataieumI18n.bind(el(tag,cls),source,params);
    const nodes=new Map(dictionary.nodes.map(n=>[n.id,n]));
    const current=nodes.get(selected);if(!current)return;
    const graph=el('div','hierarchy-map');
    const nodeButton=(node,edge,root=false)=>{
      const b=el('button','hierarchy-node'+(root?' hierarchy-current':''));b.type='button';
      b.dataset.concept=node.id;window.DataieumI18n.bind(b,root?'{name} 개념 정보':'{name} 개념으로 이동',{name:node.label},'aria-label');
      b.append(el('strong','',node.label));
      if(edge)b.append(el('span','hierarchy-restriction',edge.restriction));
      else b.append(ui('span','hierarchy-restriction',"선택한 개념 · 정의 보기",{}));
      b.addEventListener('click',()=>root?onInspect():onSelect(node.id));return b;
    };
    const band=(label,edges,side,empty)=>{
      const section=el('section','hierarchy-band');window.DataieumI18n.bind(section,label,{},'aria-label');
      section.append(ui('h3','hierarchy-label','{name} · {count}',()=>({name:window.DataieumI18n.text(label),count:window.DataieumI18n.number(edges.length)})));
      const row=el('div','hierarchy-row');let count=0;
      const more=ui('button','hierarchy-more',"더 펼치기",{});more.type='button';
      const append=()=>{for(const e of edges.slice(count,count+8))row.append(nodeButton(nodes.get(e[side]),e));count+=8;more.hidden=count>=edges.length;};
      if(edges.length)append();else row.append(ui('p','hierarchy-empty',empty));
      more.addEventListener('click',()=>{const before=row.children.length;append();row.children[before]?.focus();});
      more.hidden=edges.length<=8;section.append(row,more);return section;
    };
    graph.append(band('상위 개념',dictionary.edges.filter(e=>e.source===selected),'target','정의된 상위 개념이 아직 없어요.'));
    graph.append(ui('div','hierarchy-connector',"↓ 의미를 좁히면",{}));
    const center=el('div','hierarchy-center');center.append(nodeButton(current,null,true));graph.append(center);
    graph.append(ui('div','hierarchy-connector',"↓ 조건을 더하면",{}));
    graph.append(band('하위 개념',dictionary.edges.filter(e=>e.target===selected),'source','정의된 하위 개념이 아직 없어요.'));
    parent.append(graph);
  }
};
