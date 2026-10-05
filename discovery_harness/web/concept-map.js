/* Draw only the supplied ontology links; no ranking or inferred connections. */
window.ConceptMap = (() => {
  const names={population:'인구수',fertility_rate:'합계출산율',death_rate:'조사망률',net_migration:'국가 간 인구이동',elderly_share:'고령인구 비율'};
  const populationReasons={fertility_rate:'여성 1명당 출생아 수를 함께 봐요.',death_rate:'인구 1,000명당 사망자 수를 함께 봐요.',net_migration:'나라에 들어온 인구와 나간 인구의 차이예요.',elderly_share:'전체 인구 중 65세 이상이 차지하는 비율이에요.'};
  const name=(id,fallback)=>names[id]||fallback;
  const brief=value=>{const first=String(value||'').split(/(?<=[.!?。])\s/)[0];return first.length>72?first.slice(0,71)+'…':first;};
  const reason=(source,target,fallback)=>source==='population'&&populationReasons[target]||brief(fallback);
  function render(parent,{source,relations,onSelect}){
    const el=(tag,cls,text)=>{const n=document.createElement(tag);n.className=cls;if(text!==undefined)(window.DataieumI18n.unbind(n),n.textContent=text);return n;};
  const ui=(tag,cls,source,params={})=>window.DataieumI18n.bind(el(tag,cls),source,params);
    const map=el('div','concept-map');map.setAttribute('role','group');window.DataieumI18n.bind(map,'{name} 관계 그래프',{name:name(source.id,source.label)},'aria-label');
    const ns='http://www.w3.org/2000/svg',lines=document.createElementNS(ns,'svg');lines.setAttribute('class','concept-map-lines');lines.setAttribute('viewBox','0 0 100 100');lines.setAttribute('preserveAspectRatio','none');lines.setAttribute('aria-hidden','true');map.append(lines);
    const nodes=[];
    const node=(item,x,y,root=false)=>{
      const button=el('button','concept-map-node'+(root?' concept-map-source':''));button.type='button';button.style.left=x+'%';button.style.top=y+'%';button.dataset.indicator=item.id;
      window.DataieumI18n.bind(button,'{name} 노드',{name:name(item.id,item.label)},'aria-label');button.setAttribute('aria-pressed','false');if(root)window.DataieumI18n.bind(button,'선택해서 개념과 연결된 자료 보기',{},'title');else button.title=reason(source.id,item.id,item.reason);
      if(root)button.append(ui('span','concept-map-caption',"중심 개념",{}));
      button.append(el('strong','',name(item.id,item.label)));
      button.addEventListener('click',()=>{for(const peer of nodes)peer.setAttribute('aria-pressed',String(peer===button));onSelect(item);});nodes.push(button);map.append(button);
    };
    relations.forEach((related,index)=>{
      const angle=-Math.PI/2+index*Math.PI*2/relations.length,x=50+34*Math.cos(angle),y=50+33*Math.sin(angle);
      const line=document.createElementNS(ns,'line');for(const [key,value] of Object.entries({x1:50,y1:50,x2:x,y2:y}))line.setAttribute(key,String(value));lines.append(line);node(related,x,y);
    });
    node(source,50,50,true);parent.replaceChildren(map);
    return map;
  }
  return {name,brief,reason,render};
})();
