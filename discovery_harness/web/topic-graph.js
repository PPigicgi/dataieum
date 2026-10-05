/* Bounded views of the persisted topic taxonomy and dataset classifications. */
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
  const topicCopy = (id, original, field = 'name') => copy('{value}', () => ({value: i18n?.topic(id, original, field) || original}));
  const fieldCopy = (type, id, key, original) => copy('{value}', () => ({value: i18n?.field(type, id, key, original)?.text || original}));
  const list = value => Array.isArray(value) ? value : [];
  const validTopic = value => value && typeof value.id === 'string' && typeof value.label === 'string';
  const safeURL = value => {
    try {
      const url = new URL(value);
      return ['https:', 'http:'].includes(url.protocol) && !url.username && !url.password ? url.href : null;
    } catch { return null; }
  };
  function taxonomy(topics) {
    const unique = new Map(list(topics).filter(validTopic).map(topic => [topic.id, topic]));
    for (const topic of [...unique.values()]) {
      if (typeof topic.parent_id === 'string' && typeof topic.parent_label === 'string' && !unique.has(topic.parent_id)) {
        unique.set(topic.parent_id, {id: topic.parent_id, label: topic.parent_label, kind: 'category'});
      }
    }
    return unique;
  }
  function buildOverview(topics, parentId = null, limit = 40, band = '') {
    const filtered = ['high', 'low'].includes(band) ? list(topics).filter(topic => topic[band + '_links'] > 0) : topics;
    const unique = taxonomy(filtered);
    const all = [...unique.values()];
    const roots = all.filter(topic => !unique.has(topic.parent_id));
    const candidates = parentId ? all.filter(topic => topic.id === parentId || topic.parent_id === parentId).sort((a, b) => Number(b.id === parentId) - Number(a.id === parentId)) :
      roots.some(topic => topic.kind === 'category') ? roots : [...roots, ...all.filter(topic => unique.has(topic.parent_id))];
    const max = Math.max(1, Math.min(60, limit));
    const selected = candidates.slice(0, max);
    const visible = new Set(selected.map(topic => topic.id));
    return {
      nodes: selected.map(topic => ({...topic, kind: topic.kind === 'category' ? 'category' : 'topic'})),
      edges: selected.filter(topic => visible.has(topic.parent_id)).map(topic => ({source: topic.parent_id, target: topic.id, relation: 'parent'})),
      total: candidates.length, truncated: candidates.length > selected.length,
    };
  }
  function classifiedFor(dataset, topicId) {
    return dataset && dataset.classification?.level !== 'unclassified' &&
      list(dataset.topics).some(topic => topic.id === topicId && topic.level !== 'unclassified');
  }
  function buildNeighborhood(detail) {
    if (!validTopic(detail?.topic)) return {nodes: [], edges: [], total: 0, truncated: false};
    const topic = detail.topic;
    const datasets = list(detail.datasets).filter(dataset => typeof dataset.id === 'string' && classifiedFor(dataset, topic.id));
    const related = list(detail.related_topics).filter(item => validTopic(item) && item.id !== topic.id && item.relation === 'co_classified');
    const nodes = [{...topic, kind: 'topic'},
      ...datasets.slice(0, 10).map(dataset => ({...dataset, id: 'dataset:' + dataset.id, dataset_id: dataset.id, label: dataset.title || dataset.id, kind: 'dataset'})),
      ...related.slice(0, 6).map(item => ({...item, kind: 'topic'}))];
    return {nodes, edges: nodes.slice(1).map(node => ({source: topic.id, target: node.id,
      relation: node.kind === 'dataset' ? 'classified' : 'co_classified'})),
    total: 1 + datasets.length + related.length, truncated: datasets.length > 10 || related.length > 6 || !!detail.has_more};
  }
  function resultDatasetIds(result) {
    if (result?.state === 'clarify') return [];
    const ids = list(result?.groups).flatMap(group => list(group.sites).flatMap(site => list(site.evidence).map(item => item.dataset_id)));
    return [...new Set(ids.filter(id => typeof id === 'string' && id.length > 0 && id.length <= 512))].slice(0, 20);
  }
  function mount() {
    const $ = id => document.getElementById(id);
    if (!$('topic-graph')) return null;
    const canvas = $('topic-graph'), inspector = $('topic-inspector'), content = $('topic-inspector-content');
    const number = new Intl.NumberFormat('ko-KR');
    let topics = [], selectedId = null, sequence = 0, request = null, scale = 1, band = '';
    let taxonomyPromise = null, taxonomyReady = false;
    let lastFocus = null, lastResult = null, resultDatasets = [], lastAction = null;
    const el = (tag, cls, text) => {
      const node = document.createElement(tag);
      if (cls) node.className = cls;
      if (text !== undefined) { if (text && typeof text === 'object') ui(node, text); else node.textContent = text; }
      return node;
    };
    const metadataNotes = new Map();
    let pruneMetadata = null;
    i18n?.subscribe(() => {
      for (const [node, refresh] of metadataNotes) {
        if (node.isConnected === false) metadataNotes.delete(node); else refresh();
      }
    });
    function metadataInfo(card, fields) {
      const notice = el('p', 'topic-note metadata-language-note');
      const original = el('details', 'metadata-original');
      original.append(el('summary', '', copy('원문 보기')));
      const entries = fields.filter(item => item.original).map(item => {
        const row = el('p', 'metadata-original-field');
        row.append(el('strong', '', copy(item.label)), el('span', '', ' · ' + item.original));
        original.append(row); return {item, row};
      });
      const refresh = () => {
        let datasetOriginal = 0, datasetTranslated = 0, translated = 0;
        for (const {item, row} of entries) {
          const hasTranslation = i18n?.field(item.type, item.id, item.key, item.original)?.translated === true;
          row.hidden = !hasTranslation;
          if (hasTranslation) translated++;
          if (item.type === 'dataset') hasTranslation ? datasetTranslated++ : datasetOriginal++;
        }
        original.hidden = translated === 0;
        notice.hidden = datasetOriginal === 0;
        const source = datasetTranslated ? '번역되지 않은 항목은 제공처 원문으로 표시돼요.' : '자료의 제목과 설명은 제공처 원문으로 표시돼요.';
        notice.textContent = i18n?.text(source) || source;
      };
      metadataNotes.set(notice, refresh);card.append(notice, original);refresh();
      if (pruneMetadata === null) pruneMetadata = setTimeout(() => {
        pruneMetadata = null;
        for (const node of metadataNotes.keys()) if (node.isConnected === false) metadataNotes.delete(node);
      }, 0);
    }
    function button(label, action, cls = 'topic-action') {
      const node = el('button', cls, label); node.type = 'button'; node.addEventListener('click', action); return node;
    }
    function setStatus(message, failed = false) {
      ui($('graph-status'), message);
      $('graph-retry').hidden = !failed;
    }
    async function read(url, navigation = true) {
      const current = new AbortController();
      if (navigation) { request?.abort(); request = current; }
      const timeout = setTimeout(() => current.abort(), 12000);
      try {
        const response = await fetch(url, {signal: current.signal, cache: 'no-store'});
        if (!response.ok) throw new Error('unavailable');
        const value = await response.json();
        if (value?.ready !== true) throw new Error('preparing');
        return value;
      } finally { clearTimeout(timeout); }
    }
    function openInspector(title) {
      lastFocus = document.activeElement;
      inspector.hidden = false;
      ui($('topic-inspector-title'), title);
      $('graph-stage').classList.add('inspector-open');
      content.replaceChildren();
      window.dispatchEvent(new Event('topic-inspector-open'));
      $('close-topic-inspector').focus({preventScroll: true});
    }
    function closeInspector(restoreFocus = true) {
      inspector.hidden = true;
      $('graph-stage').classList.remove('inspector-open');
      if (restoreFocus) {
        const replacement = [...canvas.querySelectorAll('.topic-node')].find(node => node.dataset.nodeId === selectedId);
        (lastFocus?.isConnected ? lastFocus : replacement || $('graph-overview')).focus({preventScroll: true});
      }
    }
    function link(label, value) {
      const url = safeURL(value), node = el(url ? 'a' : 'span', 'topic-link', label);
      if (url) { node.href = url; node.target = '_blank'; node.rel = 'noopener noreferrer'; }
      return node;
    }
    function datasetCard(dataset, compact = false) {
      const card = el('article', 'topic-dataset');
      const heading = el('h3'); heading.append(link(fieldCopy('dataset', dataset.dataset_id || dataset.id, 'title', dataset.title || dataset.id), dataset.url)); card.append(heading);
      const meta = [dataset.source_name || dataset.source_id, ...list(dataset.formats)].filter(Boolean);
      if (meta.length) card.append(el('p', 'topic-dataset-meta', copy('{value}', () => ({value: [i18n?.field('source', dataset.source_id, 'name', dataset.source_name || dataset.source_id)?.text || dataset.source_name || dataset.source_id, ...list(dataset.formats)].filter(Boolean).join(' · ')}))));
      if (dataset.classification?.level === 'unclassified') card.append(el('span', 'topic-badge', copy('주제 미분류')));
      if (dataset.description) card.append(el('p', compact ? 'topic-description compact' : 'topic-description', fieldCopy('dataset',dataset.dataset_id || dataset.id,'description',dataset.description)));
      metadataInfo(card,[{type:'dataset',id:dataset.dataset_id || dataset.id,key:'title',original:dataset.title || dataset.id,label:'제목'},
        {type:'dataset',id:dataset.dataset_id || dataset.id,key:'description',original:dataset.description,label:'설명'},
        {type:'source',id:dataset.source_id,key:'name',original:dataset.source_name || dataset.source_id,label:'제공 사이트'}]);
      if (!compact) {
        const links = list(dataset.topics).filter(validTopic);
        if (links.length && dataset.classification?.level !== 'unclassified') {
          const group = el('div', 'topic-related');
          for (const topic of links.slice(0, 8)) group.append(button(topicCopy(topic.id,topic.label), () => selectTopic(topic.id)));
          card.append(el('p', 'topic-dataset-meta', copy('분류된 주제')), group);
        }
        if (safeURL(dataset.url)) card.append(link(copy('공식 자료 페이지 ↗'), dataset.url));
      }
      return card;
    }
    function inspectDataset(dataset) {
      openInspector('자료 정보');
      content.append(datasetCard({...dataset, id: dataset.dataset_id || dataset.id}));
      content.append(el('p', 'topic-note', copy('주제 연결은 분류 정보예요. 질문의 국가·기간·이용조건은 대화의 검색 결과에서 확인해 주세요.')));
    }
    function positions(graph) {
      const pos = new Map(), width = 1000, height = 600;
      const incoming = new Set(graph.edges.filter(edge => edge.relation === 'parent').map(edge => edge.target));
      const roots = graph.nodes.filter(node => !incoming.has(node.id));
      const neighborhood = graph.edges.some(edge => edge.relation !== 'parent');
      if (graph.result) {
        const top = graph.nodes.filter(node => node.kind !== 'dataset'), bottom = graph.nodes.filter(node => node.kind === 'dataset');
        top.forEach((node, index) => pos.set(node.id, {x: (index % 8 + 1) * width / (Math.min(8, top.length) + 1), y: 75 + Math.floor(index / 8) * 95}));
        bottom.forEach((node, index) => {
          const row = Math.floor(index / 5), columns = Math.min(5, bottom.length - row * 5);
          pos.set(node.id, {x: (index % 5 + 1) * width / (columns + 1), y: 280 + row * 88, columns});
        });
      } else if (neighborhood) {
        pos.set(graph.nodes[0].id, {x: width / 2, y: height / 2});
        graph.nodes.slice(1).forEach((node, index, rest) => {
          const angle = -Math.PI / 2 + index / rest.length * Math.PI * 2;
          pos.set(node.id, {x: width / 2 + Math.cos(angle) * 375, y: height / 2 + Math.sin(angle) * 225});
        });
      } else if (roots.length === 1) {
        pos.set(roots[0].id, {x: width / 2, y: height / 2});
        graph.nodes.filter(node => node.id !== roots[0].id).forEach((node, index, rest) => {
          const angle = -Math.PI / 2 + index / rest.length * Math.PI * 2;
          const radius = rest.length > 14 && index % 2 ? 0.68 : 1;
          pos.set(node.id, {x: width / 2 + Math.cos(angle) * 390 * radius, y: height / 2 + Math.sin(angle) * 225 * radius});
        });
      } else {
        const cols = roots.length <= 9 ? Math.min(3, roots.length) : 4;
        const rows = Math.ceil(roots.length / cols);
        roots.forEach((root, index) => {
          const cx = (index % cols + .5) * width / cols, cy = (Math.floor(index / cols) + .5) * height / rows;
          pos.set(root.id, {x: cx, y: cy});
          const children = graph.nodes.filter(node => node.parent_id === root.id);
          children.forEach((node, childIndex) => {
            const angle = -Math.PI / 2 + childIndex / children.length * Math.PI * 2;
            pos.set(node.id, {x: cx + Math.cos(angle) * Math.min(105, width / cols * .38),
              y: cy + Math.sin(angle) * Math.min(80, height / rows * .32)});
          });
        });
        graph.nodes.filter(node => !pos.has(node.id)).forEach((node, index) => pos.set(node.id, {x: 70 + index * 120, y: 550}));
      }
      return pos;
    }
    function draw(graph) {
      canvas.replaceChildren();
      const surface = el('div', 'topic-surface' + (graph.result ? ' result' : ''));
      if (graph.result) surface.style.setProperty('--result-min-height',
        160 + Math.ceil(graph.nodes.filter(node => node.kind === 'dataset').length / 5) * 80 + 'px');
      const edges = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
      edges.setAttribute('viewBox', '0 0 1000 600'); edges.setAttribute('preserveAspectRatio', 'none'); edges.setAttribute('aria-hidden', 'true');
      const pos = positions(graph);
      for (const edge of graph.edges) {
        const start = pos.get(edge.source), end = pos.get(edge.target);
        if (!start || !end) continue;
        const line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
        for (const [attr, value] of Object.entries({x1: start.x, y1: start.y, x2: end.x, y2: end.y})) line.setAttribute(attr, String(value));
        line.setAttribute('class', 'topic-edge ' + edge.relation); edges.append(line);
      }
      surface.append(edges);
      const parentIds = new Set(graph.edges.filter(edge => edge.relation === 'parent').map(edge => edge.source));
      graph.nodes.forEach((node, index) => {
        const position = pos.get(node.id);
        const item = button('', () => node.kind === 'dataset' ? inspectDataset(node) : selectTopic(node.id),
          'topic-node ' + node.kind + (node.classification?.level === 'unclassified' ? ' unclassified' : '') + (parentIds.has(node.id) || node.kind === 'category' ? ' parent' : '') + (node.id === selectedId ? ' selected' : ''));
        item.dataset.nodeId = node.id;
        item.style.setProperty('--x', position.x / 10 + '%'); item.style.setProperty('--y', position.y / 6 + '%');
        if (position.columns) item.style.setProperty('--node-space', 100 / (position.columns + 1) + '%');
        item.style.setProperty('--group', String(index % 6));
        item.setAttribute('aria-pressed', String(node.id === selectedId));
        ui(item,copy(node.kind === 'dataset' ? '{label} · 자료 정보 보기' : '{label} · 주제와 연결 자료 보기',()=>({label:node.kind==='dataset'?i18n?.field('dataset',node.dataset_id,'title',node.label)?.text || node.label:i18n?.topic(node.id,node.label)||node.label})), 'aria-label');
        item.append(el('span', 'topic-dot'), el('span', 'topic-label', node.kind==='dataset'?fieldCopy('dataset',node.dataset_id,'title',node.label):topicCopy(node.id,node.label)));
        if (parentIds.has(node.id) && Number.isSafeInteger(node.linked_datasets)) item.append(el('span', 'topic-node-count', copy('{count} 자료',{count:number.format(node.linked_datasets)})));
        surface.append(item);
      });
      canvas.append(surface);
      surface.style.setProperty('--graph-scale', String(scale));
      ui($('graph-visible'),graph.nodes.length?copy(graph.truncated?'{count}개 노드 · 일부 표시':'{count}개 노드',{count:number.format(graph.nodes.length)}):'');
    }
    async function showOverview() {
      const token = ++sequence;
      request?.abort(); selectedId = null; closeInspector(false);
      lastAction = showOverview;
      ui($('graph-title'), '주제로 연결된 데이터');
      $('graph-result-view').hidden = !lastResult;
      if (!taxonomyReady) setStatus('주제 지도를 불러오는 중…');
      try {
        await ensureTaxonomy();
        if (token !== sequence) return;
        const parent = $('topic-filter').value || null;
        const query = $('topic-search').value.trim().toLocaleLowerCase();
        const filtered = query ? topics.filter(topic => [topic.label, topic.parent_label, i18n?.topic(topic.id,topic.label), i18n?.topic(topic.parent_id,topic.parent_label || '')].some(label=>label?.toLocaleLowerCase().includes(query))) : topics;
        const graph = buildOverview(filtered, parent, 36, band);
        draw(graph);
        setStatus(!graph.nodes.length ? '일치하는 주제가 없어요. 분야나 분류 구간을 바꿔보세요.' :
          graph.truncated ? '분야를 선택하거나 주제 이름을 검색해 더 좁혀보세요.' : '주제를 선택하면 연결된 자료와 제공처를 볼 수 있어요.');
      } catch (error) {
        if (token !== sequence) return;
        setStatus(error.message === 'preparing' ? '주제 지도를 준비하고 있어요. 검색은 아래에서 계속할 수 있어요.' :
          '주제 지도를 불러오지 못했어요. 검색은 아래에서 계속할 수 있어요.', true);
      }
    }
    async function selectTopic(id) {
      const token = ++sequence;
      selectedId = id; lastAction = () => selectTopic(id);
      const known = taxonomy(topics).get(id);
      openInspector(known?topicCopy(known.id,known.label):'주제 정보');
      content.append(el('p', 'topic-note', copy('연결된 자료를 불러오는 중…')));
      setStatus('주제 연결을 확인하고 있어요…');
      canvas.querySelectorAll('.topic-node').forEach(node => {
        const selected = node.dataset.nodeId === id;
        node.setAttribute('aria-pressed', String(selected)); node.classList.toggle('selected', selected);
      });
      if (known?.kind === 'category') {
        request?.abort();
        ui($('graph-title'),topicCopy(known.id,known.label));
        $('topic-filter').value = id;
        draw(buildOverview(topics, id, 36, band));
        content.replaceChildren(el('p', 'topic-note', copy('세부 주제를 선택해 연결된 자료를 살펴보세요.')));
        const group = el('div', 'topic-related');
        for (const topic of topics.filter(topic => topic.parent_id === id && (!band || topic[band + '_links'] > 0))) group.append(button(topicCopy(topic.id,topic.label), () => selectTopic(topic.id)));
        content.append(group);
        setStatus(copy('{topic} 분야의 세부 주제예요.',()=>({topic:i18n?.topic(known.id,known.label)||known.label})));
        return;
      }
      try {
        const params = new URLSearchParams({limit: '20'});
        if (['high', 'low'].includes(band)) params.set('band', band);
        const value = await read('/api/ontology/topics/' + encodeURIComponent(id) + '?' + params);
        if (token !== sequence) return;
        if (!validTopic(value.topic) || value.topic.id !== id) throw new Error('invalid');
        ui($('graph-title'),topicCopy(value.topic.id,value.topic.label));
        ui($('topic-inspector-title'),topicCopy(value.topic.id,value.topic.label));
        const children = topics.filter(topic => topic.parent_id === id);
        draw(children.length ? buildOverview(topics, id, 28) : buildNeighborhood(value));
        content.replaceChildren();
        if (value.topic.definition) content.append(el('p', 'topic-description', topicCopy(value.topic.id,value.topic.definition,'definition')));
        if (value.topic.parent_label) content.append(el('p', 'topic-dataset-meta', copy('상위 주제 · {topic}',()=>({topic:i18n?.topic(value.topic.parent_id,value.topic.parent_label)||value.topic.parent_label}))));
        if (children.length) {
          const group = el('div', 'topic-related');
          for (const child of children) group.append(button(topicCopy(child.id,child.label), () => selectTopic(child.id)));
          content.append(el('h3', 'topic-section-label', copy('하위 주제')), group);
        }
        content.append(el('h3', 'topic-section-label', copy('연결된 자료')));
        const datasets = list(value.datasets);
        if (!datasets.length) content.append(el('p', 'topic-note', copy('이 주제에 연결된 자료를 아직 확인하지 못했어요.')));
        for (const dataset of datasets) content.append(datasetCard(dataset, true));
        if (value.has_more) content.append(el('p', 'topic-note', copy('현재 연결 자료 중 일부를 보여드리고 있어요.')));
        content.append(el('p', 'topic-note', copy('점선은 표시한 자료에서 함께 연결된 주제예요. 전체 자료의 관계 빈도나 인과관계를 뜻하지 않아요.')));
        setStatus('주제 분류와 자료 연결 · 노드를 눌러 상세 정보를 확인하세요.');
      } catch (error) {
        if (token !== sequence) return;
        content.replaceChildren(el('p', 'topic-note', copy('연결 정보를 불러오지 못했어요. 다시 시도할 수 있어요.')));
        setStatus(error.message === 'preparing' ? '주제 자료를 준비하고 있어요.' : '주제 연결을 불러오지 못했어요.', true);
      }
    }
    function renderResultGraph() {
      updateBand('');
      selectedId = null; closeInspector(false);
      ui($('graph-title'), '검색한 자료의 주제 연결');
      const classified = resultDatasets.filter(dataset => dataset.classification?.level !== 'unclassified');
      const topicMap = new Map();
      for (const dataset of classified) for (const topic of list(dataset.topics).filter(validTopic)) {
        if (topic.level !== 'unclassified' && topicMap.size < 16) topicMap.set(topic.id, {...topic, kind: 'topic'});
      }
      const nodes = [...topicMap.values(), ...resultDatasets.map(dataset => ({...dataset, dataset_id: dataset.id,
        id: 'dataset:' + dataset.id, label: dataset.title || dataset.id, kind: 'dataset'}))];
      const edges = classified.flatMap(dataset => list(dataset.topics).filter(topic => topicMap.has(topic.id) && topic.level !== 'unclassified')
        .map(topic => ({source: topic.id, target: 'dataset:' + dataset.id, relation: 'classified'})));
      // Search results have several centers: give every node its own grid position.
      draw({nodes, edges, total: nodes.length, truncated: false, result: true});
      setStatus(resultDatasets.length ? '검색 결과 자료의 저장된 주제 분류예요. 미분류 자료에는 연결선을 표시하지 않아요.' :
        '검색 결과에 연결된 주제 분류가 없어요. 사이트와 제공 근거는 대화에서 확인할 수 있어요.');
    }
    async function showResult(result) {
      const ids = resultDatasetIds(result);
      const token = ++sequence; lastAction = () => showResult(result);
      request?.abort();
      lastResult = null; resultDatasets = []; selectedId = null;
      $('graph-result-view').hidden = true;
      closeInspector(false); updateBand('');
      draw({nodes: [], edges: [], total: 0, truncated: false, result: true});
      if (!ids.length) {
        ui($('graph-title'), result?.state === 'clarify' ? '검색 조건 확인' : '연결할 주제 정보가 없어요');
        setStatus(result?.state === 'clarify' ? '이번 검색은 추가 조건을 확인하고 있어요. 대화에서 필요한 조건을 알려 주세요.' :
          '이번 검색 결과에는 연결할 자료 ID가 없어요. 사이트와 제공 근거는 대화에서 확인해 주세요.');
        return;
      }
      ui($('graph-title'), '검색 결과의 주제 연결을 확인하는 중');
      setStatus('검색 결과와 주제 그래프를 연결하고 있어요…');
      try {
        const value = await read('/api/ontology/dataset-topics?' + new URLSearchParams({ids: JSON.stringify(ids)}));
        if (token !== sequence) return;
        resultDatasets = list(value.datasets).filter(dataset => ids.includes(dataset.id));
        lastResult = result; $('graph-result-view').hidden = false;
        renderResultGraph();
      } catch {
        if (token === sequence) setStatus('주제 연결을 불러오지 못했어요. 검색 결과는 대화에서 확인할 수 있어요.', true);
      }
    }
    function ensureTaxonomy() {
      if (taxonomyReady) return Promise.resolve();
      if (taxonomyPromise) return taxonomyPromise;
      // Taxonomy is reusable page data, so changing the active graph must not
      // cancel this request or let its late response replace the chosen view.
      taxonomyPromise = (async () => {
        const value = await read('/api/ontology/topics', false);
        if (!Array.isArray(value.topics)) throw new Error('invalid');
        topics = value.topics.filter(validTopic);
        const all = taxonomy(topics);
        const parents = [...all.values()].filter(topic => !all.has(topic.parent_id));
        $('topic-filter').replaceChildren(el('option', '', copy('전체 분야')));
        $('topic-filter').firstElementChild.value = '';
        for (const topic of parents) { const option = el('option', '', topicCopy(topic.id,topic.label)); option.value = topic.id; $('topic-filter').append(option); }
        const counts = value.counts || {};
        for (const key of ['high', 'low', 'unclassified']) $('band-' + key + '-count').textContent = Number.isSafeInteger(counts[key]) ? number.format(counts[key]) : '';
        ui($('graph-total'),Number.isSafeInteger(counts.total)?copy('{count}개 자료의 주제 지도',{count:number.format(counts.total)}):'등록 자료의 주제 지도');
        ui($('graph-classification'),Number.isSafeInteger(counts.unclassified)?copy('전체 미분류 {count}개',{count:number.format(counts.unclassified)}):'');
        taxonomyReady = true;
      })().finally(() => { taxonomyPromise = null; });
      return taxonomyPromise;
    }
    $('close-topic-inspector').addEventListener('click', () => closeInspector());
    inspector.addEventListener('keydown', event => { if (event.key === 'Escape') closeInspector(); });
    $('topic-filter').addEventListener('change', showOverview);
    $('topic-search-form').addEventListener('submit', event => { event.preventDefault(); showOverview(); });
    async function showUnclassified() {
      const token = ++sequence; lastAction = showUnclassified; selectedId = null; closeInspector(false);
      ui($('graph-title'), '아직 주제가 연결되지 않은 자료');
      setStatus('미분류 자료를 불러오는 중…');
      try {
        const value = await read('/api/ontology/unclassified?limit=20');
        if (token !== sequence) return;
        const datasets = list(value.datasets).filter(dataset => dataset.classification?.level === 'unclassified');
        draw({nodes: datasets.map(dataset => ({...dataset, dataset_id: dataset.id, id: 'dataset:' + dataset.id, label: dataset.title || dataset.id, kind: 'dataset'})),
          edges: [], total: datasets.length, truncated: !!value.has_more, result: true});
        setStatus(datasets.length ? '미분류 자료 일부를 표시해요. 주제 연결을 추정하지 않고 자료 정보만 보여드려요.' : '표시할 미분류 자료가 없어요.');
      } catch { if (token === sequence) setStatus('미분류 자료를 불러오지 못했어요.', true); }
    }
    function updateBand(value) {
      band = value;
      document.querySelectorAll('[data-topic-band]').forEach(node => node.setAttribute('aria-pressed', String(node.dataset.topicBand === band)));
      $('topic-filter').disabled = band === 'unclassified'; $('topic-search').disabled = band === 'unclassified';
      $('topic-search-form').querySelector('button').disabled = band === 'unclassified';
    }
    function selectBand(value) {
      updateBand(value);
      if (band === 'unclassified') showUnclassified();
      else if (selectedId) selectTopic(selectedId);
      else showOverview();
    }
    document.querySelectorAll('[data-topic-band]').forEach(node => node.addEventListener('click', () => selectBand(node.dataset.topicBand)));
    $('graph-overview').addEventListener('click', () => { $('topic-filter').value = ''; $('topic-search').value = ''; selectedId = null; selectBand(''); });
    $('graph-result-view').addEventListener('click', () => { sequence++; request?.abort(); renderResultGraph(); });
    $('graph-retry').addEventListener('click', () => lastAction?.());
    for (const [id, change] of [['graph-zoom-in', .2], ['graph-zoom-out', -.2]]) $(id).addEventListener('click', () => {
      scale = Math.max(.6, Math.min(1.8, scale + change));
      canvas.firstElementChild?.style.setProperty('--graph-scale', String(scale));
      $('graph-zoom-out').disabled = scale <= .6; $('graph-zoom-in').disabled = scale >= 1.8;
    });
    showOverview();
    return {showResult, closeInspector: () => closeInspector(false), reset() {
      lastResult = null; resultDatasets = []; selectedId = null;
      $('topic-filter').value = ''; $('topic-search').value = ''; selectBand('');
    }};
  }
  window.TopicGraph = {buildOverview, buildNeighborhood, resultDatasetIds, safeURL, mount};
})();
