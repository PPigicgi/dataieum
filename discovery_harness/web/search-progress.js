/* Waiting copy, not server stage telemetry or a completion percentage. */
window.SearchProgress = {
  start(container, query, {signal, announcement}) {
    const compact = Array.from(query.replace(/\s+/gu, ' ').trim());
    const label = compact.slice(0, 48).join('') + (compact.length > 48 ? '…' : '');
    const pending = document.createElement('p');
    pending.className = 'pending search-progress';
    // The live region sits outside the conversation's aria-busy boundary.
    pending.setAttribute('aria-hidden', 'true');
    container.append(pending);
    const timers = [];
    let stopped = false;
    const bind = (node, source, params = {}) => {
      if (window.DataieumI18n) window.DataieumI18n.bind(node, source, params);
      else node.textContent = source.replace(/\{(\w+)\}/g, (match, key) => params[key] ?? match);
    };
    const show = (text, params = {}) => {
      if (stopped || signal.aborted) return;
      const line = document.createElement('span');
      line.className = 'search-progress-text';
      bind(line, text, params);
      pending.replaceChildren(line);
      bind(announcement, text, params);
    };
    const stop = () => {
      if (stopped) return;
      stopped = true;
      timers.forEach(clearTimeout);
      signal.removeEventListener('abort', stop);
      bind(announcement, '');
    };
    show('“{query}”에 맞는 사이트를 찾고 있어요…', {query: label});
    for (const [delay, text] of [
      [3500, '요청하신 조건으로 검색하고 있어요…'],
      [7500, '조건을 확인할 수 있는 사이트부터 보여드릴게요…'],
      [12000, '아직 검색 응답을 기다리고 있어요. 잠시만 기다려 주세요…'],
      [16000, '응답이 늦어지고 있어요. 최대 20초까지 기다려요…']
    ]) timers.push(setTimeout(() => show(text), delay));
    signal.addEventListener('abort', stop, {once: true});
    return stop;
  }
};
