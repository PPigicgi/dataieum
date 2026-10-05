/* Each topic's share uses the catalogue total, including multi-topic records. */
(() => {
  'use strict';
  if (typeof render !== 'function' || typeof state === 'undefined') return;
  function updateShares() {
    const total = Number(state.summary?.datasets);
    const names = new Map(state.concepts.map(concept => [concept.id, concept.name]));
    for (const button of document.querySelectorAll('#side-concepts .topic-nav')) {
      const count = Number(state.summary?.concept_counts?.[button.dataset.topic] || 0);
      const valid = Number.isFinite(total) && total > 0 && Number.isFinite(count) && count >= 0;
      const percent = valid ? Math.min(100, count / total * 100) : 0;
      button.style.setProperty('--topic-share', `${percent}%`);
      const description = valid
        ? `${names.get(button.dataset.topic) || ''} · ${count.toLocaleString('ko-KR')}건 · 전체 자료의 ${percent.toFixed(2)}%`
        : `${names.get(button.dataset.topic) || ''} · 자료 비율 집계 전`;
      button.title = description;
      button.setAttribute('aria-label', description);
    }
  }
  const originalRender = render;
  render = function(...args) {
    const result = originalRender.apply(this, args);
    updateShares();
    return result;
  };
  updateShares();
})();
