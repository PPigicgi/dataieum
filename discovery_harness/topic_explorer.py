"""Topic-first projection of the existing, read-only primary classification.

Counts and pages describe primary membership, while dataset metadata retains all
of its original associations. The imported module remains the owner of filters,
record decoding and the legacy band view.
"""
from functools import lru_cache
import json


class TopicExplorer:
    view = 'topics_v1'

    def __init__(self, module, connection):
        self.module, self.connection = module, connection
        self._graph = lru_cache(maxsize=256)(self._graph)

    def clear(self):
        self._graph.cache_clear()

    def graph(self, query):
        if query.get('band'):
            return self.module.graph_overview(query)
        return self._graph(tuple(sorted((key, value) for key, value in query.items() if value)))

    def _graph(self, key):
        query = dict(key)
        concept = query.get('concept', '')
        with self.connection() as db:
            if not concept or '-S' not in concept:
                nodes, total = self._topics(db, query)
            elif not query.get('source'):
                nodes, total = self._sources(db, query)
            else:
                clause, args = self.module.where(query)
                total = self.module.total(db, query, clause, args)
                nodes = self._records(db, query, total)
        edges = [
            {'a': node['id'], 'b': 'topic:' + mapping['concept_id'],
             'opacity': mapping['opacity'], 'band': node['record']['confidence_band']}
            for node in nodes if node['kind'] == 'dataset'
            for mapping in node['record']['mappings']
        ]
        return {'nodes': nodes, 'edges': edges, 'total': total, 'represented': total,
                'query': query, 'view': self.view}

    def _topics(self, db, query):
        main = query.get('concept')
        filtered = any(query.get(key) for key in ('q', 'source', 'year'))
        unclassified = 0
        if main:
            labels = db.execute('SELECT id,name AS title FROM topics WHERE main_id=? ORDER BY id',
                                (main,)).fetchall()
            if filtered:
                clause, args = self.module.where({k: v for k, v in query.items() if k != 'concept'})
                clause += (' AND ' if clause else ' WHERE ') + 't.main_id=?'
                counts = dict(db.execute(
                    'SELECT p.topic,count(*) FROM topics t JOIN primary_links p ON p.topic=t.id '
                    'JOIN records r ON r.id=p.dataset_id' + clause + ' GROUP BY p.topic', args + [main]))
            else:
                counts = dict(db.execute("SELECT topic,sum(n) FROM primary_counts "
                                         "WHERE main_id=? AND topic<>'' GROUP BY topic", (main,)))
            total = sum(counts.values())
        else:
            labels = db.execute('SELECT DISTINCT main_id AS id,main_name AS title '
                                'FROM topics ORDER BY main_id').fetchall()
            if filtered:
                clause, args = self.module.where(query)
                # One pass over matching records provides both the total and all
                # main counts, including records with no primary association.
                rows = db.execute('SELECT t.main_id,r.band,count(*) AS n FROM records r '
                                  'LEFT JOIN primary_links p ON p.dataset_id=r.id '
                                  'LEFT JOIN topics t ON t.id=p.topic' + clause +
                                  ' GROUP BY t.main_id,r.band', args)
                counts, total = {}, 0
                for row in rows:
                    total += row['n']
                    if row['main_id']:
                        counts[row['main_id']] = counts.get(row['main_id'], 0) + row['n']
                    elif row['band'] == 'unclassified':
                        unclassified += row['n']
            else:
                counts = dict(db.execute("SELECT main_id,sum(n) FROM primary_counts "
                                         "WHERE main_id<>'' AND topic='' GROUP BY main_id"))
                bands = dict(db.execute("SELECT band,sum(n) FROM primary_counts "
                                        "WHERE main_id='' AND topic='' GROUP BY band"))
                total, unclassified = sum(bands.values()), bands.get('unclassified', 0)
        nodes = [{'id': 'topic:' + row['id'], 'kind': 'topic',
                  'level': 'subtopic' if main else 'main', 'title': row['title'],
                  'count': counts.get(row['id'], 0), 'query': {**query, 'concept': row['id']}}
                 for row in labels]
        if not main:
            nodes.append({'id': 'unclassified', 'kind': 'band', 'level': 'unclassified',
                          'title': '주제 미지정', 'count': unclassified,
                          'query': {**query, 'band': 'unclassified'}})
        return nodes, total

    def _sources(self, db, query):
        clause, args = self.module.where({k: v for k, v in query.items() if k != 'concept'})
        clause += (' AND ' if clause else ' WHERE ') + 'p.topic=?'
        found = db.execute('SELECT r.source,count(*) AS n FROM primary_links p '
                           'CROSS JOIN records r ON r.id=p.dataset_id' + clause +
                           ' GROUP BY r.source ORDER BY r.source', args + [query['concept']]).fetchall()
        names = {row['id']: json.loads(row['info'])['name']
                 for row in db.execute('SELECT id,info FROM cat.sources')}
        nodes = [{'id': 'source:' + row['source'], 'kind': 'source', 'level': 'source',
                  'title': names.get(row['source'], row['source']), 'count': row['n'],
                  'query': {**query, 'source': row['source']}} for row in found]
        return nodes, sum(row['n'] for row in found)

    def _records(self, db, query, total):
        start, size = 0, total
        branch = query.get('branch', '')
        if len(branch) > 64:
            raise ValueError('잘못된 자료 묶음')
        for part in branch.split('.') if branch else []:
            if not part.isdigit() or int(part) >= 32 or size <= 48:
                raise ValueError('잘못된 자료 묶음')
            width = (size + 31) // 32
            offset = int(part) * width
            if offset >= size:
                raise ValueError('잘못된 자료 묶음')
            start += offset
            size = min(width, size - offset)
        # NativeCatalog substitutes its one-pass, at-most-64-endpoint reader for
        # large pages. These offsets are over the union, never over each band.
        ids = self.module.page_ids(db, query, start, size)
        if size > 48:
            width = (size + 31) // 32
            endpoints = {ids[offset] for offset in range(0, size, width)}
            endpoints.update(ids[min(offset + width, size) - 1] for offset in range(0, size, width))
            placeholders = ','.join('?' for _ in endpoints)
            titles = dict(db.execute('SELECT id,title FROM records WHERE id IN (' + placeholders + ')',
                                     list(endpoints)))
            nodes = []
            for index, offset in enumerate(range(0, size, width)):
                path = (branch + '.' if branch else '') + str(index)
                count = min(width, size - offset)
                first, last = (titles.get(ids[position], '')[:22]
                               for position in (offset, offset + count - 1))
                nodes.append({'id': 'range:' + path, 'kind': 'range', 'level': 'range',
                              'title': first + ' … ' + last, 'count': count,
                              'query': {**query, 'branch': path}})
            return nodes
        nodes = []
        for record in self.module.records(db, ids):
            edge = next((mapping for mapping in record['mappings']
                         if mapping['concept_id'] == query.get('concept')), None)
            nodes.append({'id': record['id'], 'kind': 'dataset', 'level': 'dataset',
                          'title': record['title'], 'count': 1, 'record': record,
                          'confidenceBand': record['confidence_band'],
                          'scoreAlpha': edge['opacity'] if edge else .5})
        return nodes

    def initial_overview(self):
        result = dict(self.module.base_catalog())
        result['view'] = self.view
        result['graph'] = self.graph({})
        # Warming main children only touches taxonomy and precomputed counts.
        # Unclassified/source expansion remains on demand.
        result['graph_children'] = [self.graph(node['query']) for node in result['graph']['nodes']
                                    if node['level'] == 'main']
        return result
