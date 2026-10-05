"""Bounded, provenance-preserving text inputs; no model or vector DB required."""
import hashlib
import json
import re

VERSION = 'official-metadata-v1'
FIELDS = ('title', 'description', 'classification_paths', 'survey_name', 'tags')
LABELS = dict(zip(FIELDS, ('제목', '설명', '공식 분류 경로', '조사명', '공식 태그')))


def prepare_metadata(record, *, max_chars=12000):
    if not isinstance(record, dict) or not 100 <= max_chars <= 24000:
        raise ValueError('invalid metadata input')
    fields, provenance, warnings = {}, {}, []
    def clean(value):
        return ' '.join(value.split()) if isinstance(value, str) else ''
    def assign(name, values, source, method='original_field'):
        if not isinstance(values, list): values = [values]
        values = list(dict.fromkeys(clean(v) for v in values if clean(v)))
        if values:
            fields[name] = values
            provenance[name] = {'field': source, 'method': method}
    assign('title', record.get('title'), 'title')
    assign('description', record.get('description'), 'description')
    # These are source catalog fields, not the platform's `mappings`.
    paths = record.get('native_catalog_path')
    if not paths:
        paths = record.get('native_catalog_paths', [])
    assign('classification_paths', paths, 'native_catalog_path' if record.get('native_catalog_path') else 'native_catalog_paths')
    assign('survey_name', record.get('survey_name'), 'survey_name')
    # KOSIS's observed official export explicitly quotes the survey in publisher.
    if not fields.get('survey_name') and record.get('source_id') == 'kosis' and record.get('collection_method') == 'official_catalog_export':
        surveys = re.findall(r'「([^「」]{1,500})」', str(record.get('publisher', '')))
        if len(surveys) == 1:
            assign('survey_name', surveys[0], 'publisher', 'quoted_kosis_survey')
    subjects = record.get('subjects', [])
    official = str(record.get('collection_method', '')).startswith('official_')
    if isinstance(subjects, list) and official:
        assign('tags', [s.get('label') for s in subjects if isinstance(s, dict) and s.get('kind') == 'tag'], 'subjects[kind=tag].label')
    # Theme labels are not automatically promoted to tags or a hierarchy path.
    missing = [name for name in FIELDS if not fields.get(name)]
    present = [name for name in FIELDS if fields.get(name)]
    text, truncated, used = [], [], 0
    for name in present:
        value = ' | '.join(fields[name])
        line = LABELS[name] + ': ' + value
        room = max(0, max_chars - used - int(bool(text)))
        if len(line) > room:
            truncated.append(name)
        if room > len(LABELS[name]) + 2:
            text.append(line[:room]); used += len(text[-1]) + int(len(text) > 1)
    if 'title' in missing:
        warnings.append('missing_title')
    if 'description' in missing:
        warnings.append('missing_description')
    if truncated:
        warnings.append('input_truncated')
    # Keep the full-input hash independent of preview clipping. Never claim a
    # tokenizer count without the teammate's actual embedding tokenizer.
    digest = hashlib.sha256(json.dumps({'version': VERSION, 'fields': fields}, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
    visible = '\n'.join(text) if 'title' not in missing else ''
    previews = {name: ' | '.join(fields[name])[:2400] for name in present}
    return {'version': VERSION, 'status': 'missing_title' if 'title' in missing else 'review_required' if truncated else 'prepared',
            'fields': previews, 'provenance': provenance, 'present_fields': present, 'missing_fields': missing,
            'input_text': visible, 'input_chars': len(visible), 'token_count': None,
            'truncated_fields': truncated, 'preview_truncated_fields': [name for name in present if len(' | '.join(fields[name])) > 2400],
            'warnings': warnings, 'input_hash': digest, 'embedding_computed': False,
            'metadata_url': record.get('metadata_url') or '', 'dataset_url': record.get('url') or ''}
