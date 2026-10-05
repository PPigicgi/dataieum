"""Fixed, server-owned cooperation mail rendering. No model or network access."""
from __future__ import annotations

import hashlib
import json
import re
import string
from datetime import date
from email.utils import parseaddr
from urllib.parse import urlsplit


class CooperationError(Exception):
    def __init__(self, status, code, message=None):
        self.status, self.code = status, code
        self.message = message or code
        super().__init__(self.message)


def fail(status, code, message):
    raise CooperationError(status, code, message)


def text(value, limit, *, required=True, line=False):
    if not isinstance(value, str) or len(value) > limit or '\x00' in value:
        fail(400, 'invalid_input', '입력값의 형식과 길이를 확인해 주세요.')
    if line and any(ord(c) < 32 or ord(c) == 127 for c in value):
        fail(400, 'invalid_header', '한 줄로 입력해 주세요.')
    value = value.strip()
    if required and not value:
        fail(400, 'required_input', '필수 항목을 입력해 주세요.')
    return value


def email(value):
    value = text(value, 254, line=True).lower()
    if not re.fullmatch(r"[a-z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?\.[a-z]{2,63}", value):
        fail(400, 'invalid_email', '이메일 주소를 확인해 주세요.')
    return value


def sender(value):
    value = text(value, 320, line=True)
    _, address = parseaddr(value)
    email(address)
    return value


def safe_url(value):
    if not isinstance(value, str) or len(value) > 4096 or any(ord(c) < 32 for c in value):
        return ''
    try:
        parsed = urlsplit(value)
        return value if parsed.scheme in {'https', 'http'} and parsed.hostname and not parsed.username and not parsed.password else ''
    except ValueError:
        return ''


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def contact_reference(row):
    """A displayed contact is informational; it never grants send permission."""
    if row.get('email_role') == 'pending_dataset_contact':
        return None
    for field, kind in (('email', 'data_inquiry'), ('portal_email', 'portal_support'),
                        ('manual_inquiry_email', 'manual_inquiry')):
        if row.get(field):
            try:
                return {'email': email(row[field]), 'kind': kind}
            except CooperationError:
                continue
    return None


def contact_options(dataset, contacts, request_type=None):
    if not isinstance(contacts, dict) or not isinstance(contacts.get('contacts'), list):
        fail(503, 'contacts_invalid', '기관 연락처 설정을 확인 중입니다.')
    candidates = [row for row in contacts['contacts'] if row.get('source_id') == dataset['source_id']
                  and (not row.get('dataset_id') or row['dataset_id'] == dataset['id'])]
    specific = [row for row in candidates if row.get('dataset_id')]
    candidates = specific or candidates
    fallback = candidates[0] if candidates else {}
    if request_type is not None:
        candidates = [row for row in candidates if request_type in row.get('request_types', [])]
    if len(candidates) > 1:
        fail(503, 'contacts_ambiguous', '기관 연락처 설정을 확인 중입니다.')
    row = candidates[0] if candidates else {key: fallback.get(key) for key in ('contact_url', 'institution')}
    available = row.get('enabled') is True and bool(row.get('email'))
    if available:
        kinds = row.get('request_types')
        if not isinstance(kinds, list) or not kinds or set(kinds) - {'usage_inquiry', 'data_provision'}:
            fail(503, 'contact_scope_invalid', '기관 문의창구의 용도를 확인 중입니다.')
        email(row['email'])
        if not safe_url(row.get('evidence_url')) or not row.get('institution'):
            fail(503, 'contact_unverified', '기관 연락처 설정을 확인 중입니다.')
        try: date.fromisoformat(row['checked_on'])
        except (ValueError, KeyError, TypeError): fail(503, 'contact_unverified', '기관 연락처 설정을 확인 중입니다.')
    return {'available': available, 'institution': row.get('institution') or dataset.get('source_name', ''),
            'email': email(row['email']) if available else None,
            'contact_reference': contact_reference(row),
            'contact_url': safe_url(row.get('contact_url')) or safe_url(dataset.get('source_url')),
            'contact': row, 'dataset': dataset}


INPUT_FIELDS = {'dataset_id', 'request_type', 'purpose', 'usage_description', 'commercial_use', 'requested_scope', 'preferred_format'}
COMMERCIAL = {'commercial': '상업적 이용', 'noncommercial': '비상업적 이용', 'undecided': '미정'}


def validate_input(data):
    if not isinstance(data, dict) or set(data) - INPUT_FIELDS:
        fail(400, 'invalid_fields', '요청 항목을 확인해 주세요.')
    kind = data.get('request_type')
    if kind not in {'usage_inquiry', 'data_provision'} or data.get('commercial_use') not in COMMERCIAL:
        fail(400, 'invalid_request_type', '메일 유형과 상업적 이용 여부를 선택해 주세요.')
    return {'dataset_id': text(data.get('dataset_id'), 2048, line=True), 'request_type': kind,
            'purpose': text(data.get('purpose'), 2000), 'usage_description': text(data.get('usage_description'), 2000),
            'commercial_use': data['commercial_use'],
            'requested_scope': text(data.get('requested_scope', ''), 2000, required=kind == 'data_provision'),
            'preferred_format': text(data.get('preferred_format', ''), 200, required=False, line=True)}


def render(template, fields):
    if not isinstance(template, str) or len(template) > 12000:
        fail(503, 'template_invalid', '메일 양식을 확인 중입니다.')
    try:
        parts = []
        for literal, key, spec, conversion in string.Formatter().parse(template):
            parts.append(literal)
            if key is not None:
                if key not in fields or spec or conversion or not re.fullmatch('[a-z_]+', key):
                    raise ValueError('unsupported placeholder')
                parts.append(fields[key])
        return ''.join(parts)
    except (ValueError, KeyError):
        fail(503, 'template_invalid', '메일 양식을 확인 중입니다.')


def prepare_request(data, member, dataset, contacts, template, from_address):
    data = validate_input(data)
    if data['dataset_id'] != dataset['id']:
        fail(400, 'dataset_mismatch', '선택한 자료를 다시 확인해 주세요.')
    options = contact_options(dataset, contacts, data['request_type'])
    if not options['available']:
        fail(409, 'contact_unavailable', '등록된 수신처가 없습니다. 공식 문의 경로를 이용해 주세요.')
    if (not isinstance(template, dict) or type(template.get('version')) is not int or template['version'] < 1
            or template.get('status') not in {'draft', 'approved'}):
        fail(503, 'template_invalid', '메일 양식을 확인 중입니다.')
    selected = template.get('types', {}).get(data['request_type'], {})
    fields = {'institution': text(options['institution'], 200, line=True),
              'dataset_title': text(dataset.get('title'), 500, line=True),
              'dataset_url': safe_url(dataset.get('url')),
              'requester_name': text(member['name'], 100, line=True),
              'organization': text(member['organization'], 200, line=True),
              'requester_email': email(member['email']), 'purpose': data['purpose'],
              'usage_description': data['usage_description'], 'commercial_use_text': COMMERCIAL[data['commercial_use']],
              'requested_scope': data['requested_scope'], 'preferred_format': data['preferred_format'] or '협의 가능'}
    if not fields['dataset_url']:
        fail(409, 'dataset_url_missing', '자료의 공식 주소를 확인 중입니다.')
    payload = {'from': sender(from_address), 'to': [options['email']], 'reply_to': fields['requester_email'],
               'subject': text(render(selected.get('subject'), fields), 998, line=True),
               'text': render(selected.get('body'), fields)}
    value = {'input': data, 'request_type': data['request_type'], 'source_id': dataset['source_id'],
             'dataset': dataset, 'institution': options['institution'], 'contact_url': options['contact_url'],
             'label': text(selected.get('label'), 100, line=True), 'template_version': template['version'],
             'sendable': template['status'] == 'approved', 'payload': payload,
             'member_fingerprint': fingerprint({key: member[key] for key in ('email', 'name', 'organization')})}
    value['fingerprint'] = fingerprint({'value': value, 'contact': options['contact'], 'template': template})
    return value
