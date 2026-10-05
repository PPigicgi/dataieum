# 공개 사본 변경 내역

운영 컨테이너에서 추출한 원본은 별도로 보관했습니다. 공개 사본에서 아래 항목만 정리했습니다.

- `operations/backup_mac.py`, `operations/release.py`: 실제 SSH 접속 대상과 키 경로를 필수 환경변수 `DATAIEUM_SSH_TARGET`, `DATAIEUM_SSH_KEY`로 분리. 원격 작업 전에 설정해야 하며, 누락 시 오류를 반환합니다.
- `discovery_harness/ontology_asset_models.json`: 검토 근거에 남아 있는 개인 PC 작업 경로를 프로젝트 상대경로로 변경. 원래 파일명·날짜·검토 내용은 유지했습니다. 해당 검토 입력 파일 전체가 이 사본에 포함되는 것은 아닙니다.
- `THIRD_PARTY_NOTICES.md`: 빌드 결과물에 포함된 React·React DOM·Scheduler의 버전과 MIT 고지 추가.
- `README.md`, `.gitignore`, 이 문서와 `SOURCE_MANIFEST.json`: 공개 범위·제약·파일 검증을 위한 안내 추가.

`SOURCE_MANIFEST.json`은 추출 원본과 공개 파일의 SHA-256을 각각 기록합니다. 기존 Git 이력, 개발 날짜 및 버전 식별자를 다시 작성하지 않았습니다. 운영 서비스·원래 프로젝트에는 이 공개 사본의 정리 내용을 적용하지 않았습니다.
