# CLAUDE.md

Hive 2.x → Hive 2.x 클러스터 간 이관 도구. 개요와 사용법은 [README.md](README.md)에 있습니다.

## 먼저 읽을 것
- 코드를 고치기 전에 [docs/작업노트.md](docs/작업노트.md)를 읽습니다. 설계 근거, Hive 2.x 함정, 인터페이스, 운영 확인 항목이 모두 여기 있습니다. 같은 내용을 이 파일에 다시 쓰지 않습니다.
- 동작을 바꾸면 작업노트의 해당 절도 함께 고칩니다. 변경 이력이 아니라 현재 동작을 적습니다.
- 작업노트는 운영 환경용 개발 명세입니다. 로컬 Docker, 샘플 데이터, 테스트 이야기는 넣지 않습니다. 어떤 AI 모델이나 사람이 읽어도 되게 [필수]/[권장]/[확인] 표시로 씁니다.

## 코드 제약
- Python 3.6 이상, 표준 라이브러리만 씁니다. f-string 외에 3.7 이상 전용 기능(dataclasses, `subprocess.run(capture_output=)` 등)은 쓰지 않습니다.
- 원본 클러스터는 beeline(HiveServer2)으로 SHOW, DESCRIBE, SELECT를 하고, HDFS는 읽기만 합니다. 원본 메타스토어 DB에는 직접 접근하지 않습니다. 원본에 쓰는 것은 작업노트 6장(MSCK, ACID 임시 테이블)에 정한 것뿐입니다.
- beeline 출력은 `\n`으로만 나누고, 결과 구역 마커(`@@NAME@@`)가 없으면 오류로 처리합니다(작업노트 3장).
- 로그 형식 `HH:MM:SS [db.table] 메시지`와 `=> STATUS` 줄은 웹 화면이 파싱합니다. 형식을 바꾸면 화면도 함께 고칩니다.
- 화면 문구, 로그 안내, 작업노트는 한국어로 씁니다.
- 커밋 메시지는 제목과 본문 모두 한국어로 씁니다. 코드 식별자, 파일 이름, 옵션은 원래 표기대로 둡니다(예: `if_exists=resume`). 이미 push한 영어 커밋은 고치지 않습니다.

## 안전 규칙
- `migrate/web_ui.py`를 재시작하기 전에 `curl -s localhost:8080/api/job`의 `running`이 false인지 확인합니다. 실행 중이면 끝날 때까지 기다립니다.
- `hive_migrate.py`는 실행할 때마다 새 프로세스로 뜹니다. 그래서 `hive_migrate.py`나 `web/index.html`만 고쳤다면 웹 서버를 재시작할 필요가 없습니다. 화면은 새로고침만 하면 됩니다. 단, `web_ui.py`와 웹 서버가 불러오는 `history.py`를 고쳤으면 재시작합니다.
- 대상 테이블이나 DB 삭제, 원본에 쓰기, 클러스터나 Docker VM 재시작처럼 되돌리기 어려운 작업은 먼저 사용자에게 묻습니다.

## 검증
- Python: `python3 -m py_compile migrate/*.py`
- 화면 JS: 이 PC에는 node가 없습니다. `<script>` 내용을 파일로 뽑아 `osascript -l JavaScript`에서 `new Function(src)`로 문법을 검사합니다.
- 실행 순서 같은 로직은 `Migrator`를 상속한 가짜 클래스로 단위 테스트합니다. `migrate_table`과 `migrate_view`를 바꿔 끼우고, 클러스터 없이 돌립니다.
- 실제 동작은 로컬 두 클러스터로 확인합니다.

## 로컬 환경
- `docker compose -f docker/docker-compose.yml up -d`로 원본(`src-*`)과 대상(`tgt-*`)을 띄웁니다. ARM 네이티브 이미지 `hive2-arm:2.3.2`는 `docker/arm`에서 빌드합니다.
- `scripts/beeline.sh src|tgt`로 beeline에 접속합니다. 설정 파일은 `conf/local.ini`, 웹 화면은 `python3 migrate/web_ui.py -c conf/local.ini`(포트 8080)입니다.
- CPU를 많이 씁니다(Colima VM CPU 6개). 실행 결과는 `reports/`에 남고, Git에는 올리지 않습니다.
