# HyundaiETLPRJ

Hive 2.x 클러스터의 데이터베이스와 테이블을 다른 Hive 2.x 클러스터로 옮기는 이관 도구입니다.
DB 전체나 테이블 단위로 옮기고, 옮긴 뒤 원본과 대상의 행 수와 해시를 비교해 검증합니다.

- 대상(target) 클러스터의 엣지 노드에서 실행합니다. 원본 클러스터에는 beeline(HiveServer2)과 HDFS 읽기로만 접근합니다.
- Python 3.6 이상, 표준 라이브러리만 사용합니다. 별도 설치가 필요 없습니다.
- 설계, 함정, 인터페이스, 운영 확인 항목은 [docs/작업노트.md](docs/작업노트.md)에 정리돼 있습니다. **코드를 고치기 전에 먼저 읽으세요.**

## 구성

| 경로 | 내용 |
|---|---|
| `migrate/hive_migrate.py` | 이관 CLI. 메타 조회, DDL 재작성, distcp 복사, 검증, 실패 시 되돌리기, 대상 삭제 |
| `migrate/history.py` | 결과 CSV 이력. 이어 하기(resume) 판단과 웹 화면 상태에 함께 씀 |
| `migrate/hive_inventory.py` | 원본 인벤토리(읽기 전용). 테이블 종류, 형식, 파티션, 크기를 CSV로 남김 |
| `migrate/web_ui.py`, `migrate/web/index.html` | 웹 화면 (원본 \| 이관 목록 \| 이관 결과) |
| `conf/local.ini` | 설정 예시 (로컬 Docker 테스트 환경용) |
| `docs/작업노트.md` | 운영 개발 명세 |
| `docker/` | 로컬 테스트용 두 클러스터 환경 (Hadoop 2.7.4 + Hive 2.3.2, ARM 네이티브) |
| `scripts/` | 로컬 테스트 도구 (beeline 접속, 샘플 데이터 생성) |
| `reports/` | 실행 로그와 결과 CSV (Git에 올리지 않음) |

## 사용법

```bash
# 1) 원본 인벤토리 (읽기 전용)
python3 migrate/hive_inventory.py -c conf/prod.ini --all

# 2) 이관 - sources.csv: "db,table[,tgt_db]" (table 이 * 이면 DB 전체)
python3 migrate/hive_migrate.py -c conf/prod.ini -s sources.csv --dry-run      # 계획만 확인
python3 migrate/hive_migrate.py -c conf/prod.ini -s sources.csv --parallel 4

# 3) 웹 화면 (기본 127.0.0.1:8080, 원격 서버라면 SSH 터널로 접속)
python3 migrate/web_ui.py -c conf/prod.ini
```

주요 옵션은 다음과 같습니다. 전체 목록은 `-h`와 작업노트 7장을 보세요.

| 옵션 | 기본값 | 설명 |
|---|---|---|
| `--parallel N` | 2 | 동시에 처리할 테이블 수 |
| `--control FILE` | 없음 | JSON `{"parallel": N}` 파일. 실행 중에 고치면 몇 초 안에 반영 |
| `--if-exists` | `resume` | 대상에 이미 있을 때: 같은 원본에서 OK로 옮긴 것은 건너뛰고 나머지는 다시 만듦 / 모두 다시 만듦(`replace`) / 건너뜀(`skip`) / 실패(`fail`) |
| `--validate` | `auto` | 원본 크기별로 고름: 100G 미만 `checksum`(`count(1)` + `sum(hash(*))`), 1T 미만 `count`(행 수), 그 이상 `files`(원본을 읽지 않고 파일 수·바이트 비교). `none`은 검증 안 함 |
| `--repair-source` | `true` | 원본 행 수가 0인 파티션 테이블은 원본에서 MSCK REPAIR 실행 |
| `--drop-target` | | 이관 대신 목록의 대상 테이블을 삭제 (`--drop-databases`로 빈 DB도 삭제) |

## 설정

`conf/local.ini`를 복사해서 운영용 `conf/prod.ini`를 만들고, 접속 명령만 바꾸면 됩니다.

```ini
[src]
beeline  = beeline -u jdbc:hive2://원본HS2호스트:10000/ -n 사용자
hdfs_uri = hdfs://원본NN:8020        ; Hadoop 버전이 다르면 webhdfs://원본NN:50070
hadoop   = hadoop

[tgt]
beeline = beeline -u jdbc:hive2://대상HS2호스트:10000/ -n 사용자
hadoop  = hadoop
```

## 로컬 테스트 환경

Docker(Colima 등)에서 원본과 대상 클러스터를 하나씩 띄웁니다.

```bash
docker build -t hive2-arm:2.3.2 docker/arm      # ARM(Apple Silicon) 네이티브 이미지
docker compose -f docker/docker-compose.yml up -d
./scripts/beeline.sh src                         # 원본 beeline / tgt: 대상
python3 migrate/web_ui.py -c conf/local.ini      # http://localhost:8080
```

| | 원본 | 대상 |
|---|---|---|
| HiveServer2 (JDBC) | `jdbc:hive2://localhost:10000/default` | `jdbc:hive2://localhost:10001/default` |
| NameNode 웹 | http://localhost:50070 | http://localhost:50071 |

- 인증은 없습니다. 사용자 이름은 아무 값이나 넣고 비밀번호는 비워 두면 됩니다.
- 샘플 데이터(자동차 업무 DB 20개, 객체 1,000개, 약 1GB)는 `scripts/gen_sample_data.py`로 원본 클러스터 안에서 만듭니다.
- CPU를 많이 씁니다. VM의 CPU 수가 동시 처리 수의 상한이 됩니다. 참고로 CPU 4개에서는 동시 3~4개면 포화됩니다.

## 주의

- **ACID(transactional) 테이블은 파일 복사로 옮기면 안 됩니다.** 이 도구는 원본에 임시 테이블을 만들어 INSERT…SELECT 방식으로 옮깁니다(작업노트 6장).
- 실행 중인 작업이 있을 때 웹 서버를 재시작해도 작업은 계속됩니다. 그래도 가능하면 작업이 없을 때 재시작하세요.
