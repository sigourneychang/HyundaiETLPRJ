#!/usr/bin/env python3
"""Hive 이관 웹 화면 (Python 3.6+ 표준 라이브러리만 사용)

    python3 migrate/web_ui.py -c conf/local.ini [--port 8080] [--host 127.0.0.1]

- 왼쪽(원본) 트리: 가장 최근 인벤토리 CSV(reports/inventory_*.csv)를 읽음. 원본 클러스터를 직접 조회하지 않음.
- 오른쪽(대상): 화면에서 담은 이관 대기열 + 지난 이관 결과(reports/migration_*.csv)의 테이블별 최신 상태.
- 이관/인벤토리는 hive_migrate.py / hive_inventory.py 를 백그라운드 프로세스로 실행 (한 번에 하나).
- 기본은 127.0.0.1 에만 열림. 원격에서는 ssh -L 8080:localhost:8080 엣지노드 로 접속.
"""
import argparse
import csv
import datetime
import glob
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import hive_migrate as hm   # 설정 읽기, beeline 실행 (대상 DB 목록 조회)
from history import History   # 이관 이력 (결과 CSV): CLI의 resume 과 같은 규칙

HERE = os.path.dirname(os.path.abspath(__file__))
BOOT_ID = datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')   # 서버가 재시작되면 바뀜 -> 화면이 새로고침
NAME_RE = re.compile(r'^[A-Za-z0-9_]+$')
LOG_RE = re.compile(r'^\d\d:\d\d:\d\d \[([^\]]+)\] (.*)$')
MAX_LINES = 20000   # 화면용으로 메모리에 들고 있는 로그 줄 수 (전체 로그는 reports/*.log)


class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):   # 3.6 호환 (3.7+는 http.server에 있음)
    daemon_threads = True


# --------------------------------------------------------------------------- 인벤토리 (왼쪽 트리)
class Inventory:
    def __init__(self, report_dir):
        self.report_dir = report_dir
        self.lock = threading.Lock()
        self.path, self.mtime, self.dbs = None, None, {}

    def latest_file(self):
        files = [f for f in glob.glob(os.path.join(self.report_dir, 'inventory_*.csv'))
                 if not f.endswith('_sources.csv')]
        return max(files, key=os.path.getmtime) if files else None

    def load(self):
        path = self.latest_file()
        with self.lock:
            if path is None:
                self.path, self.mtime, self.dbs = None, None, {}
                return
            mtime = os.path.getmtime(path)
            if path == self.path and mtime == self.mtime:
                return
            dbs = {}
            with open(path, newline='', encoding='utf-8') as f:
                for r in csv.DictReader(f):
                    dbs.setdefault(r['db'], []).append({
                        'table': r['table'], 'kind': r['kind'], 'format': r['format'],
                        'partitioned': r['partitioned'] == 'True',
                        'partition_count': _int(r.get('partition_count')),
                        'size_bytes': _int(r.get('size_bytes')),
                        'file_count': _int(r.get('file_count')),
                        'location': r.get('location', ''), 'error': r.get('error', '')})
            for rows in dbs.values():
                rows.sort(key=lambda t: t['table'])
            self.path, self.mtime, self.dbs = path, mtime, dbs

    def info(self):
        self.load()
        if not self.path:
            return {'file': None}
        return {'file': os.path.basename(self.path),
                'time': datetime.datetime.fromtimestamp(self.mtime).strftime('%Y-%m-%d %H:%M'),
                'dbs': len(self.dbs), 'tables': sum(len(v) for v in self.dbs.values())}

    def db_list(self):
        self.load()
        out = []
        for db in sorted(self.dbs):
            rows = self.dbs[db]
            kinds = {}
            for t in rows:
                kinds[t['kind']] = kinds.get(t['kind'], 0) + 1
            out.append({'db': db, 'tables': len(rows), 'kinds': kinds,
                        'size_bytes': sum(t['size_bytes'] or 0 for t in rows)})
        return out

    def tables(self, db):
        self.load()
        return self.dbs.get(db, [])

    def search(self, q, limit=1000):
        self.load()
        q = q.lower()
        out = []
        for db in sorted(self.dbs):
            for t in self.dbs[db]:
                if q in ('%s.%s' % (db, t['table'])).lower():
                    out.append(dict(t, db=db))
                    if len(out) >= limit:
                        return out, True
        return out, False


def _int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------- 백그라운드 작업
def keep_awake(pid):
    """macOS: 작업 프로세스가 끝날 때까지 자동 잠자기를 막음 (caffeinate -i -w PID, 작업이 끝나면 저절로 풀림).
    잠자기에 들어가면 Docker VM·클러스터가 통째로 멈춰 작업이 그만큼 늦어짐. macOS가 아니면 아무것도 안 함."""
    exe = shutil.which('caffeinate') if sys.platform == 'darwin' else None
    if not exe:
        return
    try:
        subprocess.Popen([exe, '-i', '-w', str(pid)], stdin=subprocess.DEVNULL,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    except OSError:
        pass


class JobRunner:
    """백그라운드 작업 (한 번에 하나).
    작업 출력은 파이프가 아니라 파일(reports/ui_job_*.out)로 보냄: 웹 서버가 재시작·종료돼도 작업은
    끝까지 돌고, 다시 뜬 서버는 reports/.ui_job.json 을 보고 실행 중인 작업을 이어받아 진행을 보여줌."""

    def __init__(self, config, report_dir):
        self.config = config
        self.report_dir = report_dir
        self.pidfile = os.path.join(report_dir, '.ui_job.json')
        # hive_migrate.py re-reads this while running: live change of the number of tables at a time
        self.control = os.path.join(report_dir, '.ui_control.json')
        self.lock = threading.Lock()
        self.proc = None          # 이 서버가 띄운 작업 (이어받은 작업이면 None)
        self.pid = None
        self.out_path = None
        self.kind = None
        self.started = None
        self.finished = None
        self.rc = None
        self.lines = []
        self.dropped = 0          # MAX_LINES 넘어서 버린 앞부분 줄 수
        self.items = {}           # 'db.table' -> 상태 (이관 작업)
        self.run_id = None
        self.dry_run = False
        self.stopping = False
        self.phase = None         # 이관: plan(1단계 준비) -> migrate(2단계 이관)
        self.adopted = False
        self._adopt()

    def _alive(self, pid):
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        # pid 재사용 방지: 정말 이 도구의 프로세스인지 확인
        try:
            cmd = subprocess.run(['ps', '-p', str(pid), '-o', 'command='], stdout=subprocess.PIPE,
                                 universal_newlines=True, timeout=5).stdout
        except Exception:
            return True
        return 'hive_migrate.py' in cmd or 'hive_inventory.py' in cmd

    def running(self):
        if self.proc is not None:
            return self.proc.poll() is None
        return bool(self.pid) and self.finished is None and self._alive(self.pid)

    def _adopt(self):
        """웹 서버가 다시 떴을 때, 이전 서버가 시작한 작업이 아직 돌고 있으면 이어받기."""
        try:
            with open(self.pidfile, encoding='utf-8') as f:
                info = json.load(f)
        except (OSError, ValueError):
            return
        if not (info.get('pid') and self._alive(info['pid']) and os.path.exists(info.get('out', ''))):
            try:
                os.remove(self.pidfile)
            except OSError:
                pass
            return
        self.pid, self.out_path, self.kind = info['pid'], info['out'], info['kind']
        self.started = datetime.datetime.strptime(info['started'], '%Y-%m-%d %H:%M:%S')
        self.dry_run, self.parallel = info.get('dry_run', False), info.get('parallel')
        self.if_exists = info.get('if_exists')
        self.items = {k: 'PENDING' for k in info.get('items', [])}
        self.phase = info.get('phase')
        self.adopted = True
        keep_awake(self.pid)   # 서버를 다시 띄워 이어받은 작업도 잠자기 방지
        self.lines = ['*** 웹 서버 재시작: 실행 중인 작업(pid %d)을 이어받았습니다 ***' % self.pid]
        threading.Thread(target=self._pump, daemon=True).start()

    def _start(self, kind, args, items=None, dry_run=False, phase='plan', parallel=None, if_exists=None):
        with self.lock:
            if self.running():
                raise ValueError('이미 실행 중인 작업이 있습니다 (%s)' % self.kind)
            # 실행 중인 작업이 없을 때만 바꿈 (확인 전에 바꾸면 거절된 요청이 실행 중인 작업의 값을 덮어씀)
            self.parallel, self.if_exists = parallel, if_exists
            if parallel:   # 이전 실행이 남긴 제어 파일 값이 --parallel 을 덮지 않게, 프로세스보다 먼저 씀
                self._write_control(int(parallel))
            env = dict(os.environ, PYTHONUNBUFFERED='1')
            stamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            self.out_path = os.path.join(self.report_dir, 'ui_job_%s.out' % stamp)
            out = open(self.out_path, 'w', encoding='utf-8')
            self.proc = subprocess.Popen(
                [sys.executable, '-u'] + args, stdout=out, stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL, env=env,
                start_new_session=True)   # 중지 시 자식(beeline, distcp)까지 같이 종료 / 서버가 죽어도 계속 실행
            out.close()
            self.pid = self.proc.pid
            keep_awake(self.pid)
            self.kind, self.rc, self.finished, self.stopping, self.adopted = kind, None, None, False, False
            self.started = datetime.datetime.now()
            self.lines, self.dropped, self.run_id, self.dry_run = [], 0, None, dry_run
            self.items = {k: 'PENDING' for k in (items or [])}
            self.phase = phase if items else None
            self.lines.append('$ ' + ' '.join(os.path.relpath(a) if a.endswith('.py') else a for a in args))
            try:
                with open(self.pidfile, 'w', encoding='utf-8') as f:
                    json.dump({'pid': self.pid, 'kind': kind, 'out': self.out_path, 'items': list(self.items),
                               'dry_run': dry_run, 'phase': self.phase, 'parallel': getattr(self, 'parallel', None),
                               'if_exists': getattr(self, 'if_exists', None),
                               'started': self.started.strftime('%Y-%m-%d %H:%M:%S')}, f, ensure_ascii=False)
            except OSError:
                pass
        threading.Thread(target=self._pump, daemon=True).start()

    def _pump(self):
        """작업 출력 파일을 따라 읽으며 로그·상태 갱신 (tail -f)."""
        buf = ''
        with open(self.out_path, encoding='utf-8', errors='replace') as f:
            while True:
                chunk = f.read()
                if chunk:
                    buf += chunk
                    lines = buf.split('\n')
                    buf = lines.pop()
                    with self.lock:
                        for line in lines:
                            self.lines.append(line)
                            self._track(line)
                        if len(self.lines) > MAX_LINES:
                            cut = len(self.lines) - MAX_LINES
                            del self.lines[:cut]
                            self.dropped += cut
                    continue
                if not self.running():
                    rest = f.read()
                    if rest:
                        buf += rest
                        continue
                    break
                time.sleep(0.5)
        rc = self.proc.wait() if self.proc is not None else None
        with self.lock:
            if buf:
                self.lines.append(buf)
                self._track(buf)
            self.rc = rc
            self.finished = datetime.datetime.now()
            for k, v in self.items.items():   # 중지/중단으로 끝나지 못한 항목
                if v in ('PENDING', 'READY', 'RUNNING'):
                    self.items[k] = 'STOPPED' if self.stopping else 'NOT RUN'
            if self.stopping == 'force':   # 강제 종료: 로그상 진행 중이던 것도 결과를 알 수 없음
                self.lines.append('*** 강제 종료됨: 대상에 만들다 만 테이블이 남았을 수 있습니다. '
                                  '다시 실행할 때 "지우고 다시 만듦"을 사용하세요 ***')
            try:
                os.remove(self.pidfile)
            except OSError:
                pass

    # 테이블 상태 (이관 작업)
    #   PENDING : 1단계(준비) - 원본 메타데이터 조회 전
    #   READY   : 메타데이터 확인 완료, 2단계에서 이관 차례(병렬 슬롯)를 기다림
    #   RUNNING : 이관 중
    #   그 외   : 최종 결과 (OK, MISMATCH, FAILED, SKIPPED, STOPPED, ...)
    def _track(self, line):
        m = LOG_RE.match(line)
        if not m:
            return
        who, msg = m.group(1), m.group(2)
        if who == 'main' and msg.startswith('run_id='):
            self.run_id = msg.split()[0].split('=', 1)[1]
        if who == 'plan' and msg.startswith('tables='):
            self.phase = 'migrate'
        if who in self.items:
            if msg.startswith('=> '):
                self.items[who] = msg[3:].split(' ', 1)[0]
            elif msg.startswith('metadata ok'):
                self.items[who] = 'READY'
            elif self.items[who] in ('PENDING', 'READY') and self.phase == 'migrate':
                self.items[who] = 'RUNNING'

    def _write_sources(self, items, prefix):
        # 실행 중이면 쓰지 않음, 이름도 실행마다 다르게: 같은 초에 들어온 (거절될) 요청이 막 시작한 작업이
        # 읽을 목록 파일을 덮어쓰면 그 작업이 엉뚱한 목록으로 돎
        with self.lock:
            if self.running():
                raise ValueError('이미 실행 중인 작업이 있습니다 (%s)' % self.kind)
        now = datetime.datetime.now()
        stamp = now.strftime('%Y%m%d_%H%M%S')
        path = os.path.join(self.report_dir, '%s_%s_%06d.csv' % (prefix, stamp, now.microsecond))
        os.makedirs(self.report_dir, exist_ok=True)
        with open(path, 'w', newline='', encoding='utf-8') as f:
            f.write('# 웹 화면에서 생성 %s\n' % stamp)
            w = csv.writer(f)
            renamed = any(len(it) > 2 and it[2] and it[2] != it[0] for it in items)
            w.writerow(['db', 'table', 'tgt_db'] if renamed else ['db', 'table'])
            for it in items:
                w.writerow([it[0], it[1], it[2] if len(it) > 2 and it[2] else it[0]] if renamed else it[:2])
        return path

    def start_migration(self, items, opts):
        if not items:
            raise ValueError('이관할 테이블이 없습니다')
        src_csv = self._write_sources(items, 'ui_sources')
        args = [os.path.join(HERE, 'hive_migrate.py'), '-c', self.config, '-s', src_csv,
                '--report-dir', self.report_dir,
                '--if-exists', opts['if_exists'], '--validate', opts['validate'],
                '--parallel', str(opts['parallel']), '--control', self.control,
                '--repair-source', opts['repair_source']]
        if opts['dry_run']:
            args.append('--dry-run')
        self._start('dry-run' if opts['dry_run'] else 'migrate', args,
                    ['%s.%s' % it[:2] for it in items], opts['dry_run'],
                    parallel='%d' % opts['parallel'], if_exists=opts['if_exists'])

    def _write_control(self, n):
        tmp = self.control + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump({'parallel': n}, f)
        os.replace(tmp, self.control)   # atomic: the migrator never reads a half-written file

    def set_parallel(self, n):
        """실행 중인 이관의 동시 처리 수 변경. 늘리면 바로 더 시작하고, 줄이면 진행 중인 것은 끝까지 둠"""
        with self.lock:
            if not self.running() or self.kind not in ('migrate', 'dry-run'):
                raise ValueError('실행 중인 이관이 없습니다')
            self._write_control(n)
            self.parallel = '%d' % n
            try:   # keep the adopted-job record in step (web server restart)
                with open(self.pidfile, encoding='utf-8') as f:
                    info = json.load(f)
                info['parallel'] = self.parallel
                with open(self.pidfile, 'w', encoding='utf-8') as f:
                    json.dump(info, f, ensure_ascii=False)
            except (OSError, ValueError):
                pass

    def start_drop(self, items, dbs=()):
        """대상 클러스터에서 테이블 삭제 (이관 결과 칸의 [선택한 대상 테이블 삭제])"""
        if not items and not dbs:
            raise ValueError('삭제할 테이블이나 DB가 없습니다')
        args = [os.path.join(HERE, 'hive_migrate.py'), '-c', self.config,
                '-s', self._write_sources(items, 'ui_drop'), '--report-dir', self.report_dir,
                '--drop-target', '--parallel', '2']
        if dbs:
            args += ['--drop-databases', ','.join(dbs)]
        self._start('drop', args, ['%s.%s' % it for it in items], phase='migrate')

    def start_inventory(self):
        self._start('inventory', [os.path.join(HERE, 'hive_inventory.py'), '-c', self.config,
                                  '--all', '--report-dir', self.report_dir])

    def stop(self, mode='now'):
        """graceful: 진행 중인 테이블은 끝내고 새 테이블은 시작하지 않음 (SIGUSR1, 이관만)
        now:      진행 중인 명령까지 끊고, 만들다 만 대상 테이블은 되돌리고, YARN distcp 작업 종료 (SIGTERM)
        force:    즉시 강제 종료 (SIGKILL) - 되돌리기/결과 기록 없음. now 가 응답 없을 때만"""
        with self.lock:
            if not self.running():
                raise ValueError('실행 중인 작업이 없습니다')
            if mode == 'graceful' and self.kind not in ('migrate', 'dry-run', 'drop'):
                mode = 'now'   # 인벤토리는 읽기 전용이라 바로 끊어도 됨
            if mode == 'graceful':
                os.kill(self.pid, signal.SIGUSR1)
                msg = '진행 중인 테이블만 끝내고 중지'
            elif mode == 'now':
                os.killpg(self.pid, signal.SIGTERM)
                msg = '즉시 중지 (진행 중인 테이블은 되돌림)'
            elif mode == 'force':
                os.killpg(self.pid, signal.SIGKILL)
                msg = '강제 종료 (되돌리기와 결과 기록 없음)'
            else:
                raise ValueError('알 수 없는 중지 방식: %s' % mode)
            self.stopping = mode
            self.lines.append('*** 중지 요청: %s ***' % msg)

    def state(self, offset):
        with self.lock:
            start = max(offset - self.dropped, 0)
            vals = list(self.items.values())
            done = sum(1 for v in vals if v not in ('PENDING', 'READY', 'RUNNING'))
            meta = sum(1 for v in vals if v != 'PENDING')
            return {
                'kind': self.kind, 'running': self.running(), 'rc': self.rc,
                'dry_run': self.dry_run, 'run_id': self.run_id, 'stopping': self.stopping,
                'started': self.started.strftime('%H:%M:%S') if self.started else None,
                'finished': self.finished.strftime('%H:%M:%S') if self.finished else None,
                'lines': self.lines[start:], 'next': self.dropped + len(self.lines),
                'items': self.items, 'total': len(vals), 'done': done,
                'phase': self.phase, 'meta_done': meta, 'parallel': getattr(self, 'parallel', None),
                'if_exists': getattr(self, 'if_exists', None),
            }


# --------------------------------------------------------------------------- 대상 DB 목록
class _QuietLog:
    def info(self, who, msg): pass
    def debug(self, who, msg): pass


class TargetDbs:
    """대상 클러스터의 실제 상태: DB 목록 + DB별 테이블 목록 (SHOW DATABASES / SHOW TABLES).
    이관 결과 칸은 이 목록을 기준으로 그린다 (도구 기록이 아니라 대상 클러스터가 기준).
    beeline 기동 비용이 커서 DB 여러 개를 한 세션에서 조회하고, 결과는 캐시함."""
    TTL = 300
    DB_BATCH = 200   # 한 beeline 세션에서 SHOW TABLES 할 DB 수

    def __init__(self, config):
        cfg = hm.load_config(config)
        r = cfg['run']
        self.beeline = hm.Beeline('tgt', cfg['tgt']['beeline'], r.getint('call_timeout', 900), 1, _QuietLog())
        # 숨기는 DB: default(대상 기본 DB, 이관 대상 DB로 보지 않음) + 이관 도구가 쓰는 DB
        self.hidden = {'default', cfg['result'].get('db', 'migration_meta').lower(),
                       cfg['staging'].get('db', 'zz_migration_stage').lower()}
        self.lock = threading.Lock()
        self.dbs, self.tables, self.at, self.error = None, None, None, None

    def _load(self):
        got = hm.require(hm.split_marked(self.beeline.run(
            'ui', [hm.marker('DBS'), 'SHOW DATABASES'], retry=True)), 'DBS')
        dbs = sorted(d.strip() for d in got['DBS'] if d.strip() and d.strip().lower() not in self.hidden)
        tables = {}
        for i in range(0, len(dbs), self.DB_BATCH):
            chunk = dbs[i:i + self.DB_BATCH]
            stmts = []
            for n, db in enumerate(chunk):
                stmts += [hm.marker('T%d' % n), 'SHOW TABLES IN `%s`' % db]
            got = hm.require(hm.split_marked(self.beeline.run('ui', stmts, retry=True)),
                             *('T%d' % n for n in range(len(chunk))))
            for n, db in enumerate(chunk):
                tables[db] = sorted(t.strip().lower() for t in got['T%d' % n] if t.strip())
        return dbs, tables

    def get(self, refresh=False):
        with self.lock:   # 동시에 여러 번 조회하지 않게
            fresh = self.at and (datetime.datetime.now() - self.at).total_seconds() < self.TTL
            if refresh or not fresh or self.dbs is None:
                try:
                    self.dbs, self.tables = self._load()
                    self.at, self.error = datetime.datetime.now(), None
                except hm.CommandError as e:
                    self.error = str(e)
            return {'dbs': self.dbs or [], 'tables': self.tables, 'error': self.error,
                    'at': self.at.strftime('%H:%M:%S') if self.at else None}


# --------------------------------------------------------------------------- HTTP
class App:
    def __init__(self, config, report_dir):
        self.config = config
        self.inventory = Inventory(report_dir)
        self.history = History(report_dir)
        self.jobs = JobRunner(config, report_dir)
        self.target_dbs = TargetDbs(config)


def make_handler(app):
    class Handler(BaseHTTPRequestHandler):
        server_version = 'HiveMigrateUI'

        def log_message(self, fmt, *args):   # 폴링 요청 로그로 터미널이 넘치지 않게
            if not self.path.startswith('/api/job'):
                sys.stderr.write('%s %s\n' % (self.log_date_time_string(), fmt % args))

        def _send(self, code, body, ctype='application/json; charset=utf-8'):
            data = body if isinstance(body, bytes) else json.dumps(body, ensure_ascii=False).encode('utf-8')
            self.send_response(code)
            self.send_header('Content-Type', ctype)
            self.send_header('Content-Length', str(len(data)))
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            u = urlparse(self.path)
            q = {k: v[0] for k, v in parse_qs(u.query).items()}
            try:
                if u.path in ('/', '/index.html'):
                    with open(os.path.join(HERE, 'web', 'index.html'), 'rb') as f:
                        return self._send(200, f.read(), 'text/html; charset=utf-8')
                if u.path == '/api/info':
                    return self._send(200, {'config': app.config, 'inventory': app.inventory.info()})
                if u.path == '/api/source/dbs':
                    return self._send(200, app.inventory.db_list())
                if u.path == '/api/source/tables':
                    return self._send(200, app.inventory.tables(q.get('db', '')))
                if u.path == '/api/source/search':
                    rows, more = app.inventory.search(q.get('q', ''))
                    return self._send(200, {'rows': rows, 'more': more})
                if u.path == '/api/target/dbs':
                    return self._send(200, app.target_dbs.get(refresh=q.get('refresh') == '1'))
                if u.path == '/api/history':
                    return self._send(200, app.history.all())
                if u.path == '/api/job':
                    return self._send(200, dict(app.jobs.state(_int(q.get('offset')) or 0), boot=BOOT_ID))
                return self._send(404, {'error': 'not found'})
            except Exception as e:
                return self._send(500, {'error': '%s: %s' % (type(e).__name__, e)})

        def do_POST(self):
            # JSON 요청만 받음 (다른 사이트의 폼 전송으로 작업이 실행되는 것 방지)
            if not self.headers.get('Content-Type', '').startswith('application/json'):
                return self._send(415, {'error': 'application/json only'})
            try:
                n = int(self.headers.get('Content-Length') or 0)
                body = json.loads(self.rfile.read(n).decode('utf-8') or '{}')
                p = urlparse(self.path).path
                items = []
                for it in body.get('items', []):   # [db, table] 또는 [db, table, 대상 db]
                    db, table = it[0], it[1]
                    tgt = (it[2] if len(it) > 2 and it[2] else db).lower()
                    if not (NAME_RE.match(db) and NAME_RE.match(table) and NAME_RE.match(tgt)):
                        raise ValueError('잘못된 이름: %s.%s -> %s' % (db, table, tgt))
                    items.append((db, table, tgt) if p == '/api/migrate' else (db, table))
                if p == '/api/drop':
                    if body.get('confirm') != '삭제':   # 화면의 확인 입력이 없으면 거부
                        raise ValueError('삭제 확인이 없습니다')
                    dbs = [d for d in body.get('drop_dbs', []) if NAME_RE.match(d)]
                    app.jobs.start_drop(items, dbs)
                elif p == '/api/migrate':
                    opts = {
                        'dry_run': bool(body.get('dry_run')),
                        'if_exists': _choice(body.get('if_exists'), ('resume', 'replace', 'skip', 'fail')),
                        'validate': _choice(body.get('validate'), ('auto', 'checksum', 'count', 'files', 'none')),
                        'parallel': max(1, min(int(body.get('parallel') or 2), 64)),
                        'repair_source': 'true' if body.get('repair_source', True) else 'false',
                    }
                    app.jobs.start_migration(items, opts)
                elif p == '/api/parallel':
                    app.jobs.set_parallel(max(1, min(int(body.get('parallel') or 0) or 2, 64)))
                elif p == '/api/inventory':
                    app.jobs.start_inventory()
                elif p == '/api/stop':
                    app.jobs.stop(body.get('mode', 'now'))
                else:
                    return self._send(404, {'error': 'not found'})
                return self._send(200, {'ok': True})
            except ValueError as e:
                return self._send(400, {'error': str(e)})
            except Exception as e:
                return self._send(500, {'error': '%s: %s' % (type(e).__name__, e)})

    return Handler


def _choice(v, allowed):
    if v is None:
        return allowed[0]
    if v not in allowed:
        raise ValueError('허용되지 않는 값: %s' % v)
    return v


def main():
    ap = argparse.ArgumentParser(description='Hive 이관 웹 화면')
    ap.add_argument('-c', '--config', required=True, help='INI 설정 (예: conf/local.ini)')
    ap.add_argument('--host', default='127.0.0.1', help='기본 127.0.0.1 (이 서버에서만 접속)')
    ap.add_argument('--port', type=int, default=8080)
    ap.add_argument('--report-dir', default='reports')
    a = ap.parse_args()
    if not os.path.exists(a.config):
        sys.exit('설정 파일이 없습니다: %s' % a.config)
    app = App(os.path.abspath(a.config), os.path.abspath(a.report_dir))
    srv = ThreadingHTTPServer((a.host, a.port), make_handler(app))
    print('Hive 이관 화면: http://%s:%d/  (종료: Ctrl+C)' % (a.host, a.port), flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        if app.jobs.running():   # 작업은 웹 서버와 별개로 계속 돎. 다시 띄우면 이어받음
            print('실행 중인 작업(pid %d)은 계속 돕니다. 웹 서버를 다시 띄우면 이어서 보여줍니다.' % app.jobs.pid, flush=True)


if __name__ == '__main__':
    main()
