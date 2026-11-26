#!/usr/bin/env python3
"""Hive 2.x cluster-to-cluster migration.

Reads a sources CSV (db,table - '*' for a whole database) and migrates each
table from the SOURCE cluster to the TARGET cluster under the SAME db/table name:

  1. read DDL + metadata on the source       (SHOW CREATE TABLE, DESCRIBE FORMATTED)
  2. create the table on the target          (DDL rewritten: db name, LOCATION, props)
  3. copy the data with distcp               (pulled from the source HDFS)
  4. register partitions (MSCK), fix stats   (copy source stats, or ANALYZE)
  5. validate source vs target               (by size: checksum / row count / file level; partitions)

ACID tables (transactional=true) are never file-copied: their delta dirs carry
source-cluster transaction ids that the target metastore doesn't know. They go
src ACID -> src staging (plain ORC) -> distcp -> tgt staging -> INSERT into tgt ACID.
Views are recreated after all tables.
At the end, one row per table is appended to a result table on the target
cluster ([result] db/table): src/tgt names, src/tgt row counts, is_match.

Usage:
  python3 migrate/hive_migrate.py -c conf/local.ini -s conf/sources.csv [--dry-run]

Only the Python 3.6+ standard library is used, so it runs on a typical edge node.
"""
import argparse
import configparser
import csv
import datetime
import json
import os
import re
import shlex
import signal
import subprocess
import sys
import threading
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from history import History  # noqa: E402  (result CSV history, shared with the web UI)

# Table properties that must not be carried over in the DDL (stats are handled separately).
DROP_PROPS = {
    'transient_lastDdlTime', 'last_modified_by', 'last_modified_time',
    'numFiles', 'numRows', 'rawDataSize', 'totalSize', 'numPartitions',
    'COLUMN_STATS_ACCURATE',
}
STATS_PROPS = ('numFiles', 'numRows', 'rawDataSize', 'totalSize')
# EXPLAIN stage lines of a plan that runs a job (MapReduce in Hive 2 by default; Tez/Spark if configured)
EXEC_STAGES = ('Map Reduce', 'Tez', 'Spark')

# Config: only the cluster commands are required; everything else has a default
# here and can be overridden in the INI only when needed.
REQUIRED = {'src': ('beeline', 'hdfs_uri'), 'tgt': ('beeline', 'hadoop')}
DEFAULTS = {
    'tgt': {'hdfs_uri': ''},              # runs on the target cluster -> its default FS
    'distcp': {'command': '',             # '' -> "<tgt hadoop> distcp"
               'options': '-update -m 20', 'timeout': '7200'},
    'staging': {'db': 'zz_migration_stage'},
    'result': {'db': 'migration_meta', 'table': 'migration_result'},
    'inventory': {'batch_size': '200', 'path_batch_size': '100'},
    'run': {'parallel': '2', 'if_exists': 'resume', 'stats': 'copy', 'validate': 'auto',
            'checksum_max_bytes': '100G', 'count_max_bytes': '1T',
            'repair_source': 'true', 'call_timeout': '900', 'retries': '2', 'max_dynamic_partitions': '100000'},
}


def load_config(path, extra_required=None):
    """Read the INI, check required keys, fill in defaults for everything else."""
    cfg = configparser.ConfigParser(inline_comment_prefixes=(';', '#'))
    if not cfg.read(path, encoding='utf-8'):
        raise SystemExit('config not found: %s' % path)
    missing = []
    for sec, keys in list(REQUIRED.items()) + list((extra_required or {}).items()):
        for k in keys:
            if not cfg.get(sec, k, fallback='').strip():
                missing.append('[%s] %s' % (sec, k))
    if missing:
        raise SystemExit('%s: missing required setting(s): %s' % (path, ', '.join(missing)))
    for sec, kv in DEFAULTS.items():
        if not cfg.has_section(sec):
            cfg.add_section(sec)
        for k, v in kv.items():
            if not cfg.get(sec, k, fallback='').strip():
                cfg[sec][k] = v
    if not cfg['distcp']['command'].strip():
        cfg['distcp']['command'] = cfg['tgt']['hadoop'] + ' distcp'
    if not cfg.has_option('tgt', 'yarn'):   # used to kill distcp jobs on stop; '' = disabled
        h = shlex.split(cfg['tgt']['hadoop'])
        cfg['tgt']['yarn'] = ' '.join(shlex.quote(a) for a in h[:-1] + ['yarn']) \
            if h and os.path.basename(h[-1]) == 'hadoop' else ''
    return cfg


def parse_size(v):
    """'100G', '1.5T', '1048576' -> bytes (K/M/G/T/P are powers of 1024)."""
    m = re.match(r'^\s*(\d+(?:\.\d+)?)\s*([KMGTP]?)B?\s*$', str(v), re.I)
    if not m:
        raise ValueError('not a size: %r (e.g. 100G, 1T)' % v)
    return int(float(m.group(1)) * 1024 ** ' KMGTP'.index((m.group(2) or ' ').upper()))


VALIDATE_MODES = ('auto', 'checksum', 'count', 'files', 'none')
# what each validation mode runs on each side (see _validation_stmts)
SIDE_MODE = {'src': {'checksum': 'checksum', 'count': 'count', 'files': 'parts', 'none': 'parts'},
             'tgt': {'checksum': 'checksum', 'count': 'count', 'files': 'count', 'none': 'parts'}}

MAX_PARALLEL = 64     # upper bound for [run] parallel (also for a live change via --control)
CONTROL_POLL = 3      # seconds: how often a running migration looks at the control file


class CommandError(Exception):
    pass


class BadOutput(CommandError):
    """A result section came back but could not be parsed (e.g. a stray line instead of the count).
    Must never be read as a data difference: the read-only query is run again."""
    pass


class Background(object):
    """Run fn(*args) in a thread; result() re-raises its exception in the caller."""
    def __init__(self, fn, *args):
        self._exc = None

        def run():
            try:
                fn(*args)
            except BaseException as e:   # handed to the caller in result()
                self._exc = e
        self._thread = threading.Thread(target=run)
        self._thread.daemon = True
        self._thread.start()

    def join(self):
        self._thread.join()

    def result(self):
        self.join()
        if self._exc is not None:
            raise self._exc


# Stop requests (set from signal handlers, see main()):
#   STOP_NEW: don't start any more tables (SIGUSR1 = finish running tables, then stop)
#   STOP_NOW: also abort running ones (SIGTERM/SIGINT; the caller signals our child processes too)
STOP_NEW = threading.Event()
STOP_NOW = threading.Event()


def check_stop(force=False):
    if STOP_NOW.is_set() and not force:
        raise CommandError('stopped by user')


# --------------------------------------------------------------------------- logging
class Log:
    def __init__(self, path):
        self.lock = threading.Lock()
        self.f = open(path, 'a', encoding='utf-8')
        self.echo = True

    def _write(self, line, echo):
        with self.lock:
            if echo and self.echo:
                try:
                    print(line, flush=True)
                except (BrokenPipeError, OSError):   # whoever read our stdout is gone: keep going, log file only
                    self.echo = False
            self.f.write(line + '\n')
            self.f.flush()

    def info(self, who, msg):
        self._write('%s [%s] %s' % (datetime.datetime.now().strftime('%H:%M:%S'), who, msg), True)

    def debug(self, who, msg):
        self._write('%s [%s] %s' % (datetime.datetime.now().strftime('%H:%M:%S'), who, msg), False)


# --------------------------------------------------------------------------- command runners
def _run(args, timeout, input_text=None):
    """Run a command, return (rc, stdout, stderr). rc=-1 on timeout."""
    try:
        p = subprocess.run(args, input=input_text, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                           universal_newlines=True, timeout=timeout)
        return p.returncode, p.stdout, p.stderr
    except FileNotFoundError:
        raise CommandError('command not found: %s (check PATH / config)' % args[0])
    except subprocess.TimeoutExpired as e:
        dec = lambda b: b.decode('utf-8', 'replace') if isinstance(b, bytes) else (b or '')
        return -1, dec(e.stdout), dec(e.stderr)


def _error_text(stdout, stderr):
    lines = [l for l in (stderr + '\n' + stdout).splitlines()
             if re.search(r'Error|Exception|FAILED|No such file', l)]
    return ' | '.join(lines[-3:]) if lines else (stderr.strip().splitlines() or ['unknown error'])[-1]


def protect_semicolons(sql):
    """Beeline splits statements on ';' even inside string literals; Hive reads '\\073' as ';'."""
    return re.sub(r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"",
                  lambda m: m.group(0).replace(';', '\\073'), sql)


# beeline could not open a session: nothing was executed, so the call can be retried safely
CONN_ERROR_RE = re.compile(r'Could not open client transport|Connection refused|Failed to open new session|'
                           r'Could not establish connection|No route to host|'
                           r'is restarting|is not running', re.I)   # last two: docker exec (local env)
CONN_RETRY_WAITS = (10, 20, 40, 60)   # seconds; ~2 minutes in total (a HiveServer2 restart takes ~30-60s)


class Beeline:
    def __init__(self, name, cmd, timeout, retries, log):
        self.name, self.cmd, self.timeout, self.retries, self.log = name, shlex.split(cmd), timeout, retries, log

    def run(self, who, statements, retry=False, force=False):
        """Run statements in ONE beeline session; return stdout lines.
        retry=True only for read-only calls (an amd64 JVM under emulation can hang).
        force=True: run even after a stop request (rollback / result recording)."""
        sql = ';\n'.join(protect_semicolons(s.strip().rstrip(';')) for s in statements if s.strip()) + ';\n'
        self.log.debug(who, '%s SQL>\n%s' % (self.name, sql))
        # SQL goes through stdin, not -e: Linux caps a single argv entry at 128KB
        # ("argument list too long"), which a large batch easily exceeds.
        args = self.cmd + ['--silent=true', '--outputformat=tsv2', '--showHeader=false', '-f', '/dev/stdin']
        attempts = 1 + (self.retries if retry else 0)
        i, conn_tries = 0, 0
        while True:
            check_stop(force)
            rc, out, err = _run(args, self.timeout, sql)
            if STOP_NOW.is_set() and not force and rc != 0:
                raise CommandError('stopped by user')
            if rc == 0:
                # -f mode prints a blank separator line around each result set, and a slow query can
                # leave a ' \r' line (console progress erase) in stdout: drop whitespace-only lines.
                # Split on \n only - str.splitlines() would also break a value at \r, \x0b, \x1c..\x1e, \x85, \u2028.
                return [l.rstrip('\r') for l in out.split('\n') if l.strip()]
            # could not even connect (HiveServer2 restarting / overloaded): no statement ran,
            # so this is safe to retry for any call, read-only or not
            if CONN_ERROR_RE.search(err + out) and conn_tries < len(CONN_RETRY_WAITS):
                wait = CONN_RETRY_WAITS[conn_tries]
                conn_tries += 1
                self.log.info(who, '%s HiveServer2 not reachable, retrying in %ds (%d/%d)'
                              % (self.name, wait, conn_tries, len(CONN_RETRY_WAITS)))
                if STOP_NOW.wait(wait) and not force:   # wakes up early on a stop request
                    raise CommandError('stopped by user')
                continue
            hung = rc in (-1, 124, 137)
            i += 1
            if hung and i < attempts:
                self.log.info(who, '%s beeline timed out, retrying (%d/%d)' % (self.name, i, self.retries))
                continue
            raise CommandError('%s beeline %s: %s' % (
                self.name, 'timeout' if hung else 'rc=%d' % rc, _error_text(out, err)))


class Hadoop:
    def __init__(self, cmd, timeout, log):
        self.cmd, self.timeout, self.log = shlex.split(cmd), timeout, log

    def run(self, who, args, timeout=None, force=False):
        check_stop(force)
        self.log.debug(who, 'HADOOP> %s' % ' '.join(args))
        rc, out, err = _run(self.cmd + args, timeout or self.timeout)
        return rc, out, err

    def file_count(self, who, uri):
        """Number of files under uri, or None if the path doesn't exist."""
        rc, out, err = self.run(who, ['fs', '-count', uri])
        if rc != 0:
            if 'No such file' in err or 'No such file' in out:
                return None
            raise CommandError('hadoop fs -count %s: %s' % (uri, _error_text(out, err)))
        parts = out.split()
        return int(parts[1]) if len(parts) >= 3 else 0


# --------------------------------------------------------------------------- parsing helpers
def marker(name):
    return "SELECT '@@%s@@'" % name


def split_marked(lines):
    out, cur = {}, None
    for l in lines:
        m = re.match(r'^@@(\w+)@@$', l.strip())
        if m:
            cur = m.group(1)
            out[cur] = []
        elif cur is not None:
            out[cur].append(l)
    return out


def require(got, *names):
    """beeline can exit 0 with truncated output when the connection drops (e.g. HiveServer2
    restart). A missing result section must be an error, never read as '0 rows'."""
    missing = [n for n in names if n not in got]
    if missing:
        raise CommandError('incomplete beeline output (missing %s) - connection lost?' % ', '.join(missing))
    return got


def _unq(v):
    if len(v) >= 2 and v[0] == '"' and v[-1] == '"':
        return v[1:-1].replace('""', '"')
    return v


def parse_describe(lines):
    info = {'columns': [], 'partition_cols': [], 'location': None, 'table_type': None,
            'params': {}, 'view_text': None, 'num_buckets': None}
    section, in_params = 'cols', False
    for raw in lines:
        f = [_unq(x) for x in raw.split('\t')] + ['', '']
        f0, f1, f2 = f[0].strip(), f[1].strip(), f[2].strip()
        if f0.startswith('# Partition Information'):
            section = 'part'; continue
        if f0.startswith('# Detailed Table Information'):
            section = 'detail'; continue
        if f0.startswith('# Storage Information'):
            section, in_params = 'storage', False; continue
        if f0.startswith('# View Information'):
            section = 'view'; continue
        if f0.startswith('# col_name'):
            continue
        if section in ('cols', 'part'):
            if f0:
                info['columns' if section == 'cols' else 'partition_cols'].append((f0, f1))
        elif section == 'detail':
            if f0 == 'Location:':
                info['location'] = f1
            elif f0 == 'Table Type:':
                info['table_type'] = f1
            elif f0 in ('Owner:', 'CreateTime:'):
                info[f0[:-1].lower()] = f1
            elif f0 == 'Table Parameters:':
                in_params = True
            elif in_params and not f0 and f1 and f1 != 'NULL':
                info['params'][f1] = f2.replace('\\"', '"')
            elif in_params and f1 == 'NULL':
                in_params = False
        elif section == 'storage':
            if f0 == 'Num Buckets:':
                info['num_buckets'] = f1
            elif f0 == 'InputFormat:':
                info['input_format'] = f1
            elif f0 == 'SerDe Library:':
                info['serde'] = f1
        elif section == 'view':
            if f0 == 'View Expanded Text:':
                info['view_text'] = f1
    return info


def strip_uri(location):
    """hdfs://nn:8020/a/b -> /a/b"""
    return re.sub(r'^[A-Za-z][\w+.-]*://[^/]*', '', location or '')


def is_default_managed_path(path, db, tbl):
    p = path.rstrip('/').lower()
    if db.lower() == 'default':
        return p.endswith('/' + tbl.lower())
    return p.endswith('/%s.db/%s' % (db.lower(), tbl.lower()))


def rewrite_table_ddl(ddl, tgt_db, tbl, location_path):
    """Retarget a SHOW CREATE TABLE statement to tgt_db.tbl.
    location_path=None drops LOCATION (target default warehouse path)."""
    ddl = re.sub(r'^(CREATE (?:EXTERNAL )?TABLE )`[^`]+`(?:\.`[^`]+`)?',
                 lambda m: '%s`%s`.`%s`' % (m.group(1), tgt_db, tbl), ddl.strip(), count=1)
    if location_path:
        ddl = re.sub(r"LOCATION\s*\n\s*'[^']*'", "LOCATION\n  '%s'" % location_path, ddl)
    else:
        ddl = re.sub(r"LOCATION\s*\n\s*'[^']*'\s*\n?", '', ddl)

    def fix_props(m):
        props = re.findall(r"'((?:[^'\\]|\\.)*)'\s*=\s*'((?:[^'\\]|\\.)*)'", m.group(1))
        keep = [(k, v) for k, v in props if k not in DROP_PROPS]
        if not keep:
            return ''
        return 'TBLPROPERTIES (\n' + ',\n'.join("  '%s'='%s'" % kv for kv in keep) + ')'
    ddl = re.sub(r'TBLPROPERTIES \((.*)\)\s*$', fix_props, ddl, flags=re.S)
    return ddl.strip()


def rewrite_view_ddl(ddl, src_db, tgt_db, view, db_map=None):
    """db_map: {source db: target db} of this run. References to any renamed db in the view
    body are retargeted in one pass (so a->b, b->c never chains)."""
    m = {k.lower(): v for k, v in (db_map or {}).items() if k.lower() != v.lower()}
    if src_db.lower() != tgt_db.lower():
        m[src_db.lower()] = tgt_db
    head = re.match(r'^CREATE VIEW `[^`]+`(?:\.`[^`]+`)?', ddl.strip())
    body = ddl.strip()[head.end():] if head else ddl.strip()
    if m:
        body = re.sub(r'`([^`]+)`\.', lambda x: '`%s`.' % m.get(x.group(1).lower(), x.group(1)), body)
    return 'CREATE VIEW `%s`.`%s`' % (tgt_db, view) + body if head else body


def map_db_path(path, src_db, tgt_db):
    """Rename the database part of a path: components equal to <src_db> or <src_db>.db
    become <tgt_db> / <tgt_db>.db.  Returns (new_path, changed)."""
    if src_db.lower() == tgt_db.lower():
        return path, False
    out, changed = [], False
    for c in path.split('/'):
        if c.lower() == src_db.lower():
            c, changed = tgt_db, True
        elif c.lower() == src_db.lower() + '.db':
            c, changed = tgt_db + '.db', True
        out.append(c)
    return '/'.join(out), changed


def col_list(cols):
    return ',\n  '.join('`%s` %s' % (n, t) for n, t in cols)


def sql_str(v):
    """Hive string literal (Hive escapes quotes with a backslash, not by doubling)."""
    if v is None:
        return 'NULL'
    return "'%s'" % str(v).replace('\\', '\\\\').replace("'", "\\'")


def sql_num(v):
    return 'NULL' if v is None else str(int(v))


# --------------------------------------------------------------------------- job model
class Job:
    def __init__(self, src_db, table, tgt_db=None):
        # table names are always identical on the target; the database may be renamed (tgt_db)
        self.src_db, self.table, self.tgt_db = src_db, table, (tgt_db or src_db)
        self.kind = None           # EXTERNAL | MANAGED | ACID | VIEW | UNSUPPORTED
        self.ddl = None
        self.meta = None
        self.status = 'PENDING'
        self.messages = []
        self.src_rows = self.tgt_rows = self.tgt_stats_rows = None
        self.src_sum = self.tgt_sum = None
        self.src_parts = self.tgt_parts = None
        self.seconds = 0.0
        self.created = None        # set once the target table exists: what to undo if the table fails
        self.tgt_loc = None        # explicit target LOCATION (external / custom path), None = warehouse default
        self.src_files = None      # file count under the source table dir (plan-phase precheck), -1 = not checked
        self.tgt_pre = None        # plan-phase precheck of the target dir: {'path': p, 'files': n or None}
        self.fast = False          # empty source table: create + stats only (no count jobs, no distcp)
        self.validated_by = None   # validation actually used: checksum | count | files | none
        self.src_bytes = None      # source table dir size / file count (plan-phase path check)
        self.tgt_files = self.tgt_bytes = None   # target dir after the copy (files validation)
        self.flags = set()         # result CSV `flags`: rolled_back, rollback_failed, resumable (see history.py)

    @property
    def src(self):
        return '%s.%s' % (self.src_db, self.table)

    @property
    def tgt(self):
        return '%s.%s' % (self.tgt_db, self.table)

    @property
    def partitioned(self):
        return bool(self.meta and self.meta['partition_cols'])

    @property
    def is_match(self):
        """Source and target row counts are known and equal."""
        return self.src_rows is not None and self.tgt_rows is not None and self.src_rows == self.tgt_rows

    def fail(self, status, msg):
        self.status = status
        self.messages.append(msg)


# --------------------------------------------------------------------------- migrator
class Migrator:
    def __init__(self, cfg, log, dry_run, run_id='manual', report_dir=None):
        self.cfg, self.log, self.dry, self.run_id = cfg, log, dry_run, run_id
        # if_exists=resume reads past results once, before this run writes its own CSV
        self.history = History(report_dir) if report_dir else None
        if self.history:
            self.history.load()
        self.control_path, self._control_mtime = None, None   # see _control_parallel
        self.yarn_cmd = shlex.split(cfg['tgt'].get('yarn', ''))
        self.distcp_started = False
        r = cfg['run']
        timeout, retries = r.getint('call_timeout', 900), r.getint('retries', 2)
        self.src = Beeline('src', cfg['src']['beeline'], timeout, retries, log)
        self.tgt = Beeline('tgt', cfg['tgt']['beeline'], timeout, retries, log)
        self.tgt_fs = Hadoop(cfg['tgt']['hadoop'], timeout, log)
        self.src_uri = cfg['src']['hdfs_uri'].rstrip('/')
        self.tgt_uri = cfg['tgt']['hdfs_uri'].rstrip('/')
        self.distcp_cmd = shlex.split(cfg['distcp']['command'])
        self.distcp_opts = shlex.split(cfg['distcp'].get('options', '-update'))
        self.distcp_timeout = cfg['distcp'].getint('timeout', 7200)
        self.if_exists = r.get('if_exists', 'replace')
        self.stats_mode = r.get('stats', 'copy')
        self.validate = r.get('validate', 'auto')
        if self.validate not in VALIDATE_MODES:
            raise CommandError('[run] validate must be one of %s' % ', '.join(VALIDATE_MODES))
        try:   # validate=auto: checksum below the first size, row count below the second, file level above
            self.checksum_max = parse_size(r.get('checksum_max_bytes', '100G'))
            self.count_max = parse_size(r.get('count_max_bytes', '1T'))
        except ValueError as e:
            raise CommandError('[run] checksum_max_bytes / count_max_bytes: %s' % e)
        self.repair_source = r.getboolean('repair_source', True)
        self.parallel = r.getint('parallel', 2)
        self.meta_batch = r.getint('meta_batch', 50)      # tables per beeline session in the plan phase
        self.path_batch = cfg['inventory'].getint('path_batch_size', 100)   # paths per hadoop fs -count
        self.stage_db = cfg['staging'].get('db', 'zz_migration_stage')
        res = cfg['result'] if cfg.has_section('result') else {}
        self.result_db = res.get('db', 'migration_meta')
        self.result_table = res.get('table', 'migration_result')
        maxp = r.get('max_dynamic_partitions', '100000')
        self.dyn_part_settings = [
            'SET hive.exec.dynamic.partition=true',
            'SET hive.exec.dynamic.partition.mode=nonstrict',
            'SET hive.exec.max.dynamic.partitions=%s' % maxp,
            'SET hive.exec.max.dynamic.partitions.pernode=%s' % maxp,
        ]

    # ---------------------------------------------------------------- planning
    def expand_targets(self, rows):
        """CSV rows (db, table[, tgt_db]) -> Jobs. table '*' means every table/view in the database."""
        rows = [(r[0], r[1], (r[2] if len(r) > 2 and r[2] else r[0])) for r in rows]
        self.db_map = {}
        for db, _, tgt in rows:   # one target name per source database
            if self.db_map.setdefault(db, tgt) != tgt:
                raise CommandError('source database %s is mapped to two target names: %s, %s'
                                   % (db, self.db_map[db], tgt))
        got = require(split_marked(self.src.run('plan', [marker('DBS'), 'SHOW DATABASES'], retry=True)), 'DBS')
        src_dbs = {d.strip() for d in got['DBS'] if d.strip()}
        whole = sorted({r[0] for r in rows if r[1] == '*'})
        missing = [d for d in {r[0] for r in rows} if d not in src_dbs]
        if missing:
            raise CommandError('source database(s) not found: %s' % ', '.join(sorted(missing)))
        tables_by_db = {}
        if whole:
            stmts = []
            for i, db in enumerate(whole):
                stmts += [marker('DB%d' % i), 'SHOW TABLES IN `%s`' % db]
            got = require(split_marked(self.src.run('plan', stmts, retry=True)),
                          *('DB%d' % i for i in range(len(whole))))
            for i, db in enumerate(whole):
                tables_by_db[db] = [t.strip() for t in got.get('DB%d' % i, []) if t.strip()]
        jobs, seen, tgt_seen = [], set(), {}
        for db, tbl, tgt in rows:
            names = tables_by_db[db] if tbl == '*' else [tbl]
            for t in names:
                key = (db, t)
                if key in seen:
                    continue
                seen.add(key)
                tkey = (tgt.lower(), t.lower())
                if tkey in tgt_seen:   # two source dbs merged into one target db with the same table
                    raise CommandError('target %s.%s would be written by both %s.%s and %s.%s'
                                       % (tgt, t, tgt_seen[tkey], t, db, t))
                tgt_seen[tkey] = db
                jobs.append(Job(db, t, tgt))
        renamed = {k: v for k, v in self.db_map.items() if k != v}
        if renamed:
            self.log.info('plan', 'database rename: %s' % ', '.join('%s -> %s' % kv for kv in sorted(renamed.items())))
        return jobs

    def existing_target_tables(self, jobs):
        tgt_dbs = sorted({j.tgt_db for j in jobs})
        got = require(split_marked(self.tgt.run('plan', [marker('DBS'), 'SHOW DATABASES'], retry=True)), 'DBS')
        have = {d.strip() for d in got['DBS'] if d.strip()}
        result = {}
        present = [d for d in tgt_dbs if d in have]
        if present:
            stmts = []
            for i, db in enumerate(present):
                stmts += [marker('DB%d' % i), 'SHOW TABLES IN `%s`' % db]
            got = require(split_marked(self.tgt.run('plan', stmts, retry=True)),
                          *('DB%d' % i for i in range(len(present))))
            for i, db in enumerate(present):
                result[db] = {t.strip() for t in got.get('DB%d' % i, []) if t.strip()}
        return result

    def load_meta(self, job):
        """Read one table's DDL + DESCRIBE (fallback when a batch fails)."""
        if STOP_NEW.is_set():
            job.kind = 'UNSUPPORTED'
            job.fail('STOPPED', 'not started (stop requested)')
            self.log.info(job.src, '=> STOPPED not started')
            return
        try:
            got = require(split_marked(self.src.run(job.src, [
                marker('DDL'), 'SHOW CREATE TABLE `%s`.`%s`' % (job.src_db, job.table),
                marker('DESC'), 'DESCRIBE FORMATTED `%s`.`%s`' % (job.src_db, job.table),
            ], retry=True)), 'DDL', 'DESC')
            self._classify(job, got['DDL'], got['DESC'])
        except CommandError as e:
            job.kind = 'UNSUPPORTED'
            job.fail('FAILED', str(e))
            self.log.info(job.src, '=> %s %s' % (job.status, '; '.join(job.messages)))

    def _classify(self, job, ddl_lines, desc_lines):
        job.ddl = '\n'.join(ddl_lines)
        job.meta = parse_describe(desc_lines)
        ttype = job.meta['table_type']
        if ttype == 'VIRTUAL_VIEW':
            job.kind = 'VIEW'
        elif re.search(r'\bSTORED BY\b', job.ddl):
            job.kind = 'UNSUPPORTED'
            job.fail('UNSUPPORTED', 'storage-handler table (STORED BY) is not migrated')
        elif job.meta['params'].get('transactional', '').lower() == 'true':
            job.kind = 'ACID'
        elif ttype == 'EXTERNAL_TABLE':
            job.kind = 'EXTERNAL'
        elif ttype == 'MANAGED_TABLE':
            job.kind = 'MANAGED'
        else:
            job.kind = 'UNSUPPORTED'
            job.fail('UNSUPPORTED', 'table type %s is not migrated' % ttype)
        if job.kind != 'UNSUPPORTED':   # progress marker (the web UI counts these)
            self.log.info(job.src, 'metadata ok (%s)' % job.kind)
        else:                           # final for this run: log the verdict line like migrated tables
            self.log.info(job.src, '=> %s %s' % (job.status, '; '.join(job.messages)))

    def load_meta_batched(self, jobs):
        """Plan phase: DDL + DESCRIBE for many tables per beeline session (beeline start-up costs
        seconds; one call per table dominated the plan phase). A failing batch (e.g. a table
        dropped meanwhile stops beeline -f) falls back to one call per table."""
        if not jobs:
            return
        size = max(1, min(self.meta_batch, -(-len(jobs) // self.parallel)))   # keep all workers busy
        batches = [jobs[i:i + size] for i in range(0, len(jobs), size)]

        def one(batch):
            if STOP_NEW.is_set() or len(batch) == 1:
                for j in batch:
                    self.load_meta(j)
                return
            stmts = []
            for i, j in enumerate(batch):
                stmts += [marker('DDL%d' % i), 'SHOW CREATE TABLE `%s`.`%s`' % (j.src_db, j.table),
                          marker('DESC%d' % i), 'DESCRIBE FORMATTED `%s`.`%s`' % (j.src_db, j.table)]
            try:
                got = require(split_marked(self.src.run('plan', stmts, retry=True)),
                              *[n % i for i in range(len(batch)) for n in ('DDL%d', 'DESC%d')])
            except CommandError as e:
                self.log.info('plan', 'metadata batch of %d failed (%s), reading one by one'
                              % (len(batch), str(e)[:120]))
                for j in batch:
                    self.load_meta(j)
                return
            for i, j in enumerate(batch):
                self._classify(j, got['DDL%d' % i], got['DESC%d' % i])

        with ThreadPoolExecutor(self.parallel) as ex:
            list(ex.map(one, batches))

    def _target_location(self, job):
        """Explicit target LOCATION for external / custom-path tables (db part renamed), else None."""
        src_path = strip_uri(job.meta['location'])
        if job.kind == 'EXTERNAL' or not is_default_managed_path(src_path, job.src_db, job.table):
            tgt_loc, changed = map_db_path(src_path, job.src_db, job.tgt_db)
            if job.tgt_db != job.src_db:
                job.messages.append('path %s -> %s' % (src_path, tgt_loc) if changed else
                                    'path %s kept (no db name in it)' % src_path)
            return tgt_loc
        return None

    def _count_paths(self, uris):
        """hadoop fs -count for many paths per call -> {uri: file count, or None if missing}.
        Also fills self.path_info[uri] = (dir count, file count, bytes)."""
        result = {}
        self.path_info = getattr(self, 'path_info', {})
        uris = list(dict.fromkeys(uris))
        chunks = [uris[i:i + self.path_batch] for i in range(0, len(uris), self.path_batch)]

        def one(chunk):
            rc, out, err = self.tgt_fs.run('plan', ['fs', '-count'] + chunk)
            got = {}
            for line in out.splitlines():
                f = line.split()
                if len(f) >= 4 and f[0].isdigit() and f[1].isdigit() and f[2].isdigit():
                    got[f[-1].rstrip('/')] = (int(f[0]), int(f[1]), int(f[2]))
            for u in chunk:
                if u.rstrip('/') in got:
                    self.path_info[u] = got[u.rstrip('/')]
                    result[u] = got[u.rstrip('/')][1]
                elif 'No such file' in err and u in err:
                    result[u] = None
                # otherwise unknown (error): left out, the per-table checks run as before
        with ThreadPoolExecutor(self.parallel) as ex:
            list(ex.map(one, chunks))
        return result

    def precheck_paths(self, tables, existing):
        """Plan phase, one batched hadoop call instead of per-table ones:
        - source table dir file counts -> empty tables take the fast path
        - target dir file counts (external path / predicted managed path) -> no per-table check
        - source size -> validate=auto mode. Read-only, so a dry run does it too (shows the fast path and mode)."""
        if not tables:
            return
        tdbs = sorted({j.tgt_db for j in tables})
        present = [d for d in tdbs if d in existing]
        stmts = [marker('WH'), 'SET hive.metastore.warehouse.dir']
        for i, d in enumerate(present):
            stmts += [marker('DB%d' % i), 'DESCRIBE DATABASE `%s`' % d]
        dbloc = {}
        try:
            got = split_marked(self.tgt.run('plan', stmts, retry=True))
            wh = next((l.split('=', 1)[1].strip() for l in got.get('WH', []) if '=' in l), '')
            wh = strip_uri(wh).rstrip('/')
            for i, d in enumerate(present):
                for line in got.get('DB%d' % i, []):
                    loc = next((f.strip() for f in line.split('\t') if re.match(r'^[A-Za-z][\w+.-]*://|^/', f.strip())), None)
                    if loc:
                        dbloc[d] = strip_uri(loc).rstrip('/')
            for d in tdbs:
                if d not in dbloc and wh:
                    dbloc[d] = '%s/%s.db' % (wh, d.lower())
        except CommandError as e:
            self.log.info('plan', 'target db locations unknown (%s); managed paths checked per table' % str(e)[:100])

        uris = []
        for j in tables:
            j.tgt_loc = self._target_location(j)
            j.src_path = strip_uri(j.meta['location'])
            j.tgt_check = j.tgt_loc or ('%s/%s' % (dbloc[j.tgt_db], j.table.lower()) if j.tgt_db in dbloc else None)
            uris.append(self.src_uri + j.src_path)
            if j.tgt_check:
                uris.append(self.tgt_uri + j.tgt_check)
        counts = self._count_paths(uris)
        n_fast = 0
        for j in tables:
            su = self.src_uri + j.src_path
            j.src_files = counts.get(su, 0) if su in counts else -1
            info = self.path_info.get(su)
            j.src_dirs, j.src_bytes = (info[0], info[2]) if info else (None, None)
            if j.tgt_check and (self.tgt_uri + j.tgt_check) in counts:
                j.tgt_pre = {'path': j.tgt_check, 'files': counts[self.tgt_uri + j.tgt_check]}
            replace = self._action(j, existing)[0] == 'replace'
            # fast path: source dir has no files (0 rows for sure), not partitioned (partition dirs may live
            # elsewhere and must be registered), and no old files at an explicit target path to clean up
            stale = j.tgt_loc and (not j.tgt_pre or j.tgt_pre['files'])
            if j.src_files in (0, None) and not j.partitioned and not stale:
                j.fast = True
                n_fast += 1
        self.log.info('plan', 'path check: %d path(s) in %d call(s); %d empty table(s) take the fast path'
                      % (len(uris), -(-len(uris) // self.path_batch) if uris else 0, n_fast))

    # ---------------------------------------------------------------- shared SQL bits    # ---------------------------------------------------------------- shared SQL bits
    def validate_mode(self, job):
        """Validation for this object. validate=auto picks by the source size from the plan-phase path check:
        checksum (< checksum_max_bytes), count (< count_max_bytes), files (above). Unknown size -> count.
        ACID -> at most count: its staging INSERT reads the whole source anyway, and the copied files are
        the staging table's, not the table's."""
        if self.validate != 'auto':
            return self.validate
        b = job.src_bytes
        if job.kind == 'VIEW' or b is None:
            return 'count'
        if b < self.checksum_max:
            return 'checksum'
        if b < self.count_max or job.kind == 'ACID':
            return 'count'
        return 'files'

    def _validation_stmts(self, db, tbl, partitioned, with_stats_count, mode='checksum'):
        """Read-only validation statements for one side. mode (SIDE_MODE): checksum = count(1) + sum(hash(*)),
        count = count(1), parts = partition list only (no data read). Can be empty (parts, unpartitioned)."""
        fq = '`%s`.`%s`' % (db, tbl)
        stmts = []
        if mode in ('checksum', 'count'):
            agg = 'count(1), sum(hash(*))' if mode == 'checksum' else 'count(1), 0'
            stmts = ['SET hive.compute.query.using.stats=false', marker('CNT'), 'SELECT %s FROM %s' % (agg, fq)]
            if with_stats_count:  # what an end user's count(*) returns: plan only (no job), see _stat_count
                stmts += ['SET hive.compute.query.using.stats=true', marker('STATPLAN'),
                          'EXPLAIN SELECT count(*) FROM %s' % fq]
        if partitioned:
            stmts += [marker('PARTS'), 'SHOW PARTITIONS %s' % fq]
        return stmts

    @staticmethod
    def _read_counts(got, partitioned=False, stats=False, rows=True):
        """-> (rows, checksum, count(*) answered from stats?, partitions). rows=False: no count was asked."""
        require(got, *(('CNT',) if rows else ()) + (('PARTS',) if partitioned else ()) + (('STATPLAN',) if stats else ()))
        parts = sorted(p.strip() for p in got['PARTS'] if p.strip()) if 'PARTS' in got else None
        if not rows:
            return None, None, None, parts
        if not got['CNT']:
            raise CommandError('incomplete beeline output (empty count result) - connection lost?')
        cnt = got['CNT'][0].split('\t')
        if not cnt[0].strip().lstrip('-').isdigit():
            raise BadOutput('unreadable count result: %r' % got['CNT'][:3])
        rows = int(cnt[0])
        csum = cnt[1].strip() if len(cnt) > 1 else None
        stat = Migrator._plan_uses_stats(got['STATPLAN']) if 'STATPLAN' in got else None
        return rows, csum, stat, parts

    @staticmethod
    def _plan_uses_stats(plan):
        """EXPLAIN of count(*) with hive.compute.query.using.stats=true. Answered from metastore stats:
        a Fetch stage only. Otherwise the plan has an execution stage that reads the data."""
        if not any('STAGE PLANS' in l for l in plan):
            raise BadOutput('unreadable count(*) plan: %r' % plan[:3])
        return not any(l.strip() in EXEC_STAGES for l in plan)

    def _stat_count(self, who, fq, uses_stats, rows):
        """What an end user's count(*) returns. A plan that reads the data returns the real row count
        (already counted, so no second scan); only a stats answer is fetched - a metastore lookup."""
        if uses_stats is None:
            return None
        if not uses_stats:
            return rows
        for attempt in (1, 2):
            got = require(split_marked(self.tgt.run(who, ['SET hive.compute.query.using.stats=true', marker('STATCNT'),
                                                          'SELECT count(*) FROM %s' % fq], retry=True)), 'STATCNT')
            v = (got['STATCNT'] or [''])[0].strip()
            if v.lstrip('-').isdigit():
                self.log.info(who, 'count(*) on target is answered from stats: %s' % v)
                return int(v)
            if attempt == 2:
                raise BadOutput('unreadable count(*) result: %r' % got['STATCNT'][:3])

    def _query_counts(self, side, who, stmts, partitioned):
        """Run read-only validation statements and parse them; one more try on unreadable output."""
        if not stmts:
            return None, None, None, None
        for attempt in (1, 2):
            got = split_marked(side.run(who, stmts, retry=True))
            try:
                return self._read_counts(got, partitioned, marker('STATPLAN') in stmts, marker('CNT') in stmts)
            except BadOutput as e:
                if attempt == 2:
                    raise
                self.log.info(who, '%s %s - running the count again' % (side.name, e))

    def _distcp(self, job, src_path, tgt_path, replace):
        opts = list(self.distcp_opts)
        if replace and '-delete' not in opts:
            opts.append('-delete')   # mirror: drop leftover files of the replaced table
        check_stop()
        # job name lets a stop request find and kill this run's distcp jobs on YARN
        name = '-Dmapreduce.job.name=%s %s' % (self.yarn_tag, job.src)
        args = self.distcp_cmd + [name] + opts + [self.src_uri + src_path, self.tgt_uri + tgt_path]
        self.distcp_started = True
        self.log.info(job.src, 'distcp %s -> %s' % (src_path, tgt_path))
        self.log.debug(job.src, 'DISTCP> %s' % ' '.join(args))
        rc, out, err = _run(args, self.distcp_timeout)
        if rc != 0:
            check_stop()
            raise CommandError('distcp failed (rc=%d): %s' % (rc, _error_text(out, err)))

    def _stats_stmts(self, job, fq):
        if self.stats_mode == 'none' or job.kind == 'ACID':   # ACID: INSERT gathers stats itself
            return []
        if self.stats_mode == 'analyze':
            part = ' PARTITION(%s)' % ', '.join('`%s`' % c for c, _ in job.meta['partition_cols']) \
                if job.partitioned else ''
            return ['ANALYZE TABLE %s%s COMPUTE STATISTICS' % (fq, part)]
        if job.partitioned:   # partition-level stats aren't copied; MSCK-added partitions get scanned
            return []
        p = job.meta['params']
        if '"BASIC_STATS":"true"' in p.get('COLUMN_STATS_ACCURATE', ''):
            props = ["'%s'='%s'" % (k, p[k]) for k in STATS_PROPS if k in p]
            props.append("'COLUMN_STATS_ACCURATE'='{\"BASIC_STATS\":\"true\"}'")
        else:  # a fresh managed table claims "0 rows, accurate" -> must be invalidated
            props = ["'COLUMN_STATS_ACCURATE'='false'"]
        return ['ALTER TABLE %s SET TBLPROPERTIES (%s)' % (fq, ', '.join(props))]

    def _repair_source_partitions(self, job):
        """Source partitioned table reads 0 rows: its partition dirs may exist on HDFS without being
        registered in the source metastore. MSCK REPAIR on the source, then re-read the source numbers.
        Returns True if partitions were added. (Only called for 0-row tables, so healthy tables never
        cause a metastore write or an HDFS directory scan on the source.)"""
        fq = '`%s`.`%s`' % (job.src_db, job.table)
        n0 = len(job.src_parts or [])
        self.log.info(job.src, 'source has 0 rows -> MSCK REPAIR on source (partitions in metastore: %d)' % n0)
        self.src.run(job.src, ['MSCK REPAIR TABLE %s' % fq], retry=True)
        job.src_rows, job.src_sum, _, job.src_parts = self._query_counts(self.src, job.src, self._validation_stmts(
            job.src_db, job.table, True, False, SIDE_MODE['src'][job.validated_by]), True)
        added = len(job.src_parts) - n0
        if added > 0:
            self.log.info(job.src, 'source MSCK REPAIR registered %d partition(s), source rows now %s'
                          % (added, job.src_rows))
            job.messages.append('source MSCK repaired %d partition(s)' % added)
        else:
            self.log.info(job.src, 'source MSCK REPAIR found nothing to add (table is really empty)')
        return added > 0

    # ---------------------------------------------------------------- target already there?
    def _action(self, job, existing):
        """What to do with the target object: ('new'|'replace'|'skip'|'fail', message).
        resume: skip only when the last result for this source -> target is OK (history) and the object is
        on the target; anything else that is on the target (failed, mismatch, not made by this tool) is replaced."""
        if job.table not in existing.get(job.tgt_db, set()):
            return 'new', None
        if self.if_exists == 'skip':   # (decided before the source metadata is read: kind not known yet)
            return 'skip', 'already on target (if_exists=skip)'
        if self.if_exists == 'fail':
            return 'fail', 'already on target (if_exists=fail)'
        if self.if_exists == 'resume':
            run = self.history.done_ok(job.src, job.tgt) if self.history else None
            if run:
                return 'skip', 'already OK (run %s)' % run
        return 'replace', None

    def skip_done(self, objs, existing):
        """Plan phase: settle the objects that are not migrated in this run (skip / fail because the target
        exists), so they cost no path checks and no worker. Returns the objects that still have to run."""
        rest, n = [], {'skip': 0, 'fail': 0, 'replace': 0}
        for j in objs:
            act, msg = self._action(j, existing)
            if act in n:
                n[act] += 1
            if act in ('skip', 'fail'):
                j.fail('SKIPPED' if act == 'skip' else 'FAILED', msg)
                self.log.info(j.src, '=> %s %s' % (j.status, msg))
            else:
                rest.append(j)
        if any(n.values()):
            self.log.info('plan', 'already on target (if_exists=%s): %d skipped, %d failed, %d to re-create'
                          % (self.if_exists, n['skip'], n['fail'], n['replace']))
        return rest

    # ---------------------------------------------------------------- one table
    @property
    def yarn_tag(self):
        return 'hive_migrate_%s' % self.run_id

    def migrate_table(self, job, existing):
        t0 = time.time()
        if STOP_NEW.is_set():
            job.fail('STOPPED', 'not started (stop requested)')
            self.log.info(job.src, '=> STOPPED not started')
            return
        try:
            self._migrate_table(job, existing)
        except CommandError as e:
            job.fail('STOPPED' if STOP_NOW.is_set() else 'FAILED', str(e))
        except Exception as e:  # keep going with the other tables
            job.fail('STOPPED' if STOP_NOW.is_set() else 'FAILED', '%s: %s' % (type(e).__name__, e))
        if job.status in ('FAILED', 'STOPPED'):
            if job.created:
                self._rollback(job)
            elif job.kind == 'ACID' and not self.dry:   # failed before the target table existed
                self._drop_source_stage(job)
        job.seconds = time.time() - t0
        self.log.info(job.src, '=> %s %s' % (job.status, '; '.join(job.messages)))

    def _rollback(self, job):
        """A table that failed half-way must not stay on the target: the next run with
        if_exists=skip would skip it as if it were migrated. Undo what this run created."""
        c = job.created
        try:
            stmts = ['DROP TABLE IF EXISTS %s PURGE' % c['table']]   # managed: data goes too
            if c.get('stage'):
                stmts.append('DROP TABLE IF EXISTS %s PURGE' % c['stage'])
            self.tgt.run(job.src, stmts, force=True)
            if c.get('path'):   # external / custom location: the files are not owned by the table
                rc, out, err = self.tgt_fs.run(job.src, ['fs', '-rm', '-r', '-f', '-skipTrash',
                                                         self.tgt_uri + c['path']], force=True)
                if rc != 0:
                    raise CommandError('rm %s: %s' % (c['path'], _error_text(out, err)))
                self._remove_empty_parents(job.src, c['path'], self._db_location(job.src, job.tgt_db))
            if c.get('stage'):
                self._drop_source_stage(job)
            job.messages.append('rolled back (target table%s removed)' % (' and files' if c.get('path') else ''))
            job.flags.add('rolled_back')
            self.log.info(job.src, 'rolled back: dropped %s on target' % job.tgt)
        except CommandError as e:
            job.messages.append('ROLLBACK FAILED, clean up by hand: %s' % e)
            job.flags.add('rollback_failed')
            self.log.info(job.src, 'ROLLBACK FAILED: %s' % e)

    def _stage_name(self, job):
        return '`%s`.`%s__%s`' % (self.stage_db, job.src_db, job.table)

    def _drop_source_stage(self, job):
        """ACID staging copy on the source; never fails the table (only leaves a note)."""
        try:
            self.src.run(job.src, ['DROP TABLE IF EXISTS %s PURGE' % self._stage_name(job)], force=True)
        except CommandError as e:
            job.messages.append('source staging table not dropped: %s' % e)
            self.log.info(job.src, 'could not drop source staging table: %s' % e)

    def kill_yarn_jobs(self):
        """After a hard stop: distcp jobs already submitted to YARN keep running without their client."""
        if not (self.distcp_started and self.yarn_cmd):
            return
        rc, out, err = _run(self.yarn_cmd + ['application', '-list', '-appStates', 'NEW,NEW_SAVING,SUBMITTED,ACCEPTED,RUNNING'], 120)
        if rc != 0:
            self.log.info('stop', 'could not list YARN applications (rc=%s): %s' % (rc, _error_text(out, err)))
            return
        apps = [l.split()[0] for l in out.splitlines() if l.startswith('application_') and self.yarn_tag in l]
        for app in apps:
            rc, out, err = _run(self.yarn_cmd + ['application', '-kill', app], 120)
            self.log.info('stop', 'yarn kill %s: %s' % (app, 'ok' if rc == 0 else _error_text(out, err)))
        if not apps:
            self.log.info('stop', 'no running distcp job of this run on YARN')

    def _migrate_empty(self, job, replace, tgt_loc, new_ddl, fq_tgt):
        """Source table dir has no files -> 0 rows for sure. One target session: create, set exact
        stats (0 rows), read count(*) from stats. No count jobs, no distcp, no ACID staging."""
        self.log.info(job.src, 'empty source (no files): fast path')
        job.src_rows, job.src_sum, job.src_parts = 0, None, None
        stmts = ['CREATE DATABASE IF NOT EXISTS `%s`' % job.tgt_db]
        if replace:
            stmts.append('DROP TABLE IF EXISTS %s PURGE' % fq_tgt)
        stmts += [new_ddl, marker('DESC'), 'DESCRIBE FORMATTED %s' % fq_tgt]
        if job.kind != 'ACID':   # exact stats (0 rows); then check what count(*) returns
            stmts += ["ALTER TABLE %s SET TBLPROPERTIES ('numRows'='0', 'rawDataSize'='0', 'totalSize'='0', "
                      "'numFiles'='0', 'COLUMN_STATS_ACCURATE'='{\"BASIC_STATS\":\"true\"}')" % fq_tgt,
                      'SET hive.compute.query.using.stats=true', marker('STATPLAN'),
                      'EXPLAIN SELECT count(*) FROM %s' % fq_tgt]
        self.log.info(job.src, 'creating %s on target' % job.tgt)
        job.created = {'table': fq_tgt, 'path': tgt_loc, 'stage': None}
        got = require(split_marked(self.tgt.run(job.src, stmts)), 'DESC')
        path = strip_uri(parse_describe(got['DESC'])['location']).rstrip('/')
        # the new table must sit on an empty directory (checked in the plan phase, or now)
        if replace and not tgt_loc:
            n = 0   # replaced managed table: DROP ... PURGE removed the old directory with its files
        else:       # external path (precheck found it empty) or new table: plan-phase count, else check now
            pre = job.tgt_pre if job.tgt_pre and job.tgt_pre['path'] == path else None
            n = pre['files'] if pre else self.tgt_fs.file_count(job.src, self.tgt_uri + path)
        if n:
            raise CommandError('target table path %s already holds %d file(s)' % (path, n))
        job.tgt_rows, job.tgt_sum, job.tgt_parts = 0, None, None
        if job.kind != 'ACID':   # a scan of the (checked) empty directory returns 0
            job.tgt_stats_rows = self._stat_count(job.src, fq_tgt, self._plan_uses_stats(
                require(got, 'STATPLAN')['STATPLAN']), 0)
            if job.tgt_stats_rows != 0:
                raise CommandError('count(*) on the new empty table returns %r' % job.tgt_stats_rows)
        job.created = None
        job.messages.append('empty table (fast path)')
        job.status = 'OK'

    def _migrate_table(self, job, existing):
        m = job.meta
        act, msg = self._action(job, existing)
        if act in ('skip', 'fail'):
            job.fail('SKIPPED' if act == 'skip' else 'FAILED', msg)
            return
        replace = act == 'replace'

        src_path = strip_uri(m['location'])
        # same path on the target cluster for external / custom paths (db part renamed); None = warehouse
        tgt_loc = job.tgt_loc if hasattr(job, 'tgt_check') else self._target_location(job)
        new_ddl = rewrite_table_ddl(job.ddl, job.tgt_db, job.table, tgt_loc)
        fq_tgt = '`%s`.`%s`' % (job.tgt_db, job.table)
        stage = self._stage_name(job)

        job.validated_by = 'files' if job.fast else self.validate_mode(job)   # empty: 0 files proves 0 rows
        if self.dry:
            self.log.info(job.src, 'DRY-RUN kind=%s partitioned=%s size=%s validate=%s%s -> %s%s\n%s' % (
                job.kind, job.partitioned, '?' if job.src_bytes is None else job.src_bytes, job.validated_by,
                ' (empty, fast path)' if job.fast else '', job.tgt, ' (REPLACE)' if replace else '', new_ddl))
            job.status = 'DRY-RUN'
            return

        if job.fast:
            return self._migrate_empty(job, replace, tgt_loc, new_ddl, fq_tgt)

        # 1) source side: validation numbers (+ staging copy for ACID). Non-ACID: the source count runs
        #    alongside the target steps (create, distcp, finalize) and is joined before the verdict.
        self.log.info(job.src, 'reading source (%s%s)' % (job.kind, ', partitioned' if job.partitioned else ''))
        src_work = None
        if job.validated_by == 'files' and not job.partitioned:
            # no source scan: the source row count comes from the source stats when they are exact
            # (partitioned: table-level numRows isn't kept up to date -> not used)
            p = m['params']
            if '"BASIC_STATS":"true"' in p.get('COLUMN_STATS_ACCURATE', '') and p.get('numRows', '').isdigit():
                job.src_rows = int(p['numRows'])
        if job.partitioned or SIDE_MODE['src'][job.validated_by] != 'parts':   # anything to read on the source
            if job.kind == 'ACID':
                self._read_source(job)
            else:
                src_work = Background(self._read_source, job)
        try:
            self._migrate_target(job, replace, src_path, tgt_loc, new_ddl, fq_tgt, stage)
        finally:
            if src_work:   # never leave a source query running behind a finished or failed table
                src_work.join()
        if src_work:
            src_work.result()   # source failed: raises, the target table is rolled back

        job.created = None   # target is complete; from here on nothing must be rolled back
        if job.kind == 'ACID':
            self._drop_source_stage(job)

        # 5) verdict
        problems = []
        mode = job.validated_by
        if mode == 'files':   # no source scan: the copied files, plus the source row count from exact stats
            if (job.src_files, job.src_bytes) != (job.tgt_files, job.tgt_bytes):
                problems.append('files src=%s (%s bytes) tgt=%s (%s bytes)'
                                % (job.src_files, job.src_bytes, job.tgt_files, job.tgt_bytes))
            else:
                job.messages.append('validated by files: %s file(s), %s bytes' % (job.tgt_files, job.tgt_bytes))
        if mode in ('checksum', 'count') or (mode == 'files' and job.src_rows is not None):
            if job.src_rows != job.tgt_rows:
                problems.append('rows src=%s%s tgt=%s' % (job.src_rows, ' (stats)' if mode == 'files' else '',
                                                          job.tgt_rows))
        if mode == 'checksum' and job.src_sum != job.tgt_sum:
            problems.append('checksum src=%s tgt=%s' % (job.src_sum, job.tgt_sum))
        if mode != 'none':
            if job.tgt_stats_rows is not None and job.tgt_stats_rows != job.tgt_rows:
                problems.append('count(*) on target returns %s (stats) but table has %s rows'
                                % (job.tgt_stats_rows, job.tgt_rows))
        if job.partitioned and job.src_parts != job.tgt_parts:
            miss = sorted(set(job.src_parts or []) - set(job.tgt_parts or []))
            problems.append('partitions src=%d tgt=%d missing=%s' % (
                len(job.src_parts or []), len(job.tgt_parts or []), miss[:5]))
        if problems:
            job.fail('MISMATCH', '; '.join(problems))
        else:
            job.status = 'OK'

    def _read_source(self, job):
        """Source validation numbers; MSCK REPAIR on the source for a 0-row partitioned table."""
        m = job.meta
        job.src_rows, job.src_sum, _, job.src_parts = self._query_counts(self.src, job.src, self._validation_stmts(
            job.src_db, job.table, job.partitioned, False, SIDE_MODE['src'][job.validated_by]), job.partitioned)
        # 0 rows in a partitioned table: partitions may be missing from the source metastore
        # (MSCK only matters for partitioned tables; it doesn't apply to ACID)
        if job.src_rows == 0 and job.partitioned and job.kind != 'ACID' and self.repair_source:
            # one partition column: table dir holds one dir per partition. If the dir count matches the
            # partitions the metastore knows, nothing is unregistered -> skip MSCK (no source write)
            ndirs = getattr(job, 'src_dirs', None)
            if ndirs is not None and len(m['partition_cols']) == 1 and ndirs - 1 == len(job.src_parts or []):
                self.log.info(job.src, 'source has 0 rows; all %d partition dir(s) registered, MSCK skipped'
                              % len(job.src_parts or []))
            else:
                self._repair_source_partitions(job)

    def _migrate_target(self, job, replace, src_path, tgt_loc, new_ddl, fq_tgt, stage):
        """Steps 1b-4: ACID staging copy, target table, data, finalize + target validation numbers."""
        m = job.meta
        copy_from = src_path
        if job.kind == 'ACID':
            pcols = m['partition_cols']
            stage_ddl = 'CREATE TABLE %s (\n  %s)%s\nSTORED AS ORC' % (
                stage, col_list(m['columns']),
                '\nPARTITIONED BY (%s)' % ', '.join('`%s` %s' % c for c in pcols) if pcols else '')
            ins = 'INSERT OVERWRITE TABLE %s%s SELECT * FROM `%s`.`%s`' % (
                stage, ' PARTITION(%s)' % ', '.join('`%s`' % c for c, _ in pcols) if pcols else '',
                job.src_db, job.table)
            self.log.info(job.src, 'ACID: copying into source staging table %s' % stage)
            got = split_marked(self.src.run(job.src, [
                'CREATE DATABASE IF NOT EXISTS `%s`' % self.stage_db,
                'DROP TABLE IF EXISTS %s PURGE' % stage, stage_ddl] + self.dyn_part_settings + [
                ins, marker('DESC'), 'DESCRIBE FORMATTED %s' % stage]))
            copy_from = strip_uri(parse_describe(require(got, 'DESC')['DESC'])['location'])

        # 2) target side: refuse to land on foreign data in an external path
        if tgt_loc and not replace:
            pre = job.tgt_pre if job.tgt_pre and job.tgt_pre['path'] == tgt_loc else None
            n = pre['files'] if pre else self.tgt_fs.file_count(job.src, self.tgt_uri + tgt_loc)
            if n:
                raise CommandError('target path %s already holds %d file(s); '
                                   'use if_exists=replace or clean it first' % (tgt_loc, n))

        stmts = ['CREATE DATABASE IF NOT EXISTS `%s`' % job.tgt_db]
        if replace:
            stmts.append('DROP TABLE IF EXISTS %s' % fq_tgt)
        stmts += [new_ddl, marker('DESC'), 'DESCRIBE FORMATTED %s' % fq_tgt]
        if job.kind == 'ACID':
            stmts += ['CREATE DATABASE IF NOT EXISTS `%s`' % self.stage_db,
                      'DROP TABLE IF EXISTS %s PURGE' % stage, stage_ddl,
                      marker('SDESC'), 'DESCRIBE FORMATTED %s' % stage]
        self.log.info(job.src, 'creating %s on target' % job.tgt)
        job.created = {'table': fq_tgt, 'path': tgt_loc, 'stage': stage if job.kind == 'ACID' else None}
        got = split_marked(self.tgt.run(job.src, stmts))
        tgt_table_path = strip_uri(parse_describe(require(got, 'DESC')['DESC'])['location'])
        copy_to = strip_uri(parse_describe(require(got, 'SDESC')['SDESC'])['location']) if job.kind == 'ACID' else tgt_table_path

        if not tgt_loc and not replace:   # managed default path: must be empty before copying
            pre = job.tgt_pre if job.tgt_pre and job.tgt_pre['path'] == copy_to.rstrip('/') else None
            n = pre['files'] if pre else self.tgt_fs.file_count(job.src, self.tgt_uri + copy_to)
            if n:
                raise CommandError('target table path %s already holds %d file(s)' % (copy_to, n))

        # 3) data
        if job.kind == 'ACID' and job.src_rows == 0 and not job.partitioned:
            # (non-ACID: the source count is still running, so always copy; a table with no files at all
            # took the fast path. Partitioned: empty partition dirs still need copying)
            self.log.info(job.src, 'source has 0 rows, skipping distcp')
        else:
            self._distcp(job, copy_from, copy_to, replace and job.kind != 'ACID')
        if job.validated_by == 'files':   # (never ACID) what landed on the target, compared in the verdict
            rc, out, err = self.tgt_fs.run(job.src, ['fs', '-count', self.tgt_uri + copy_to])
            f = out.split()
            if rc != 0 or len(f) < 4 or not (f[1].isdigit() and f[2].isdigit()):
                raise CommandError('hadoop fs -count %s after the copy: %s' % (copy_to, _error_text(out, err)))
            job.tgt_files, job.tgt_bytes = int(f[1]), int(f[2])

        # 4) partitions / ACID load / stats / validation on target
        post = []
        if job.kind == 'ACID':
            pcols = m['partition_cols']
            if pcols:
                post.append('MSCK REPAIR TABLE %s' % stage)
            post += self.dyn_part_settings + [
                'INSERT INTO TABLE %s%s SELECT * FROM %s' % (
                    fq_tgt, ' PARTITION(%s)' % ', '.join('`%s`' % c for c, _ in pcols) if pcols else '', stage),
                'DROP TABLE IF EXISTS %s PURGE' % stage]
        elif job.partitioned:
            post.append('MSCK REPAIR TABLE %s' % fq_tgt)
        post += self._stats_stmts(job, fq_tgt)
        vstmts = self._validation_stmts(job.tgt_db, job.table, job.partitioned, True, SIDE_MODE['tgt'][job.validated_by])
        post += vstmts
        if not post:   # nothing to finalize or read (validate=none, unpartitioned, stats=none)
            return
        self.log.info(job.src, 'finalizing on target (%s)' % ', '.join(
            s.split(' ')[0] for s in post if not s.startswith(('SET', "SELECT '@@")))[:80])
        got = split_marked(self.tgt.run(job.src, post))
        rows = marker('CNT') in vstmts
        try:
            job.tgt_rows, job.tgt_sum, uses_stats, job.tgt_parts = self._read_counts(got, job.partitioned, rows, rows)
        except BadOutput as e:   # the writes above are done; re-run only the read-only validation part
            self.log.info(job.src, 'tgt %s - running the validation again' % e)
            job.tgt_rows, job.tgt_sum, uses_stats, job.tgt_parts = self._query_counts(
                self.tgt, job.src, vstmts, job.partitioned)
        job.tgt_stats_rows = self._stat_count(job.src, fq_tgt, uses_stats, job.tgt_rows)

    # ---------------------------------------------------------------- views
    def migrate_view(self, job, existing):
        t0 = time.time()
        try:
            act, msg = self._action(job, existing)
            if act in ('skip', 'fail'):
                job.fail('SKIPPED' if act == 'skip' else 'FAILED', msg)
                return True
            exists = act == 'replace'
            ddl = rewrite_view_ddl(job.ddl, job.src_db, job.tgt_db, job.table, getattr(self, 'db_map', None))
            if self.dry:
                self.log.info(job.src, 'DRY-RUN kind=VIEW -> %s\n%s' % (job.tgt, ddl))
                job.status = 'DRY-RUN'
                return True
            stmts = ['CREATE DATABASE IF NOT EXISTS `%s`' % job.tgt_db]
            if exists:
                stmts.append('DROP VIEW IF EXISTS `%s`.`%s`' % (job.tgt_db, job.table))
            stmts.append(ddl)
            job.validated_by = 'none' if self.validate == 'none' else 'count'   # views: row count only
            if self.validate != 'none':
                stmts += ['SET hive.compute.query.using.stats=false', marker('CNT'),
                          'SELECT count(1), 0 FROM `%s`.`%s`' % (job.tgt_db, job.table)]
            got = split_marked(self.tgt.run(job.src, stmts))
            job.tgt_rows = self._read_counts(got)[0] if self.validate != 'none' else None
            if self.validate != 'none':
                got = split_marked(self.src.run(job.src, [
                    'SET hive.compute.query.using.stats=false', marker('CNT'),
                    'SELECT count(1), 0 FROM `%s`.`%s`' % (job.src_db, job.table)], retry=True))
                job.src_rows = self._read_counts(got)[0]
            if self.validate != 'none' and job.src_rows != job.tgt_rows:
                job.fail('MISMATCH', 'rows src=%s tgt=%s' % (job.src_rows, job.tgt_rows))
            else:
                job.status = 'OK'
            return True
        except CommandError as e:
            job.messages = [str(e)]
            job.status = 'FAILED'
            return False   # may depend on another view: retried in the next round
        finally:
            job.seconds += time.time() - t0
            if job.status != 'FAILED':
                self.log.info(job.src, '=> %s %s' % (job.status, '; '.join(job.messages)))

    # ---------------------------------------------------------------- result table
    def write_result_table(self, run_id, started, jobs):
        """Append one row per migrated object to the result table on the target cluster.
        SKIPPED / DRY-RUN objects weren't migrated in this run and are not recorded."""
        done = [j for j in jobs if j.status not in ('SKIPPED', 'DRY-RUN')]
        if self.dry or not done:
            return
        fq = '`%s`.`%s`' % (self.result_db, self.result_table)
        ts = started.strftime('%Y-%m-%d %H:%M:%S')
        values = ',\n'.join('(%s)' % ', '.join([
            sql_str(run_id), sql_str(ts), sql_str(j.src_db), sql_str(j.table),
            sql_str(j.tgt_db), sql_str(j.table), sql_num(j.src_rows), sql_num(j.tgt_rows),
            'true' if j.is_match else 'false', sql_str(j.status)]) for j in done)
        self.tgt.run('result', [
            'CREATE DATABASE IF NOT EXISTS `%s`' % self.result_db,
            'CREATE TABLE IF NOT EXISTS %s (\n'
            '  run_id STRING COMMENT "migration run id (yyyymmdd_hhmmss)",\n'
            '  migrated_at TIMESTAMP COMMENT "run start time",\n'
            '  src_db STRING, src_table STRING, tgt_db STRING, tgt_table STRING,\n'
            '  src_count BIGINT, tgt_count BIGINT,\n'
            '  is_match BOOLEAN COMMENT "src_count = tgt_count",\n'
            '  status STRING COMMENT "OK | MISMATCH | FAILED | UNSUPPORTED")\n'
            'STORED AS ORC' % fq,
            'INSERT INTO TABLE %s VALUES\n%s' % (fq, values)], force=True)
        self.log.info('result', '%d row(s) written to %s.%s (run_id=%s)'
                      % (len(done), self.result_db, self.result_table, run_id))

    # ---------------------------------------------------------------- drop (target cleanup)
    def _db_location(self, who, db):
        """Target database directory (never removed by the empty-parent cleanup). None if unknown."""
        try:
            got = require(split_marked(self.tgt.run(who, [marker('DB'), 'DESCRIBE DATABASE `%s`' % db],
                                                    retry=True, force=True)), 'DB')
        except CommandError:
            return None
        for line in got['DB']:
            for f in line.split('\t'):
                if re.match(r'^[A-Za-z][\w+.-]*://|^/', f.strip()):
                    return strip_uri(f.strip()).rstrip('/')
        return None

    def _remove_empty_parents(self, who, path, keep=None):
        """After a table directory is deleted, remove parent directories that became empty
        (e.g. /data/<db> created by distcp). Stops at the first non-empty directory (rmdir only
        removes empty ones), at top-level dirs (/data), warehouse roots, *.db dirs and \`keep\`
        (the database's own location)."""
        removed, cur = [], path.rstrip('/')
        while True:
            parent = cur.rsplit('/', 1)[0]
            comps = [x for x in parent.split('/') if x]
            if len(comps) < 2 or comps[-1].lower().endswith('.db') or (keep and parent == keep) or \
                    parent.lower() in ('/user/hive/warehouse', '/apps/hive/warehouse', '/warehouse'):
                break
            rc, out, err = self.tgt_fs.run(who, ['fs', '-rmdir', self.tgt_uri + parent], force=True)
            if rc != 0:   # not empty (or already gone): stop
                break
            removed.append(parent)
            cur = parent
        if removed:
            self.log.info(who, 'removed empty parent dir(s): %s' % ', '.join(removed))
        return removed

    @staticmethod
    def _check_drop_path(path):
        """Refuse to delete paths that look like a shared root rather than one table's directory."""
        parts = [x for x in path.split('/') if x]
        if len(parts) < 2 or parts[-1].lower().endswith('.db') or \
                path.rstrip('/').lower() in ('/user/hive/warehouse', '/apps/hive/warehouse', '/warehouse'):
            raise CommandError('refusing to delete files under %r (looks like a shared directory); '
                               'table not dropped, clean up by hand' % path)

    def drop_table(self, job):
        """Drop one table/view on the TARGET cluster (never touches the source).
        external tables: DROP keeps the files, so the table directory is removed as well."""
        t0 = time.time()
        fq = '`%s`.`%s`' % (job.tgt_db, job.table)
        if STOP_NEW.is_set():
            job.fail('STOPPED', 'not started (stop requested)')
            self.log.info(job.src, '=> STOPPED not started')
            return
        try:
            try:
                got = require(split_marked(self.tgt.run(
                    job.src, [marker('DESC'), 'DESCRIBE FORMATTED %s' % fq], retry=True)), 'DESC')
            except CommandError as e:
                if re.search(r'Table not found|does not exist', str(e), re.I):
                    job.status = 'DROPPED'
                    job.messages.append('not on target (nothing to drop)')
                    return
                raise
            meta = parse_describe(got['DESC'])
            ttype = meta['table_type']
            job.kind = {'VIRTUAL_VIEW': 'VIEW', 'EXTERNAL_TABLE': 'EXTERNAL',
                        'MANAGED_TABLE': 'MANAGED'}.get(ttype, ttype)
            if meta['params'].get('transactional', '').lower() == 'true':
                job.kind = 'ACID'
            path = strip_uri(meta['location']) if job.kind == 'EXTERNAL' else None
            if path:
                self._check_drop_path(path)
            what = '%s%s' % (job.kind, ' + files %s' % path if path else '')
            if self.dry:
                job.status = 'DRY-RUN'
                job.messages.append('would drop %s' % what)
                return
            self.log.info(job.src, 'dropping %s on target (%s)' % (job.tgt, what))
            self.tgt.run(job.src, ['DROP VIEW IF EXISTS %s' % fq if job.kind == 'VIEW'
                                   else 'DROP TABLE IF EXISTS %s PURGE' % fq])
            if path:
                rc, out, err = self.tgt_fs.run(job.src, ['fs', '-rm', '-r', '-f', '-skipTrash', self.tgt_uri + path])
                if rc != 0:
                    raise CommandError('table dropped but files not removed: %s: %s' % (path, _error_text(out, err)))
                gone = self._remove_empty_parents(job.src, path, self._db_location(job.src, job.tgt_db))
                if gone:
                    what += ' + empty dir(s) %s' % ', '.join(gone)
            job.status = 'DROPPED'
            job.messages.append('dropped on target: %s' % what)
        except CommandError as e:
            job.fail('STOPPED' if STOP_NOW.is_set() else 'FAILED', str(e))
        finally:
            job.seconds = time.time() - t0
            self.log.info(job.src, '=> %s %s' % (job.status, '; '.join(job.messages)))

    def drop_targets(self, rows, drop_dbs=()):
        if any(t == '*' for _, t in rows):
            raise CommandError('drop needs explicit table names (no *)')
        jobs = [Job(db, t) for db, t in rows]
        self.jobs = jobs
        self.log.info('plan', '%d object(s) to drop on the TARGET cluster (parallel=%d)%s' % (
            len(jobs), self.parallel, ', then database(s): %s' % ', '.join(drop_dbs) if drop_dbs else ''))
        with ThreadPoolExecutor(self.parallel) as ex:
            list(ex.map(self.drop_table, jobs))
        for db in drop_dbs:   # only if empty (no CASCADE): tables not listed here are never dropped
            if STOP_NEW.is_set():
                break
            if self.dry:
                self.log.info('db:' + db, 'DRY-RUN would drop database %s if it is empty' % db)
                continue
            try:
                self.tgt.run('db:' + db, ['DROP DATABASE IF EXISTS `%s`' % db])
                self.log.info('db:' + db, 'database %s dropped on target' % db)
            except CommandError as e:
                self.log.info('db:' + db, 'database %s NOT dropped (still has tables?): %s' % (db, e))
        return jobs

    # ---------------------------------------------------------------- orchestration
    def _control_parallel(self, current):
        """Live change of the number of workers: the control file (--control, JSON {"parallel": N}) is
        re-read when its mtime changes. Raising it hands out more work at once; lowering it lets running
        tables finish and simply hands out less (nothing is interrupted)."""
        path = self.control_path
        if not path:
            return current
        try:
            mtime = os.path.getmtime(path)
            if mtime == self._control_mtime:
                return current
            self._control_mtime = mtime
            with open(path, encoding='utf-8') as f:
                n = int(json.load(f).get('parallel'))
        except (OSError, ValueError, TypeError, AttributeError):
            return current   # missing / half-written / bad value: keep the current setting
        n = max(1, min(n, MAX_PARALLEL))
        if n != current:
            self.log.info('plan', 'parallel changed: %d -> %d' % (current, n))
            self.parallel = n
        return n

    def _run_objects(self, tables, views, existing):
        """Tables and views in ONE worker pool of `parallel` workers. Work is handed out only when a
        worker is free (no pre-filled queue), and a view whose referenced objects in this run are
        finished goes before the remaining tables - otherwise it would wait behind every table.
        Views that fail get one more try at the end."""
        in_run = {j.src.lower() for j in tables + views}

        def deps(v):
            refs = {('%s.%s' % (a, b)).lower() for a, b in re.findall(r'`([^`]+)`\.`([^`]+)`', v.ddl or '')}
            return {r for r in refs if r in in_run and r != v.src.lower()}
        vdeps = {v.src: deps(v) for v in views}
        workers = max(1, min(self.parallel, MAX_PARALLEL))
        self.log.info('plan', 'workers: %d at a time (%d tables + %d views)' % (workers, len(tables), len(views)))
        finished, pending, failed_views = set(), list(views), []
        queue, futs = deque(tables), {}
        ex = ThreadPoolExecutor(MAX_PARALLEL)   # real limit is `workers` (only that many are handed out)
        try:
            while futs or pending or queue:
                workers = self._control_parallel(workers)
                while len(futs) < workers:
                    ready = next((v for v in pending if vdeps[v.src] <= finished), None)
                    if ready is None and not futs and not queue and pending:
                        ready = pending[0]   # dependency we can't satisfy (cycle?): just try it
                    if ready is not None:
                        pending.remove(ready)
                        if STOP_NEW.is_set():
                            ready.status, ready.messages = 'STOPPED', ['not started (stop requested)']
                            self.log.info(ready.src, '=> STOPPED not started')
                            finished.add(ready.src.lower())
                            continue
                        futs[ex.submit(self.migrate_view, ready, existing)] = ready
                    elif queue:
                        j = queue.popleft()
                        futs[ex.submit(self.migrate_table, j, existing)] = j
                    else:
                        break
                if not futs:
                    continue
                # short timeout: a changed `parallel` in the control file is picked up within seconds
                done, _ = wait(list(futs), timeout=CONTROL_POLL, return_when=FIRST_COMPLETED)
                for f in done:
                    j = futs.pop(f)
                    finished.add(j.src.lower())
                    if j.kind == 'VIEW' and f.result() is False:
                        failed_views.append(j)
        finally:
            ex.shutdown(wait=True)
        for v in failed_views:   # e.g. referenced an object that was created later in the run
            if not STOP_NEW.is_set() and self.migrate_view(v, existing):
                continue
            if STOP_NEW.is_set() and v.status in ('PENDING', 'FAILED'):
                v.status, v.messages = 'STOPPED', ['not started (stop requested)']
            self.log.info(v.src, '=> %s %s' % (v.status, '; '.join(v.messages)))

    def run(self, rows):
        self.log.info('plan', 'expanding %d source row(s)' % len(rows))
        jobs = self.expand_targets(rows)
        jobs = [j for j in jobs if j.src_db not in (self.stage_db, self.result_db)]
        self.jobs = jobs   # the periodic report writer reads this
        # target first: objects settled by if_exists (e.g. resume: already OK) need no source metadata read
        existing = self.existing_target_tables(jobs)
        todo = self.skip_done(jobs, existing)
        self.log.info('plan', '%d object(s) to migrate, reading source metadata (parallel=%d)'
                      % (len(todo), self.parallel))
        self.load_meta_batched(todo)

        tables = [j for j in todo if j.kind in ('EXTERNAL', 'MANAGED', 'ACID')]
        views = [j for j in todo if j.kind == 'VIEW']
        for j in todo:
            if j.kind == 'UNSUPPORTED' and j.status == 'PENDING':
                j.fail('UNSUPPORTED', 'unknown table type')
        self.precheck_paths(tables, existing)   # still plan phase (the web UI switches phase on the next line)
        skipped = sum(j.status == 'SKIPPED' for j in jobs)
        self.log.info('plan', 'tables=%d (external=%d managed=%d acid=%d) views=%d unsupported/failed=%d skipped=%d' % (
            len(tables), sum(j.kind == 'EXTERNAL' for j in tables), sum(j.kind == 'MANAGED' for j in tables),
            sum(j.kind == 'ACID' for j in tables), len(views), len(jobs) - len(tables) - len(views) - skipped, skipped))

        self._run_objects(tables, views, existing)
        if STOP_NOW.is_set():
            self.kill_yarn_jobs()
        return jobs


# --------------------------------------------------------------------------- I/O
def read_sources(path, allow_empty=False, with_target=False):
    """sources CSV: header 'db,table[,tgt_db]'; table '*' = whole database; '#' lines are comments.
    tgt_db (optional): rename the database on the target; empty = same name.
    with_target=False returns (db, table) pairs, True returns (db, table, tgt_db)."""
    rows = []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.reader(l for l in f if l.strip() and not l.lstrip().startswith('#'))
        header = [h.strip().lower() for h in next(reader, [])]
        if header[:2] != ['db', 'table']:
            raise SystemExit('sources CSV header must be: db,table (got %s)' % ','.join(header))
        for n, rec in enumerate(reader, start=2):
            rec = [c.strip().lower() for c in rec] + ['', '', '']
            if not rec[0] and not rec[1]:
                continue
            if not rec[0] or not rec[1]:
                raise SystemExit('%s: row %d needs both db and table (use * for a whole db): %s'
                                 % (path, n, ','.join(rec[:2])))
            if rec[2] and not re.match(r'^[a-z0-9_]+$', rec[2]):
                raise SystemExit('%s: row %d: invalid target db name %r' % (path, n, rec[2]))
            rows.append((rec[0], rec[1], rec[2] or rec[0]) if with_target else (rec[0], rec[1]))
    if not rows and not allow_empty:
        raise SystemExit('%s: no tables listed' % path)
    return rows


def write_report(path, jobs):
    tmp = path + '.tmp'   # write + rename: a reader (or a crash) never sees a half-written file
    with open(tmp, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['src', 'tgt', 'kind', 'partitioned', 'status', 'src_rows', 'tgt_rows',
                    'tgt_count_star', 'src_checksum', 'tgt_checksum', 'src_partitions',
                    'tgt_partitions', 'seconds', 'message', 'flags',
                    'validated_by', 'src_files', 'src_bytes', 'src_ddl_time'])
        for j in jobs:
            w.writerow([j.src, j.tgt, j.kind, j.partitioned, j.status, j.src_rows, j.tgt_rows,
                        j.tgt_stats_rows, j.src_sum, j.tgt_sum,
                        len(j.src_parts) if j.src_parts is not None else '',
                        len(j.tgt_parts) if j.tgt_parts is not None else '',
                        '%.1f' % j.seconds, '; '.join(j.messages), ';'.join(sorted(j.flags)),
                        # source state when migrated (to find tables changed since, before the cut-over)
                        j.validated_by or '', '' if j.src_files in (None, -1) else j.src_files,
                        '' if j.src_bytes is None else j.src_bytes,
                        (j.meta or {}).get('params', {}).get('transient_lastDdlTime', '')])
    os.replace(tmp, path)


def start_report_flusher(path, migrator, every=2):
    """Rewrite the report with the finished objects every few seconds, so a run that is killed
    (server restart, kill -9, reboot) still leaves a record of what it already did on the target."""
    stop = threading.Event()

    def loop():
        while not stop.wait(every):
            done = [j for j in getattr(migrator, 'jobs', []) if j.status != 'PENDING']
            if done:
                try:
                    write_report(path, done)
                except OSError:
                    pass
    threading.Thread(target=loop, daemon=True).start()
    return stop


def print_summary(jobs):
    print('\n%-34s %-34s %-9s %-10s %8s %8s  %s' % ('SOURCE', 'TARGET', 'KIND', 'STATUS', 'SRC_ROWS', 'TGT_ROWS', 'MESSAGE'))
    for j in jobs:
        print('%-34s %-34s %-9s %-10s %8s %8s  %s' % (
            j.src, j.tgt, j.kind or '-', j.status, '' if j.src_rows is None else j.src_rows,
            '' if j.tgt_rows is None else j.tgt_rows, '; '.join(j.messages)[:120]))
    counts = {}
    for j in jobs:
        counts[j.status] = counts.get(j.status, 0) + 1
    print('\n' + '  '.join('%s=%d' % kv for kv in sorted(counts.items())))


def main():
    ap = argparse.ArgumentParser(description='Hive 2.x cluster-to-cluster migration')
    ap.add_argument('-c', '--config', required=True, help='INI config (clusters, distcp, options)')
    ap.add_argument('-s', '--sources', required=True, help='CSV: db,table (table * = whole db)')
    ap.add_argument('--dry-run', action='store_true', help='read metadata and print the plan only')
    ap.add_argument('--parallel', type=int, help='override [run] parallel (tables/views at a time)')
    ap.add_argument('--small-parallel', type=int, help=argparse.SUPPRESS)   # removed; accepted and ignored
    ap.add_argument('--control', help='JSON file {"parallel": N}; re-read while running, so the number of '
                                      'tables/views at a time can be changed without restarting')
    ap.add_argument('--if-exists', choices=['resume', 'replace', 'skip', 'fail'],
                    help='override [run] if_exists (default resume: skip what the last run of the same '
                         'source -> target finished OK, drop and re-create anything else on the target)')
    ap.add_argument('--validate', choices=list(VALIDATE_MODES),
                    help='override [run] validate (default auto: by source size checksum = row count + '
                         'sum(hash(*)), count, or files = file count/bytes without a source scan)')
    ap.add_argument('--repair-source', choices=['true', 'false'], help='override [run] repair_source')
    ap.add_argument('--drop-target', action='store_true',
                    help='instead of migrating: DROP the listed tables on the TARGET cluster '
                         '(external: table directory removed too). Use with --dry-run to preview')
    ap.add_argument('--drop-databases', default='',
                    help='with --drop-target: also DROP these databases (comma separated) if empty afterwards')
    ap.add_argument('--report-dir', default='reports')
    a = ap.parse_args()

    cfg = load_config(a.config)
    for key, val in (('parallel', a.parallel),
                     ('if_exists', a.if_exists), ('validate', a.validate),
                     ('repair_source', a.repair_source)):
        if val is not None:
            cfg['run'][key] = str(val)

    os.makedirs(a.report_dir, exist_ok=True)
    started = datetime.datetime.now()
    run_id = started.strftime('%Y%m%d_%H%M%S')
    base = os.path.join(a.report_dir, 'migration_%s%s' % (run_id, '_dryrun' if a.dry_run else ''))
    log = Log(base + '.log')
    log.info('main', 'run_id=%s config=%s sources=%s dry_run=%s %s' % (
        run_id, a.config, a.sources, a.dry_run, 'mode=DROP-TARGET' if a.drop_target else
        'if_exists=%s stats=%s validate=%s' % (cfg['run'].get('if_exists'), cfg['run'].get('stats'),
                                               cfg['run'].get('validate'))))

    def on_signal(signum, frame):   # only set flags here; logging happens on another thread
        hard = signum != getattr(signal, 'SIGUSR1', None)
        if hard and STOP_NOW.is_set():
            return
        STOP_NEW.set()
        if hard:
            STOP_NOW.set()
        threading.Thread(target=log.info, args=('main', 'STOP requested: %s' % (
            'aborting running tables, rolling back' if hard else
            'finishing running tables, not starting new ones'),)).start()
    for sig in ('SIGTERM', 'SIGINT', 'SIGUSR1'):
        if hasattr(signal, sig):
            signal.signal(getattr(signal, sig), on_signal)

    # dropping only (empty) databases needs no table rows
    rows = read_sources(a.sources, allow_empty=bool(a.drop_target and a.drop_databases), with_target=True)
    if a.drop_target:   # drop: the db column is the TARGET db
        renamed = sorted(set('%s->%s' % (d, t) for d, _, t in rows if t != d))
        if renamed:   # a migration list fed to --drop-target would drop the tables under the source names
            raise SystemExit('--drop-target: the db column is the target db; the tgt_db column is not allowed '
                             '(found %s). List the target names in the db column.' % ', '.join(renamed[:5]))
        rows = [(d, t) for d, t, _ in rows]
    try:
        migrator = Migrator(cfg, log, a.dry_run, run_id, a.report_dir)
        migrator.control_path = a.control
        flusher = start_report_flusher(base + '.csv', migrator)
        jobs = migrator.drop_targets(rows, [d for d in a.drop_databases.split(',') if d]) \
            if a.drop_target else migrator.run(rows)
    except CommandError as e:
        log.info('main', 'ABORTED: %s' % e)
        sys.exit(2)
    flusher.set()
    write_report(base + '.csv', jobs)
    try:
        migrator.write_result_table(run_id, started, jobs)
    except CommandError as e:   # the migration itself is done; the local report still exists
        log.info('result', 'FAILED to write result table: %s' % e)
    print_summary(jobs)
    print('\nreport: %s.csv\nlog:    %s.log' % (base, base))
    bad = [j for j in jobs if j.status not in ('OK', 'SKIPPED', 'DRY-RUN', 'DROPPED')]
    sys.exit(1 if bad else 0)


if __name__ == '__main__':
    main()
