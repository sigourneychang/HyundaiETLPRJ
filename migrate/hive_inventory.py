#!/usr/bin/env python3
"""Inventory of Hive tables on the SOURCE cluster (read-only).

For every table it records type (EXTERNAL / MANAGED / ACID / VIEW), storage
format, partitioning, HDFS location, size and file count, and metastore stats,
so the migration can be planned (big tables, ACID tables, partition-heavy
tables, unsupported tables) before anything is copied.

Nothing is written to either cluster. Work is batched for tens of thousands of
tables: one beeline session per batch of DESCRIBEs, one `hadoop fs -count` per
batch of paths. If a statement in a batch fails (e.g. a table with a broken
SerDe), that batch is retried table by table and the bad table is reported.

It also writes a ready-to-use sources CSV for hive_migrate.py (grouped by db and
kind, with sizes in comments; ERROR/UNSUPPORTED tables commented out with the
reason). Because the source is frozen, that file can be regenerated later from
the inventory CSV alone, without touching the cluster (--from-inventory).

Usage:
  python3 migrate/hive_inventory.py -c conf/local.ini --all
  python3 migrate/hive_inventory.py -c conf/local.ini -s conf/sources.csv
  python3 migrate/hive_inventory.py --from-inventory reports/inventory_X.csv -o conf/sources.csv
"""
import argparse
import configparser
import csv
import datetime
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from hive_migrate import (DEFAULTS, Beeline, CommandError, Hadoop, Log, is_default_managed_path,  # noqa: E402
                          load_config, marker, parse_describe, parse_size, read_sources, split_marked, strip_uri)
from history import History  # noqa: E402

SKIP_DBS = {'sys', 'information_schema'}
FORMATS = {
    'OrcInputFormat': 'ORC', 'MapredParquetInputFormat': 'PARQUET', 'TextInputFormat': 'TEXT',
    'AvroContainerInputFormat': 'AVRO', 'SequenceFileInputFormat': 'SEQUENCE',
    'RCFileInputFormat': 'RCFILE',
}
SIZE_BUCKETS = [(1 << 30, '< 1 GB'), (100 << 30, '1-100 GB'), (1 << 40, '100 GB-1 TB'),
                (10 << 40, '1-10 TB'), (None, '>= 10 TB')]
COLUMNS = ['db', 'table', 'kind', 'format', 'partitioned', 'partition_cols', 'partition_count',
           'num_buckets', 'location', 'location_type', 'size_bytes', 'size_gb', 'file_count',
           'dir_count', 'stats_rows', 'stats_accurate', 'serde', 'owner', 'create_time',
           'ddl_time', 'dir_mtime', 'last_change', 'error']
RECENT_DAYS = 30   # summary: tables changed within this many days (the source should be frozen)


def epoch_text(v):
    """transient_lastDdlTime (epoch seconds) -> 'YYYY-MM-DD HH:MM' (local time), '' if not a number."""
    try:
        return datetime.datetime.fromtimestamp(int(v)).strftime('%Y-%m-%d %H:%M')
    except (TypeError, ValueError, OverflowError, OSError):
        return ''


def chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def human(n):
    for unit in ('B', 'KB', 'MB', 'GB', 'TB', 'PB'):
        if n < 1024 or unit == 'PB':
            return '%.1f %s' % (n, unit) if unit != 'B' else '%d B' % n
        n /= 1024.0


class Inventory:
    def __init__(self, cfg, log):
        r, inv = cfg['run'], cfg['inventory']   # defaults filled in by load_config
        self.log = log
        self.src = Beeline('src', cfg['src']['beeline'], r.getint('call_timeout', 900),
                           r.getint('retries', 2), log)
        self.fs = Hadoop(cfg['src']['hadoop'], r.getint('call_timeout', 900), log)
        self.parallel = r.getint('parallel', 2)
        self.batch = int(inv.get('batch_size', 200))
        self.path_batch = int(inv.get('path_batch_size', 100))
        self.skip_dbs = SKIP_DBS | {cfg['staging']['db'], cfg['result']['db']}

    # ------------------------------------------------------------ table list
    def list_tables(self, rows):
        """rows=None -> every database; else (db, table) rows from a sources CSV."""
        dbs = [d.strip() for d in self.src.run('inv', ['SHOW DATABASES'], retry=True) if d.strip()]
        if rows is None:
            whole, explicit = [d for d in dbs if d not in self.skip_dbs], []
        else:
            missing = sorted({db for db, _ in rows} - set(dbs))
            if missing:
                raise CommandError('source database(s) not found: %s' % ', '.join(missing))
            whole = sorted({db for db, t in rows if t == '*'})
            explicit = [(db, t) for db, t in rows if t != '*']
        found = []
        for part in chunks(whole, self.batch):
            stmts = []
            for i, db in enumerate(part):
                stmts += [marker('D%d' % i), 'SHOW TABLES IN `%s`' % db]
            got = split_marked(self.src.run('inv', stmts, retry=True))
            for i, db in enumerate(part):
                found += [(db, t.strip()) for t in got.get('D%d' % i, []) if t.strip()]
        seen, out = set(), []
        for key in found + explicit:
            if key not in seen:
                seen.add(key)
                out.append(key)
        return out

    # ------------------------------------------------------------ metadata
    def _run_batch(self, tables, stmt_fn, tag):
        """Run one statement per table in one session; on failure fall back to one call per table.
        Returns {(db, table): lines or CommandError}."""
        stmts = []
        for i, (db, t) in enumerate(tables):
            stmts += [marker('T%d' % i), stmt_fn(db, t)]
        try:
            got = split_marked(self.src.run(tag, stmts, retry=True))
            return {k: got.get('T%d' % i, []) for i, k in enumerate(tables)}
        except CommandError as e:
            if len(tables) == 1:
                return {tables[0]: e}
            self.log.info(tag, 'batch of %d failed (%s) -> retrying one by one'
                          % (len(tables), str(e)[:120]))
            res = {}
            for k in tables:
                res.update(self._run_batch([k], stmt_fn, tag))
            return res

    def describe(self, tables):
        res = {}
        batches = list(chunks(tables, self.batch))
        with ThreadPoolExecutor(self.parallel) as ex:
            for n, r in enumerate(ex.map(lambda b: self._run_batch(
                    b, lambda d, t: 'DESCRIBE FORMATTED `%s`.`%s`' % (d, t), 'describe'), batches), 1):
                res.update(r)
                self.log.info('describe', 'batch %d/%d done (%d tables)' % (n, len(batches), len(res)))
        return res

    def partition_counts(self, tables):
        res = {}
        batches = list(chunks(tables, self.batch))
        with ThreadPoolExecutor(self.parallel) as ex:
            for r in ex.map(lambda b: self._run_batch(
                    b, lambda d, t: 'SHOW PARTITIONS `%s`.`%s`' % (d, t), 'partitions'), batches):
                for k, v in r.items():
                    res[k] = v if isinstance(v, CommandError) else sum(1 for l in v if l.strip())
        return res

    def sizes(self, paths):
        """{path: (dirs, files, bytes)} via `hadoop fs -count` on batches of paths."""
        res = {}
        batches = list(chunks(sorted(set(p for p in paths if p)), self.path_batch))

        def one(batch):
            rc, out, err = self.fs.run('size', ['fs', '-count'] + batch)
            got = {}
            for line in out.splitlines():
                parts = line.split(None, 3)
                if len(parts) == 4 and parts[0].isdigit():
                    got[strip_uri(parts[3].strip())] = (int(parts[0]), int(parts[1]), int(parts[2]))
            return got
        with ThreadPoolExecutor(self.parallel) as ex:
            for n, r in enumerate(ex.map(one, batches), 1):
                res.update(r)
                if n % 10 == 0 or n == len(batches):
                    self.log.info('size', 'path batch %d/%d done' % (n, len(batches)))
        return res

    def mtimes(self, paths):
        """{path: 'YYYY-MM-DD HH:MM'}: modification time of each table directory, NameNode metadata only.
        It changes when an entry directly under the directory is added, removed or replaced (files of an
        unpartitioned table, partition directories) - not when files are added inside an existing
        partition directory. `fs -stat %Y` gives epoch ms, so it is converted in the same time zone as
        ddl_time (`fs -ls` prints in the JVM's time zone, which may differ).
        Pass existing paths only: %n is the last path component, so lines are matched by position; a batch
        whose output doesn't line up is read again one path at a time."""
        res = {}
        batches = list(chunks(sorted(set(p.rstrip('/') for p in paths if p)), self.path_batch))

        def stat(batch):
            rc, out, err = self.fs.run('mtime', ['fs', '-stat', '%Y %n'] + batch)
            lines = [l.split(' ', 1) for l in out.split('\n') if re.match(r'^\d+ ', l)]
            if len(lines) != len(batch) or any(n.strip() != p.rsplit('/', 1)[-1] for (_, n), p in zip(lines, batch)):
                return None
            return {p: epoch_text(int(ms) // 1000) for (ms, _), p in zip(lines, batch)}

        def one(batch):
            got = stat(batch)
            if got is None:
                got = {}
                for p in (batch if len(batch) > 1 else []):
                    got.update(stat([p]) or {})
            return got
        with ThreadPoolExecutor(self.parallel) as ex:
            for r in ex.map(one, batches):
                res.update(r)
        return res

    # ------------------------------------------------------------ drift (before the cut-over)
    def drift(self, report_dir):
        """Tables that are OK on the target (result history) whose source changed since they were migrated:
        file count / bytes of the table directory and transient_lastDdlTime, compared with what the result
        CSV recorded at migration time. Source reads only: DESCRIBE and `hadoop fs -count`.
        Returns rows {src, tgt, kind, result, detail, run_id}; result = changed | gone | unknown | same."""
        h = History(report_dir)
        h.load()
        ok = sorted((r for r in h.target.values() if r['status'] == 'OK'), key=lambda r: (r['src'], r['tgt']))
        keys = sorted({tuple(r['src'].split('.', 1)) for r in ok})
        self.log.info('drift', '%d OK table(s) on the target from %s, %d source table(s) to check'
                      % (len(ok), report_dir, len(keys)))
        desc = self.describe(keys) if keys else {}
        now = {}
        for k in keys:
            d = desc.get(k)
            if isinstance(d, CommandError) or d is None:
                now[k] = {'error': str(d or 'no output')}
                continue
            m = parse_describe(d)
            now[k] = {'view': m['table_type'] == 'VIRTUAL_VIEW', 'location': strip_uri(m['location'] or ''),
                      'ddl_time': m['params'].get('transient_lastDdlTime', '')}
        sz = self.sizes([v['location'] for v in now.values() if v.get('location') and not v['view']])
        out = []
        for r in ok:
            k = tuple(r['src'].split('.', 1))
            cur = now[k]
            row = {'src': r['src'], 'tgt': r['tgt'], 'kind': r['kind'], 'run_id': r['run_id']}
            if 'error' in cur:
                gone = re.search(r'Table not found|does not exist', cur['error'], re.I)
                row.update(result='gone' if gone else 'unknown', detail=cur['error'][:200])
                out.append(row)
                continue
            diffs, known = [], False
            if r['src_ddl_time']:
                known = True
                if r['src_ddl_time'] != cur['ddl_time']:
                    diffs.append('ddl_time %s -> %s' % (r['src_ddl_time'], cur['ddl_time']))
            if not cur['view'] and r['src_files'] != '' and r['src_bytes'] != '':
                known = True
                s0 = sz.get(cur['location'].rstrip('/')) or sz.get(cur['location'])
                if s0 is None:
                    diffs.append('directory %s not found' % cur['location'])
                elif (str(s0[1]), str(s0[2])) != (r['src_files'], r['src_bytes']):
                    diffs.append('files %s -> %d, bytes %s -> %d' % (r['src_files'], s0[1], r['src_bytes'], s0[2]))
            if diffs:
                row.update(result='changed', detail='; '.join(diffs))
            elif known:
                row.update(result='same', detail='')
            else:
                row.update(result='unknown', detail='no source state recorded (result CSV older than the columns)')
            out.append(row)
        return out

    # ------------------------------------------------------------ assemble
    def run(self, rows):
        tables = self.list_tables(rows)
        self.log.info('inv', '%d table(s)/view(s) to inspect (batch=%d, parallel=%d)'
                      % (len(tables), self.batch, self.parallel))
        desc = self.describe(tables)

        records, metas = [], {}
        for key in tables:
            db, t = key
            rec = dict.fromkeys(COLUMNS, '')
            rec.update(db=db, table=t)
            d = desc.get(key)
            if isinstance(d, CommandError) or d is None:
                rec.update(kind='ERROR', error=str(d)[:300] if d else 'no output')
                records.append(rec)
                continue
            m = parse_describe(d)
            metas[key] = m
            p = m['params']
            if m['table_type'] == 'VIRTUAL_VIEW':
                kind = 'VIEW'
            elif p.get('storage_handler'):
                kind = 'UNSUPPORTED'
                rec['error'] = 'storage handler %s' % p['storage_handler']
            elif p.get('transactional', '').lower() == 'true':
                kind = 'ACID'
            elif m['table_type'] == 'EXTERNAL_TABLE':
                kind = 'EXTERNAL'
            elif m['table_type'] == 'MANAGED_TABLE':
                kind = 'MANAGED'
            else:
                kind, rec['error'] = 'UNSUPPORTED', 'table type %s' % m['table_type']
            ifmt = (m.get('input_format') or '').rsplit('.', 1)[-1]
            path = strip_uri(m['location']) if m['location'] else ''
            if kind == 'VIEW':
                loc_type = 'none'
            elif kind == 'EXTERNAL':
                loc_type = 'external'
            else:
                loc_type = 'default' if is_default_managed_path(path, db, t) else 'custom'
            nb = m.get('num_buckets') or ''
            rec.update(
                kind=kind, format='' if kind == 'VIEW' else FORMATS.get(ifmt, ifmt),
                partitioned=bool(m['partition_cols']),
                partition_cols=';'.join('%s %s' % c for c in m['partition_cols']),
                num_buckets='' if nb in ('', '-1') else nb, location=path, location_type=loc_type,
                stats_rows=p.get('numRows', ''),
                stats_accurate='"BASIC_STATS":"true"' in p.get('COLUMN_STATS_ACCURATE', ''),
                serde=m.get('serde', ''), owner=m.get('owner', ''), create_time=m.get('createtime', ''),
                ddl_time=epoch_text(p.get('transient_lastDdlTime')))
            records.append(rec)

        parted = [(r['db'], r['table']) for r in records if r['partitioned'] is True]
        if parted:
            self.log.info('inv', 'counting partitions of %d partitioned table(s)' % len(parted))
            pc = self.partition_counts(parted)
            for r in records:
                v = pc.get((r['db'], r['table']))
                if isinstance(v, CommandError):
                    r['error'] = ('SHOW PARTITIONS failed: %s' % v)[:300]
                elif v is not None:
                    r['partition_count'] = v

        paths = [r['location'] for r in records if r['location']]
        self.log.info('inv', 'measuring %d HDFS location(s)' % len(paths))
        sz = self.sizes(paths)
        for r in records:
            if not r['location']:
                continue
            s = sz.get(r['location'].rstrip('/')) or sz.get(r['location'])
            if s is None:
                r['error'] = (r['error'] + '; ' if r['error'] else '') + 'location not found on HDFS'
                continue
            r['dir_count'], r['file_count'], r['size_bytes'] = s
            r['size_gb'] = '%.3f' % (s[2] / float(1 << 30))

        self.log.info('inv', 'reading modification time of the HDFS location(s)')
        mt = self.mtimes([r['location'] for r in records if r['location'] and r['dir_count'] != ''])
        for r in records:
            r['dir_mtime'] = mt.get(r['location'].rstrip('/'), '') if r['location'] else ''
            r['last_change'] = max(r['ddl_time'], r['dir_mtime'])   # same text format -> max = latest
        return records


# ---------------------------------------------------------------- reporting
def summarize(records, recent_days=RECENT_DAYS, now=None):
    lines = []
    total = sum(r['size_bytes'] or 0 for r in records)
    lines.append('objects: %d   total size: %s' % (len(records), human(total)))

    def group(key):
        agg = {}
        for r in records:
            k = r[key] or '-'
            c, s = agg.get(k, (0, 0))
            agg[k] = (c + 1, s + (r['size_bytes'] or 0))
        return sorted(agg.items(), key=lambda kv: -kv[1][1])

    for title, key in (('by kind', 'kind'), ('by format', 'format')):
        lines.append('\n%s:' % title)
        for k, (c, s) in group(key):
            lines.append('  %-12s %8d tables  %12s' % (k, c, human(s)))

    lines.append('\nby size (tables):')
    for i, (limit, label) in enumerate(SIZE_BUCKETS):
        lo = SIZE_BUCKETS[i - 1][0] if i else 0
        sel = [r for r in records if r['size_bytes'] != '' and r['size_bytes'] >= lo
               and (limit is None or r['size_bytes'] < limit)]
        lines.append('  %-12s %8d tables  %12s' % (label, len(sel), human(sum(r['size_bytes'] for r in sel))))

    top = sorted((r for r in records if r['size_bytes'] != ''), key=lambda r: -r['size_bytes'])[:20]
    lines.append('\nlargest tables:')
    for r in top:
        lines.append('  %-50s %-9s %12s  files=%s parts=%s' % (
            '%s.%s' % (r['db'], r['table']), r['kind'], human(r['size_bytes']), r['file_count'],
            r['partition_count'] or '-'))

    many = sorted((r for r in records if isinstance(r['partition_count'], int)),
                  key=lambda r: -r['partition_count'])[:10]
    if many:
        lines.append('\nmost partitions:')
        for r in many:
            lines.append('  %-50s %8d partitions' % ('%s.%s' % (r['db'], r['table']), r['partition_count']))

    # the source is supposed to be frozen during the migration: anything changed lately is being written to
    since = ((now or datetime.datetime.now()) - datetime.timedelta(days=recent_days)).strftime('%Y-%m-%d %H:%M')
    recent = [r for r in records if r.get('last_change') and r['last_change'] >= since]
    lines.append('\nchanged in the last %d days (DDL time or directory mtime; source should be frozen): '
                 '%d table(s)' % (recent_days, len(recent)))
    by_db = {}
    for r in recent:
        n, newest = by_db.get(r['db'], (0, ''))
        by_db[r['db']] = (n + 1, max(newest, r['last_change']))
    for db, (n, newest) in sorted(by_db.items(), key=lambda kv: kv[1][1], reverse=True)[:30]:
        lines.append('  %-30s %6d table(s)  newest %s' % (db, n, newest))
    if len(by_db) > 30:
        lines.append('  ... %d more database(s) (see CSV, last_change)' % (len(by_db) - 30))

    acid = [r for r in records if r['kind'] == 'ACID']
    for title, sel in (
            ('ACID tables (source staging write needed, not file copy; total %s)'
             % human(sum(r['size_bytes'] or 0 for r in acid)), acid),
            ('managed tables with a custom location', [r for r in records if r['location_type'] == 'custom']),
            ('errors / unsupported', [r for r in records if r['error'] or r['kind'] in ('ERROR', 'UNSUPPORTED')])):
        lines.append('\n%s: %d' % (title, len(sel)))
        for r in sel[:20]:
            lines.append('  %-50s %s' % ('%s.%s' % (r['db'], r['table']), r['error'] or human(r['size_bytes'] or 0)))
        if len(sel) > 20:
            lines.append('  ... %d more (see CSV)' % (len(sel) - 20))
    return '\n'.join(lines)


KIND_ORDER = ['EXTERNAL', 'MANAGED', 'ACID', 'VIEW']
KIND_NOTE = {'ACID': 'copied via staging tables + INSERT, not file copy',
             'VIEW': 'recreated after the tables'}


def write_sources(records, path, origin):
    """sources CSV for hive_migrate.py, grouped by db and kind, sizes in comments."""
    def size(r):
        return r['size_bytes'] if isinstance(r['size_bytes'], int) else 0
    ok = [r for r in records if r['kind'] in KIND_ORDER]
    bad = [r for r in records if r['kind'] not in KIND_ORDER]
    out = ['# Generated from %s at %s' % (origin, datetime.datetime.now().strftime('%Y-%m-%d %H:%M')),
           '# %d table(s)/view(s), %s. Comment out (#) or delete lines to leave tables out.'
           % (len(ok), human(sum(size(r) for r in ok))),
           'db,table']
    for db in sorted({r['db'] for r in ok}):
        rows = [r for r in ok if r['db'] == db]
        last = max((r.get('last_change') or '' for r in rows), default='')
        out += ['', '# ' + '=' * 70,
                '# %s : %d object(s), %s%s' % (db, len(rows), human(sum(size(r) for r in rows)),
                                             ', last change %s' % last if last else ''),
                '# ' + '=' * 70]
        for kind in KIND_ORDER:
            sel = sorted((r for r in rows if r['kind'] == kind), key=lambda r: r['table'])
            if not sel:
                continue
            note = ' - %s' % KIND_NOTE[kind] if kind in KIND_NOTE else ''
            out.append('# --- %s : %d, %s%s' % (kind, len(sel), human(sum(size(r) for r in sel)), note))
            out += ['%s,%s' % (r['db'], r['table']) for r in sel]
    if bad:
        out += ['', '# ' + '=' * 70, '# NOT MIGRATABLE as-is (%d) - review before uncommenting' % len(bad),
                '# ' + '=' * 70]
        for r in sorted(bad, key=lambda r: (r['db'], r['table'])):
            out.append('# %s,%s    <- %s: %s' % (r['db'], r['table'], r['kind'], (r['error'] or '')[:150]))
    with open(path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(out) + '\n')
    return len(ok), len(bad)


WAVES = ('wave1_small', 'wave2_medium', 'wave3_large', 'wave4_views')


def wave_of(r, small_max, medium_max):
    """Wave of one inventory record (same size limits as validate=auto). Views go last: a view can only be
    created once the tables it reads are on the target. Unknown size counts as medium."""
    if r['kind'] == 'VIEW':
        return WAVES[3]
    b = r['size_bytes'] if isinstance(r['size_bytes'], int) else None
    if b is not None and b < small_max:
        return WAVES[0]
    if b is None or b < medium_max:
        return WAVES[1]
    return WAVES[2]


def write_waves(records, base, origin, small_max, medium_max, max_per_file):
    """Sources CSVs by size wave, at most max_per_file objects each: <base>_<wave>_NNN.csv, in the order
    to run them. Objects that can't be migrated go to <base>_not_migratable.csv (commented out).
    Returns [(path, objects)]."""
    ok = [r for r in records if r['kind'] in KIND_ORDER]
    out = []
    for wave in WAVES:
        sel = sorted((r for r in ok if wave_of(r, small_max, medium_max) == wave), key=lambda r: (r['db'], r['table']))
        for n, i in enumerate(range(0, len(sel), max_per_file), 1):
            path = '%s_%s_%03d.csv' % (base, wave, n)
            write_sources(sel[i:i + max_per_file], path, '%s (%s %d)' % (origin, wave, n))
            out.append((path, len(sel[i:i + max_per_file])))
    bad = [r for r in records if r['kind'] not in KIND_ORDER]
    if bad:
        path = '%s_not_migratable.csv' % base
        write_sources(bad, path, origin)
        out.append((path, 0))
    return out


def write_drift(rows, base):
    """<base>.csv: every checked table; <base>_sources.csv: the changed ones, to migrate again with
    --if-exists replace (db,table,tgt_db)."""
    with open(base + '.csv', 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=['src', 'tgt', 'kind', 'result', 'detail', 'run_id'])
        w.writeheader()
        w.writerows(rows)
    changed = [r for r in rows if r['result'] == 'changed']
    with open(base + '_sources.csv', 'w', newline='', encoding='utf-8') as f:
        f.write('# source tables changed since they were migrated (%s.csv)\n'
                '# migrate again with: --if-exists replace\n' % os.path.basename(base))
        w = csv.writer(f)
        w.writerow(['db', 'table', 'tgt_db'])
        for r in changed:
            db, t = r['src'].split('.', 1)
            w.writerow([db, t, r['tgt'].split('.', 1)[0]])
    return changed


def load_inventory_csv(path):
    records = []
    with open(path, newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            for k in ('size_bytes', 'file_count', 'dir_count', 'partition_count'):
                r[k] = int(r[k]) if (r.get(k) or '').isdigit() else ''
            records.append(r)
    return records


def main():
    ap = argparse.ArgumentParser(description='Read-only inventory of source Hive tables')
    ap.add_argument('-c', '--config', help='INI config (not needed with --from-inventory)')
    scope = ap.add_mutually_exclusive_group(required=True)
    scope.add_argument('--all', action='store_true', help='every database on the source cluster')
    scope.add_argument('-s', '--sources', help='CSV: db,table (table * = whole db)')
    scope.add_argument('--from-inventory', metavar='CSV',
                       help='only (re)generate a sources CSV from an existing inventory CSV')
    scope.add_argument('--drift', metavar='REPORT_DIR',
                       help='before the cut-over: list tables OK on the target (result CSVs in REPORT_DIR) '
                            'whose source changed since they were migrated')
    ap.add_argument('-o', '--sources-out', help='where to write the generated sources CSV '
                    '(default: next to the inventory report)')
    ap.add_argument('--batch-size', type=int, help='override [inventory] batch_size')
    ap.add_argument('--parallel', type=int, help='override [run] parallel')
    ap.add_argument('--recent-days', type=int, default=RECENT_DAYS,
                    help='summary: list tables changed within this many days (default %d)' % RECENT_DAYS)
    ap.add_argument('--split-by-size', action='store_true',
                    help='write the sources as size waves (small, medium, large, views; limits = [run] '
                         'checksum_max_bytes / count_max_bytes) instead of one file')
    ap.add_argument('--max-per-file', type=int, default=3000, help='with --split-by-size: objects per file')
    ap.add_argument('--report-dir', default='reports')
    a = ap.parse_args()

    run_cfg = load_config(a.config, extra_required={'src': ('hadoop',)})['run'] if a.config else DEFAULTS['run']
    try:
        small_max = parse_size(run_cfg.get('checksum_max_bytes') or DEFAULTS['run']['checksum_max_bytes'])
        medium_max = parse_size(run_cfg.get('count_max_bytes') or DEFAULTS['run']['count_max_bytes'])
    except ValueError as e:
        ap.error('[run] checksum_max_bytes / count_max_bytes: %s' % e)
    if a.max_per_file < 1:
        ap.error('--max-per-file must be at least 1')

    def sources(records, default_base, origin):
        if a.split_by_size:
            files = write_waves(records, os.path.splitext(a.sources_out)[0] if a.sources_out else default_base,
                                origin, small_max, medium_max, a.max_per_file)
            return '\n'.join('           %s  (%d)' % f for f in files).lstrip()
        out = a.sources_out or default_base + '.csv'
        n_ok, n_bad = write_sources(records, out, origin)
        return '%s  (%d to migrate, %d commented out)' % (out, n_ok, n_bad)

    if a.from_inventory:
        print('sources:   ' + sources(load_inventory_csv(a.from_inventory),
                                      os.path.splitext(a.from_inventory)[0] + '_sources', a.from_inventory))
        return
    if not a.config:
        ap.error('-c/--config is required unless --from-inventory is used')

    cfg = load_config(a.config, extra_required={'src': ('hadoop',)})
    if a.batch_size:
        cfg['inventory']['batch_size'] = str(a.batch_size)
    if a.parallel:
        cfg['run']['parallel'] = str(a.parallel)

    os.makedirs(a.report_dir, exist_ok=True)
    stamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    if a.drift:
        base = os.path.join(a.report_dir, 'drift_%s' % stamp)
        log = Log(base + '.log')
        try:
            rows = Inventory(cfg, log).drift(a.drift)
        except CommandError as e:
            log.info('main', 'ABORTED: %s' % e)
            sys.exit(2)
        changed = write_drift(rows, base)
        n = {}
        for r in rows:
            n[r['result']] = n.get(r['result'], 0) + 1
        for r in rows:
            if r['result'] != 'same':
                print('  %-8s %-45s %s' % (r['result'], r['src'] + (' -> ' + r['tgt'] if r['tgt'] != r['src'] else ''),
                                          r['detail'][:120]))
        print('\nchecked %d: %s' % (len(rows), '  '.join('%s=%d' % kv for kv in sorted(n.items()))))
        print('drift:   %s.csv\nsources: %s_sources.csv  (%d changed: migrate again with --if-exists replace)'
              '\nlog:     %s.log' % (base, base, len(changed), base))
        sys.exit(1 if changed or n.get('gone') else 0)
    base = os.path.join(a.report_dir, 'inventory_%s' % stamp)
    log = Log(base + '.log')
    started = datetime.datetime.now()
    try:
        records = Inventory(cfg, log).run(None if a.all else read_sources(a.sources))
    except CommandError as e:
        log.info('main', 'ABORTED: %s' % e)
        sys.exit(2)
    with open(base + '.csv', 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        w.writerows(records)
    summary = summarize(records, a.recent_days)
    with open(base + '_summary.txt', 'w', encoding='utf-8') as f:
        f.write(summary + '\n')
    src_text = sources(records, base + '_sources', os.path.basename(base) + '.csv')
    print('\n' + summary)
    print('\nelapsed: %s' % str(datetime.datetime.now() - started).split('.')[0])
    print('inventory: %s.csv\nsummary:   %s_summary.txt\nsources:   %s\nlog:       %s.log'
          % (base, base, src_text, base))


if __name__ == '__main__':
    main()
