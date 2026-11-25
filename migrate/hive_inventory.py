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
import sys
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from hive_migrate import (Beeline, CommandError, Hadoop, Log, is_default_managed_path,  # noqa: E402
                          load_config, marker, parse_describe, read_sources, split_marked, strip_uri)

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
           'dir_count', 'stats_rows', 'stats_accurate', 'serde', 'owner', 'create_time', 'error']


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
                serde=m.get('serde', ''), owner=m.get('owner', ''), create_time=m.get('createtime', ''))
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
        return records


# ---------------------------------------------------------------- reporting
def summarize(records):
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

    for title, sel in (
            ('ACID tables (go through staging, not file copy)', [r for r in records if r['kind'] == 'ACID']),
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
        out += ['', '# ' + '=' * 70,
                '# %s : %d object(s), %s' % (db, len(rows), human(sum(size(r) for r in rows))),
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
    ap.add_argument('-o', '--sources-out', help='where to write the generated sources CSV '
                    '(default: next to the inventory report)')
    ap.add_argument('--batch-size', type=int, help='override [inventory] batch_size')
    ap.add_argument('--parallel', type=int, help='override [run] parallel')
    ap.add_argument('--report-dir', default='reports')
    a = ap.parse_args()

    if a.from_inventory:
        out = a.sources_out or os.path.splitext(a.from_inventory)[0] + '_sources.csv'
        n_ok, n_bad = write_sources(load_inventory_csv(a.from_inventory), out, a.from_inventory)
        print('sources: %s  (%d to migrate, %d commented out)' % (out, n_ok, n_bad))
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
    summary = summarize(records)
    with open(base + '_summary.txt', 'w', encoding='utf-8') as f:
        f.write(summary + '\n')
    sources_out = a.sources_out or base + '_sources.csv'
    n_ok, n_bad = write_sources(records, sources_out, os.path.basename(base) + '.csv')
    print('\n' + summary)
    print('\nelapsed: %s' % str(datetime.datetime.now() - started).split('.')[0])
    print('inventory: %s.csv\nsummary:   %s_summary.txt\nsources:   %s  (%d to migrate, %d commented out)'
          '\nlog:       %s.log' % (base, base, sources_out, n_ok, n_bad, base))


if __name__ == '__main__':
    main()
