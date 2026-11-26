"""Migration history from the result CSVs (reports/migration_*.csv, dry runs excluded).

Shared by the CLI (if_exists=resume: which tables are already done) and the web UI (state shown per table),
so both read the same rules.

    target   : target 'db.table' -> state of the table that is on the target now
               OK / MISMATCH (both sides exist, validation differed). Dropped (DROPPED) and rolled-back
               failures are removed; a failure before the target table was created leaves the target state
               as it was (the table migrated before is still there).
               Exceptions kept as FAILED, because a table is left on the target: rollback failed,
               or a big table kept for the next run (resumable).
    attempts : source 'db.table' -> last attempt (failures included)

Flags (CSV column `flags`, ';'-separated): rolled_back, rollback_failed, resumable.
CSVs written before the column existed are read from the message text.
"""
import csv
import glob
import os
import threading

FLAGS = ('rolled_back', 'rollback_failed', 'resumable')


def row_flags(r):
    """Flags of one result CSV row (from the `flags` column, or the message of an older CSV)."""
    if 'flags' in r:
        return {f for f in (r.get('flags') or '').split(';') if f}
    msg, out = r.get('message') or '', set()
    if 'ROLLBACK FAILED' in msg:
        out.add('rollback_failed')
    elif 'rolled back' in msg:
        out.add('rolled_back')
    return out


class History:
    def __init__(self, report_dir):
        self.report_dir = report_dir
        self.lock = threading.Lock()
        self.seen = {}      # path -> mtime
        self.target, self.attempts, self.tgt2src = {}, {}, {}

    @staticmethod
    def _apply(target, attempts, tgt2src, r, run_id):
        src, tgt, st, msg = r['src'], (r.get('tgt') or r['src']), r['status'], r.get('message', '')
        flags = row_flags(r)
        row = {'src': src, 'tgt': tgt, 'status': st, 'kind': r['kind'], 'src_rows': r['src_rows'],
               'tgt_rows': r['tgt_rows'], 'message': msg, 'run_id': run_id, 'flags': sorted(flags)}
        not_started = st == 'STOPPED' and msg.startswith('not started')
        if st == 'DROPPED':   # dropping is not a migration attempt: the source goes back to 'not migrated'
            s0 = tgt2src.pop(tgt, None)
            if s0 and attempts.get(s0, {}).get('tgt') == tgt:
                attempts.pop(s0, None)
        # last attempt (a skip or a stop before starting doesn't hide the previous attempt)
        elif not ((st == 'SKIPPED' or not_started) and src in attempts):
            attempts[src] = row
            tgt2src[tgt] = src
        # target state
        if st in ('OK', 'MISMATCH'):
            target[tgt] = row
        elif st == 'DROPPED' or (st in ('FAILED', 'STOPPED') and 'rolled_back' in flags):
            target.pop(tgt, None)                      # gone from the target
        elif flags & {'rollback_failed', 'resumable'}:
            target[tgt] = row                          # a table is left on the target
        elif st == 'SKIPPED' and tgt not in target:
            target[tgt] = row                          # was on the target already
        # other failures / stops / unsupported: before the target table existed -> target state unchanged

    def load(self):
        files = sorted(f for f in glob.glob(os.path.join(self.report_dir, 'migration_*.csv'))
                       if '_dryrun' not in f)
        with self.lock:
            changed = [f for f in files if self.seen.get(f) != os.path.getmtime(f)]
            if not changed:
                return
            if any(f in self.seen for f in changed) or (self.seen and min(changed) < max(self.seen)):
                self.seen, self.target, self.attempts, self.tgt2src = {}, {}, {}, {}   # order changed: start over
                changed = files
            for f in changed:   # file names are in run_id order, so a later file is newer
                run_id = os.path.basename(f)[len('migration_'):-len('.csv')]
                with open(f, newline='', encoding='utf-8') as fh:
                    for r in csv.DictReader(fh):
                        self._apply(self.target, self.attempts, self.tgt2src, r, run_id)
                self.seen[f] = os.path.getmtime(f)

    def all(self):
        self.load()
        return {'target': self.target, 'attempts': self.attempts}

    def done_ok(self, src, tgt):
        """if_exists=resume: the run_id of the OK migration of src -> tgt that is the target's current state,
        else None. Uses what was loaded last (call load() first); the caller also checks that the table is
        really on the target."""
        row = self.target.get(tgt)
        if row and row['status'] == 'OK' and row['src'] == src:
            return row['run_id']
        return None

    def resumable(self, src, tgt):
        """if_exists=resume: the run_id that kept a partly copied src -> tgt table for the next run, else None."""
        row = self.target.get(tgt)
        if row and row['src'] == src and 'resumable' in row['flags']:
            return row['run_id']
        return None
