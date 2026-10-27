"""Privacy-minimal activity ledger. All times are service observations."""
import hashlib
import hmac
import os
import sqlite3
import time
import math
from pathlib import Path
from urllib.parse import urlsplit
from .policy import Policy
from .control import fence_stamp

GAP_REASONS = {'unspecified', 'unattributed_command', 'unattributed_completion',
               'unacknowledged_notification', 'observer_disconnect',
               'observer_error', 'observation_clock_gap', 'browser_epoch_changed'}
GAP_METHODS = {'Runtime.evaluate', 'Runtime.callFunctionOn', 'Runtime.getProperties',
               'Security.setIgnoreCertificateErrors', 'Network.getCookies',
               'Target.sendMessageToTarget', 'Target.activateTarget',
               'Input.dispatchKeyEvent', 'Page.navigate', 'DOM.getDocument',
               'Storage.clearDataForOrigin', 'Emulation.setFocusEmulationEnabled'}


def page_target(target):
    """Browser/extension UI is never an admitted website target."""
    return target.get('type') == 'page' and urlsplit(target.get('url','')).scheme.lower() not in {
        'chrome', 'chrome-extension', 'devtools', 'chrome-untrusted', 'chrome-search'}


class Store:
    def __init__(self, state_dir, clock=time.time, policy=None):
        self.policy = policy if policy is not None else Policy()
        self.path = Path(state_dir)
        self.disabled = False
        self.fence_stamp = fence_stamp(self.path)
        self.path.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.path, 0o700)
        keypath = self.path / 'url.key'
        try:
            fd = os.open(keypath, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            pass
        else:
            with os.fdopen(fd, 'wb') as f:
                f.write(os.urandom(32))
        self.key = keypath.read_bytes()
        self.db = sqlite3.connect(self.path / 'ledger.sqlite3')
        os.chmod(self.path / 'ledger.sqlite3', 0o600)
        self.db.row_factory = sqlite3.Row
        self.db.executescript('''CREATE TABLE IF NOT EXISTS pages (
            target TEXT PRIMARY KEY, epoch TEXT, url_hash TEXT, first_seen REAL,
            last_activation REAL, last_tool REAL, duplicate_since REAL,
            duplicate_winner TEXT, observed_at REAL, revision INTEGER);
            CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT);''')
        self.clock = clock
        self.epoch = None
        self.coverage_since = clock()
        self.fresh = 0
        self.ready = False
        self.pending = {}
        self.leases = {}
        self.command_count = 0
        self.observer_grace = {}
        self.observer_missing = set()
        self.last_healthy_at = None
        self.last_gap_at = None
        self.gap_count = 0
        self.gap_events = []


    def meta(self, key, value):
        self.db.execute('INSERT OR REPLACE INTO metadata VALUES (?,?)', (key,str(value)))
        self.db.commit()

    def digest(self, url):
        return hmac.new(self.key, url.encode(), hashlib.sha256).hexdigest()

    def coverage(self, epoch):
        self.epoch = epoch
        self.coverage_since = self.clock()
        self.ready = False
        self.fresh = 0
        self.meta('browser_epoch', epoch)
        self.meta('coverage_since', self.coverage_since)
        self.db.execute('DELETE FROM pages WHERE epoch != ?', (epoch,))
        self.db.execute('UPDATE pages SET duplicate_since=NULL, duplicate_winner=NULL, revision=revision+1')
        self.db.commit()

    def gap(self, reason='unspecified', method=None):
        self.gap_count += 1
        self.last_gap_at = self.clock()
        reason = reason if isinstance(reason, str) and reason in GAP_REASONS else 'unspecified'
        method = method if isinstance(method, str) and method in GAP_METHODS else None
        self.gap_events.append({'at':self.last_gap_at, 'reason':reason, 'method':method})
        del self.gap_events[:-16]
        self.coverage(self.epoch)

    def heartbeat(self):
        now = self.clock()

        self.fresh = now
        if not self.observer_missing:
            self.last_healthy_at = now
        self.ready = True
        self.meta('heartbeat', now)

    def healthy(self):
        try:
            if fence_stamp(self.path) != self.fence_stamp:
                self.disabled = True
        except OSError:
            self.disabled = True
        return (not self.disabled and self.ready and not self.observer_missing
                and 0 <= self.clock() - self.fresh <= 20)

    def rows(self):
        return [dict(r) for r in self.db.execute('SELECT * FROM pages')]

    def observe_target(self, target):
        # A creation receipt is incremental: never delete other inventory rows.
        if not page_target(target):
            return False
        tid = target['targetId']
        now = self.clock()
        hashed = self.digest(target.get('url', ''))
        old = self.db.execute('SELECT * FROM pages WHERE target=?', (tid,)).fetchone()
        if old is None:
            self.db.execute('INSERT INTO pages VALUES (?,?,?,?,?,?,?,?,?,?)',
                            (tid,self.epoch,hashed,now,0,0,None,None,now,0))
        elif old['url_hash'] != hashed:
            self.db.execute('UPDATE pages SET url_hash=?,last_tool=?,duplicate_since=NULL,duplicate_winner=NULL,observed_at=?,revision=revision+1 WHERE target=?', (hashed,now,now,tid))
        self.regroup()
        self.db.commit()
        return True

    def observe(self, targets):
        now = self.clock()
        ids = []
        for t in targets:
            if not page_target(t):
                continue
            tid = t['targetId']; ids.append(tid)
            hashed = self.digest(t.get('url', ''))
            old = self.db.execute('SELECT * FROM pages WHERE target=?', (tid,)).fetchone()
            if old is None:
                self.db.execute('INSERT INTO pages VALUES (?,?,?,?,?,?,?,?,?,?)',
                                (tid,self.epoch,hashed,now,0,0,None,None,now,0))
            elif old['url_hash'] != hashed:
                self.db.execute('UPDATE pages SET url_hash=?,last_tool=?,duplicate_since=NULL,duplicate_winner=NULL,observed_at=?,revision=revision+1 WHERE target=?', (hashed,now,now,tid))
            else:
                self.db.execute('UPDATE pages SET observed_at=? WHERE target=?', (now,tid))
        for r in self.rows():
            if r['target'] not in ids:
                self.db.execute('DELETE FROM pages WHERE target=?', (r['target'],))
        self.observer_grace = {k:v for k,v in self.observer_grace.items() if k in ids}
        self.observer_missing.intersection_update(ids)
        self.regroup()
        self.db.commit()

    def regroup(self):
        groups = {}
        for r in self.rows():
            groups.setdefault(r['url_hash'], []).append(r)
        now = self.clock()
        for group in groups.values():
            winner = min(group, key=lambda r: (-max(r['first_seen'],r['last_activation'],r['last_tool']), r['target']))['target']
            for r in group:
                loser = len(group) > 1 and r['target'] != winner
                if not loser:
                    self.db.execute('UPDATE pages SET duplicate_since=NULL,duplicate_winner=NULL WHERE target=?',(r['target'],))
                elif r['duplicate_winner'] != winner or r['duplicate_since'] is None:
                    self.db.execute('UPDATE pages SET duplicate_since=?,duplicate_winner=? WHERE target=?',(now,winner,r['target']))

    def touch(self, target, kind):
        field = 'last_activation' if kind == 'activation' else 'last_tool'
        self.db.execute(f'UPDATE pages SET {field}=?, duplicate_since=NULL,duplicate_winner=NULL,revision=revision+1 WHERE target=?',(self.clock(),target))
        self.regroup()
        self.db.commit()

    def lease_start(self, call_id, ttl):
        ttl = float(ttl)
        if not math.isfinite(ttl):
            raise ValueError('invalid_ttl')
        self.leases[call_id] = self.clock() + min(1900, max(60, ttl))

    def lease_end(self, call_id):
        self.leases.pop(call_id,None)

    def fenced(self, target=None):
        now = self.clock()
        self.leases = {k:v for k,v in self.leases.items() if v > now}
        return bool(self.leases) or bool(self.pending.get('*',0)) or bool(self.pending.get(target,0))

    def candidates(self):
        if not self.healthy() or self.fenced():
            return []
        now = self.clock(); result = []
        for r in sorted(self.rows(), key=lambda r:r['target']):
            if self.fenced(r['target']):
                continue
            last = max(r['first_seen'],r['last_activation'],r['last_tool'],self.coverage_since,
                       self.observer_grace.get(r['target'], 0))
            if r['url_hash'] == self.digest('about:blank') and now-r['first_seen'] >= self.policy.blank_minutes * 60:
                result.append((r['target'],'blank'))
            elif now-last >= self.policy.idle_hours * 3600:
                result.append((r['target'],'idle'))
            elif r['duplicate_since'] is not None and now-max(r['duplicate_since'],last) >= self.policy.duplicate_hours * 3600:
                result.append((r['target'],'duplicate'))
        return result

    def close(self):
        self.db.close()
