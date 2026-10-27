"""Offline observer regressions: no browser, network, or tab closures."""
import asyncio
import json
import tempfile
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import tabwatch_plugin.service as module
from tabwatch_plugin.service import Service, WORLD


def page(tid='a', url='https://example.org/'):
    return {'targetId': tid, 'type': 'page', 'url': url}


class ObserverLivenessTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = Service(Path(self.tmp.name))
        self.calls = []

    async def asyncTearDown(self):
        for task in self.service.tasks:
            task.cancel()
        await asyncio.gather(*self.service.tasks, return_exceptions=True)
        self.service.store.close()
        self.tmp.cleanup()

    def covered(self, sid, tid, child=False):
        s = self.service
        s.sessions[sid] = tid
        s.session_targets[sid] = sid if child else tid
        s.root_contexts[sid] = 1
        s.contexts.add((sid, 1))
        s.covered_sessions.add(sid)
        s.scripts[sid] = 'script-' + sid
        if child:
            s.child_sessions.add(sid)
        else:
            s.installed.add(tid)

    async def cycle(self, targets, on_inventory=None, on_install=None, final=None):
        s = self.service
        count = 0
        class Connection:
            async def __aenter__(self): return self
            async def __aexit__(self, *args): pass
        async def call(method, params=None, session=None):
            nonlocal count
            self.calls.append((method, params, session))
            if method == 'Target.getTargets':
                count += 1
                if on_inventory:
                    on_inventory(count)
                return {'targetInfos': targets if count == 1 or final is None else final}
            return {}
        class FakeRPC:
            async def call(self, method, params=None, session=None):
                return await call(method, params, session)
            async def close(self): pass
        rpc = FakeRPC()
        async def version():
            return {'webSocketDebuggerUrl': 'ws://127.0.0.1/devtools/browser/offline'}
        async def install(target):
            self.covered('root-' + target['targetId'], target['targetId'])
            s.store.observer_missing.discard(target['targetId'])
            if on_install:
                on_install(target)
        async def stop_after_cycle(*args): raise asyncio.CancelledError
        s.version = version
        s.install = install
        with patch.object(module, 'connect', return_value=Connection()), \
                patch.object(module, 'RPC', return_value=rpc), \
                patch.object(module.asyncio, 'sleep', side_effect=stop_after_cycle):
            with self.assertRaises(asyncio.CancelledError):
                await s.observe_forever()

    async def test_disconnect_during_final_inventory_cannot_restore_health(self):
        def disconnect(count):
            if count == 2:
                self.service.observer_event({'method': '__tabwatch.disconnected'})
        await self.cycle([page()], on_inventory=disconnect)
        self.assertFalse(self.service.store.healthy())

    async def test_gap_during_final_inventory_cannot_restore_health(self):
        def gap(count):
            if count == 2:
                self.service.store.gap()
        await self.cycle([page()], on_inventory=gap)
        self.assertFalse(self.service.store.healthy())

    async def test_successful_recovery_clears_old_observer_error(self):
        self.service.observer_error = {'kind': 'URLError', 'method': None}
        await self.cycle([page()])
        self.assertTrue(self.service.store.healthy())
        self.assertIsNone(self.service.observer_error)

    async def test_incomplete_recovery_retains_error_and_close_fence(self):
        error = {'kind': 'URLError', 'method': None}
        self.service.observer_error = error.copy()
        await self.cycle([page()], final=[page(url='https://changed.example/')])
        self.assertFalse(self.service.store.healthy())
        self.assertEqual(self.service.observer_error, error)
        self.assertEqual((await self.service.sweep(apply=True))['closed'], [])

    async def test_refused_discovery_reports_safe_numeric_cause(self):
        from urllib.error import URLError
        async def refused():
            raise URLError(ConnectionRefusedError(111, 'PRIVATE_URL_AND_TOKEN'))
        async def stop(*args):
            raise asyncio.CancelledError
        self.service.version = refused
        with patch.object(module.asyncio, 'sleep', side_effect=stop):
            with self.assertRaises(asyncio.CancelledError):
                await self.service.observe_forever()
        error = self.service.observer_error
        self.assertEqual(error['kind'], 'URLError')
        self.assertEqual(error['stage'], 'discovery')
        self.assertEqual(error['cause_kind'], 'ConnectionRefusedError')
        self.assertEqual(error['errno'], 111)
        self.assertNotIn('PRIVATE', json.dumps(error))
        self.assertFalse(self.service.store.healthy())

    async def test_http_discovery_reports_status_without_reason_or_url(self):
        from urllib.error import HTTPError
        async def refused():
            raise HTTPError('http://private.example/SECRET', 503, 'SECRET', {}, None)
        async def stop(*args):
            raise asyncio.CancelledError
        self.service.version = refused
        with patch.object(module.asyncio, 'sleep', side_effect=stop):
            with self.assertRaises(asyncio.CancelledError):
                await self.service.observe_forever()
        self.assertEqual(self.service.observer_error['http_status'], 503)
        self.assertNotIn('SECRET', json.dumps(self.service.observer_error))

    async def test_retired_and_nonadmitted_roots_remove_exact_sessions(self):
        s = self.service
        def seed(count):
            if count != 1: return
            for tid in ('gone', 'internal'):
                self.covered('root-' + tid, tid)
                self.covered('child-' + tid, tid, child=True)
            s.install_errors['gone'] = {'kind': 'RuntimeError'}
        await self.cycle([page('internal', 'chrome://settings/')], on_inventory=seed)
        self.assertEqual(s.sessions, {})
        self.assertEqual(s.scripts, {})
        self.assertEqual(s.root_contexts, {})
        self.assertEqual(s.contexts, set())
        self.assertEqual(s.session_targets, {})
        self.assertEqual(s.child_sessions, set())
        self.assertEqual(s.install_errors, {})
        detached = {p['sessionId'] for m, p, _ in self.calls if m == 'Target.detachFromTarget'}
        self.assertEqual(detached, {'root-gone', 'child-gone', 'root-internal', 'child-internal'})
        self.assertNotIn('Target.closeTarget', [m for m, _, _ in self.calls])
        s.observer_event({'method': 'Runtime.executionContextCreated', 'sessionId': 'root-gone',
                          'params': {'context': {'name': WORLD, 'id': 99}}})
        s.observer_event({'method': 'Runtime.executionContextsCleared', 'sessionId': 'root-gone'})
        s.observer_event({'method': 'Target.detachedFromTarget', 'params': {'sessionId': 'root-gone'}})
        self.assertEqual(s.contexts, set())
        self.assertEqual(s.store.observer_missing, set())
        self.assertTrue(s.store.healthy())

    async def test_worker_and_metadata_churn_during_install_does_not_starve(self):
        s = self.service
        def churn(target):
            for method in ('Target.targetCreated', 'Target.targetInfoChanged', 'Target.targetDestroyed'):
                s.observer_event({'method': method, 'params': {
                    'targetId': 'worker', 'targetInfo': {'targetId': 'worker', 'type': 'worker', 'url': 'https://example.org/w.js'}}})
            s.observer_event({'method': 'Target.targetInfoChanged', 'params': {
                'targetInfo': dict(target, title='changing title', attached=True)}})
        await self.cycle([page()], on_install=churn)
        self.assertTrue(s.store.healthy())
        self.assertEqual(s.inventory_generation, 0)

    async def test_final_inventory_url_change_remains_fenced(self):
        await self.cycle([page()], final=[page(url='https://other.example/')])
        self.assertFalse(self.service.store.healthy())

    async def test_context_loss_during_final_inventory_remains_fenced(self):
        def lose(count):
            if count == 2:
                self.service.observer_event({'method': 'Runtime.executionContextsCleared',
                                             'sessionId': 'root-a'})
        await self.cycle([page()], on_inventory=lose)
        self.assertFalse(self.service.store.healthy())
        self.assertIn('a', self.service.store.observer_missing)

    async def test_real_url_and_admission_changes_fence_ready_observer(self):
        await self.cycle([page()])
        s = self.service
        for info in (page(url='https://other.example/'), page(url='chrome://settings/')):
            s.store.heartbeat()
            s.observer_event({'method': 'Target.targetInfoChanged', 'params': {'targetInfo': info}})
            self.assertFalse(s.store.ready)
        s.store.heartbeat()
        s.observer_event({'method': 'Target.targetCreated', 'params': {'targetInfo': page('new')}})
        self.assertFalse(s.store.ready)


    async def test_native_creation_receipt_then_immediate_read_preserves_old_grace(self):
        s = self.service
        now = [1000000.]
        s.store.clock = lambda: now[0]
        s.store.coverage('offline')
        s.store.observe([page('old')])
        coverage = s.store.coverage_since
        old = s.store.rows()[0].copy()
        now[0] += 8000
        commands = [
            {'id':1, 'method':'Target.createTarget', 'params':{'url':'https://new.example/?secret=opaque'}},
            {'id':2, 'method':'Target.attachToTarget', 'params':{'targetId':'new','flatten':True}},
            {'id':3, 'method':'Runtime.evaluate', 'sessionId':'new-session', 'params':{'expression':'42'}},
        ]
        test = self
        class Client:
            request = type('Request', (), {'path':'/devtools/browser/offline'})()
            def __init__(self):
                self.queue = asyncio.Queue()
                self.queue.put_nowait(json.dumps(commands[0]))
            def __aiter__(self): return self
            async def __anext__(self): return await self.queue.get()
            async def close(self, *args): pass
            async def send(self, raw):
                msg = json.loads(raw)
                if msg['id'] == 1:
                    # Admission must precede delivery of the native receipt.
                    test.assertIn('new', {r['target'] for r in s.store.rows()})
                if msg['id'] < 3:
                    self.queue.put_nowait(json.dumps(commands[msg['id']]))
        class Upstream:
            def __init__(self): self.queue = asyncio.Queue()
            async def __aenter__(self): return self
            async def __aexit__(self, *args): pass
            def __aiter__(self): return self
            async def __anext__(self):
                raw = await self.queue.get()
                if raw is None: raise StopAsyncIteration
                return raw
            async def send(self, raw):
                msg = json.loads(raw)
                result = {1:{'targetId':'new'}, 2:{'sessionId':'new-session'}, 3:{'result':{'value':42}}}[msg['id']]
                self.queue.put_nowait(json.dumps(dict(msg, result=result)))
                if msg['id'] == 3: self.queue.put_nowait(None)
        with patch.object(module, 'connect', return_value=Upstream()):
            await asyncio.wait_for(s.proxy(Client()), timeout=2)
        self.assertEqual(s.store.coverage_since, coverage)
        self.assertEqual(s.store.gap_count, 0)
        self.assertEqual(next(r for r in s.store.rows() if r['target'] == 'old'), old)
        self.assertEqual(next(r for r in s.store.rows() if r['target'] == 'new')['last_tool'], now[0])
        self.assertNotIn(b'opaque', (s.store.path / 'ledger.sqlite3').read_bytes())

    async def test_creation_event_admits_only_missing_pages(self):
        s = self.service
        s.store.coverage('offline')
        s.store.observe([page('old')])
        before = s.store.coverage_since
        s.observer_event({'method':'Target.targetCreated','params':{'targetInfo':page('new')}})
        self.assertEqual({r['target'] for r in s.store.rows()}, {'old','new'})
        original = next(r for r in s.store.rows() if r['target'] == 'new')
        s.observer_event({'method':'Target.targetCreated','params':{'targetInfo':page('new','https://stale.example/')}})
        self.assertEqual(next(r for r in s.store.rows() if r['target'] == 'new'), original)
        self.assertEqual(s.store.coverage_since, before)
        self.assertEqual(s.store.gap_count, 0)


    async def test_retired_child_failure_callback_cannot_poison_surviving_root(self):
        s = self.service
        s.store.coverage('offline')
        s.store.observe([page('gone'),page('survivor')])
        self.covered('gone-root','gone')
        self.covered('survivor-root','survivor')
        started = asyncio.Event()
        async def pending_install(sid):
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                raise RuntimeError('late retired child failure')
        s.install_session = pending_install
        s.observer_event({'method':'Target.attachedToTarget','sessionId':'gone-root',
                          'params':{'sessionId':'gone-child','targetInfo':{'type':'iframe','targetId':'frame'}}})
        await started.wait()
        class RPC:
            async def call(self, method, params=None, session=None):
                # Real detach receipts may arrive synchronously during cleanup.
                if method == 'Target.detachFromTarget':
                    s.observer_event({'method':method.replace('detachFromTarget','detachedFromTarget'),
                                      'params':params})
                return {}
        s.rpc = RPC()
        s.store.observe([page('survivor')])
        await s.retire_sessions({'gone'})
        self.assertEqual(s.sessions, {'survivor-root':'survivor'})
        self.assertEqual(s.session_installs, {})
        self.assertEqual(s.covered_sessions, {'survivor-root'})
        self.assertEqual(s.store.observer_missing, set())
        s.store.heartbeat()
        self.assertTrue(s.store.healthy())

    async def test_root_loss_during_slow_install_cannot_be_accepted(self):
        def lose(target):
            self.service.observer_event({'method':'Runtime.executionContextsCleared',
                                         'sessionId':'root-a'})
        await self.cycle([page()], on_install=lose)
        self.assertFalse(self.service.store.healthy())
        self.assertIn('a', self.service.store.observer_missing)

    async def test_title_and_attachment_metadata_leave_ready_inventory_ready(self):
        await self.cycle([page()])
        s = self.service
        generation = s.inventory_generation
        s.observer_event({'method':'Target.targetInfoChanged','params':{
            'targetInfo':dict(page(),title='new title',attached=False)}})
        self.assertTrue(s.store.healthy())
        self.assertEqual(s.inventory_generation,generation)


    async def test_proven_child_removal_during_install_uses_final_validation(self):
        s = self.service
        def churn(target):
            self.covered('child-a','a',child=True)
            s.observer_event({'method':'Target.detachedFromTarget','sessionId':'root-a',
                              'params':{'sessionId':'child-a'}})
            s.observer_event({'method':'Page.frameDetached','sessionId':'root-a',
                              'params':{'frameId':'child-a','reason':'remove'}})
        await self.cycle([page()], on_install=churn)
        self.assertGreater(s.inventory_generation,0)
        self.assertTrue(s.store.healthy())
        self.assertEqual(s.store.observer_grace,{})
