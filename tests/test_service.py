"""Synthetic-clock policy tests; integration uses only disposable Chrome."""
import tempfile
import unittest
from pathlib import Path
from tabwatch_plugin.store import Store

class PolicyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.now = 1000000.
        self.s = Store(Path(self.tmp.name), clock=lambda: self.now)
        self.s.coverage('browser-one')
    def tearDown(self):
        self.s.close()
        self.tmp.cleanup()
    def observe(self, targets):
        self.s.observe([{'targetId': t, 'type': 'page', 'url': u} for t,u in targets])
        self.s.heartbeat()
    def test_idle_requires_continuous_coverage(self):
        self.observe([('a','https://example.org/?secret=yes')])
        self.now += 86401
        self.observe([('a','https://example.org/?secret=yes')])
        self.assertEqual(self.s.candidates(), [('a','idle')])
        self.s.coverage('browser-one')
        self.assertEqual(self.s.candidates(), [])
        self.assertNotIn(b'secret', (Path(self.tmp.name)/'ledger.sqlite3').read_bytes())
    def test_duplicate_timer_resets_on_activity_and_winner(self):
        self.observe([('a','https://example.org/'), ('b','https://example.org/')])
        self.now += 7201
        self.observe([('a','https://example.org/'), ('b','https://example.org/')])
        self.assertEqual(self.s.candidates(), [('b','duplicate')])
        self.s.touch('b','tool')
        self.assertEqual(self.s.candidates(), [])
        self.now += 7201
        self.observe([('a','https://example.org/'), ('b','https://example.org/')])
        self.assertEqual(self.s.candidates(), [('a','duplicate')])
    def test_browser_ui_and_extensions_are_outside_scope(self):
        self.observe([('normal','https://example.org/'),('ui','chrome://settings/'),
                      ('wallet','chrome-extension://example/wallet.html')])
        self.assertEqual([r['target'] for r in self.s.rows()],['normal'])

    def test_nonfinite_lease_cannot_bypass_fence(self):
        with self.assertRaises(ValueError):
            self.s.lease_start('bad',float('nan'))

    def test_exact_url_includes_query_and_fragment(self):
        self.observe([('a','https://example.org/?x=1#one'),
                      ('b','https://example.org/?x=1#two'),
                      ('c','https://example.org/?x=2#one')])
        self.now+=7201
        self.s.heartbeat()
        self.assertEqual(self.s.candidates(),[])
        self.assertEqual(len({r['url_hash'] for r in self.s.rows()}),3)

    def test_last_survivor_expires_at_24h(self):
        self.observe([('a','https://example.org/'),('b','https://example.org/')])
        self.now+=7201
        self.observe([('a','https://example.org/')])
        self.assertEqual(self.s.candidates(),[])
        self.now+=79200
        self.s.heartbeat()
        self.assertEqual(self.s.candidates(),[('a','idle')])

    def test_restart_resets_coverage_grace_and_preserves_hmac(self):
        url='https://example.org/?private=opaque'
        self.observe([('a',url)])
        hashed=self.s.rows()[0]['url_hash']
        self.s.close()
        self.now+=90000
        self.s=Store(Path(self.tmp.name),clock=lambda:self.now)
        self.s.coverage('browser-one')
        self.observe([('a',url)])
        self.assertEqual(self.s.rows()[0]['url_hash'],hashed)
        self.assertEqual(self.s.candidates(),[])

    def test_pending_leases_and_stale_health_fail_closed(self):
        self.observe([('a','https://example.org/')])
        self.now += 86401
        self.observe([('a','https://example.org/')])
        self.s.pending['a'] = 1
        self.assertEqual(self.s.candidates(), [])
        self.s.pending.clear()
        self.s.lease_start('call', 1)
        self.assertEqual(self.s.candidates(), [])
        self.now += 61
        self.assertEqual(self.s.candidates(), [])
        self.s.heartbeat()
        self.assertEqual(self.s.candidates(), [('a','idle')])

class CloseRaceTests(unittest.IsolatedAsyncioTestCase):
    async def test_changed_duplicate_winner_url_blocks_close(self):
        from tabwatch_plugin.service import Service
        with tempfile.TemporaryDirectory() as d:
            service=Service(Path(d),'http://127.0.0.1:1',0)
            now=[1000000.]
            service.store.clock=lambda:now[0]
            service.store.coverage('synthetic-browser')
            service.store.observe([{'targetId':t,'type':'page','url':'https://example.org/'} for t in ['a','b']])
            now[0]+=7201
            service.store.heartbeat()
            class SyntheticRPC:
                closes=0
                async def call(self,method,params=None):
                    if method=='Target.closeTarget':
                        self.closes+=1
                        return {'success':True}
                    return {'targetInfos':[{'targetId':'a','type':'page','url':'https://other.example/'},
                                           {'targetId':'b','type':'page','url':'https://example.org/'}]}
            rpc=SyntheticRPC(); service.rpc=rpc
            async def version(): return {'webSocketDebuggerUrl':'ws://127.0.0.1/devtools/browser/synthetic-browser'}
            service.version=version
            try:
                await service.sweep(True)
                self.assertEqual(rpc.closes,0)
            finally: service.store.close()

class DisposableTests(unittest.IsolatedAsyncioTestCase):
    async def test_proxy_observer_and_verified_close(self):
        import asyncio, json, os, shutil, subprocess, urllib.request
        import websockets
        from tabwatch_plugin.service import Service
        self.assertTrue(shutil.which('chromium'))
        with tempfile.TemporaryDirectory() as d:
            proc = subprocess.Popen(['chromium','--headless=new','--no-sandbox','--disable-gpu',
                '--remote-debugging-port=0', '--user-data-dir='+d+'/chrome','about:blank'],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
            service = None
            try:
                active = Path(d)/'chrome'/'DevToolsActivePort'
                for _ in range(100):
                    if active.exists(): break
                    await asyncio.sleep(.1)
                port = int(active.read_text().splitlines()[0])
                service = Service(Path(d)/'state', 'http://127.0.0.1:'+str(port), 0)
                await service.start()
                for _ in range(100):
                    if service.store.healthy(): break
                    await asyncio.sleep(.1)
                self.assertTrue(service.store.healthy())
                proxy_port = service.server.sockets[0].getsockname()[1]
                def fetch():
                    return json.load(urllib.request.urlopen(f'http://127.0.0.1:{proxy_port}/json/version'))
                discovery = await asyncio.to_thread(fetch)
                self.assertIn(':'+str(proxy_port)+'/', discovery['webSocketDebuggerUrl'])
                with self.assertRaises(websockets.exceptions.InvalidStatus):
                    async with websockets.connect(discovery['webSocketDebuggerUrl'],origin='https://untrusted.example'):
                        pass
                async with websockets.connect(discovery['webSocketDebuggerUrl']) as ws:
                    seq = 0
                    async def rpc(method, params=None, session=None):
                        nonlocal seq
                        seq += 1; ident=seq
                        msg={'id':ident,'method':method,'params':params or {}}
                        if session: msg['sessionId']=session
                        await ws.send(json.dumps(msg))
                        while True:
                            reply=json.loads(await ws.recv())
                            if reply.get('id')==ident:
                                self.assertNotIn('error',reply)
                                return reply.get('result',{})
                    a=(await rpc('Target.createTarget',{'url':'about:blank'}))['targetId']
                    b=(await rpc('Target.createTarget',{'url':'about:blank'}))['targetId']
                    for _ in range(100):
                        if a in service.installed and b in service.installed: break
                        await asyncio.sleep(.1)
                    sid=(await rpc('Target.attachToTarget',{'targetId':a,'flatten':True}))['sessionId']
                    before={r['target']:r['last_tool'] for r in service.store.rows()}
                    result=await rpc('Runtime.evaluate',{'expression':'6*7','returnByValue':True},sid)
                    self.assertEqual(result['result']['value'],42)
                    after={r['target']:r['last_tool'] for r in service.store.rows()}
                    self.assertGreater(after[a],before[a]); self.assertEqual(after[b],before[b])
                    loser=next(r for r in service.store.rows() if r['target']==b)
                    self.assertEqual(loser['duplicate_winner'],a)
                    await rpc('Runtime.enable',session=sid)
                    self.assertEqual(next(r for r in service.store.rows() if r['target']==a)['last_tool'],after[a])
                    slow=asyncio.create_task(rpc('Runtime.evaluate',{
                        'expression':'new Promise(r => setTimeout(() => r(17), 500))',
                        'awaitPromise':True,'returnByValue':True},sid))
                    for _ in range(30):
                        if service.store.pending.get(a): break
                        await asyncio.sleep(.01)
                    self.assertEqual(service.store.pending.get(a),1)
                    started=next(r for r in service.store.rows() if r['target']==a)['last_tool']
                    await slow
                    self.assertEqual(service.store.pending.get(a),0)
                    self.assertGreater(next(r for r in service.store.rows() if r['target']==a)['last_tool'],started)
                    # Direct-page websocket routing also identifies exactly one target.
                    async with websockets.connect(f'ws://127.0.0.1:{proxy_port}/devtools/page/{b}') as direct_ws:
                        await direct_ws.send(json.dumps({'id':1,'method':'Runtime.evaluate','params':{'expression':'42'}}))
                        self.assertEqual(json.loads(await direct_ws.recv())['result']['result']['value'],42)
                    self.assertEqual(next(r for r in service.store.rows() if r['target']==a)['duplicate_winner'],b)
                    await rpc('Page.navigate',{'url':'data:text/html,<title>disposable</title>'},sid)
                    await rpc('Target.activateTarget',{'targetId':a})
                    await asyncio.sleep(.1)
                    old_activation=next(r for r in service.store.rows() if r['target']==b)['last_activation']
                    await rpc('Target.activateTarget',{'targetId':b})
                    await asyncio.sleep(.5)
                    self.assertGreater(next(r for r in service.store.rows() if r['target']==b)['last_activation'],old_activation)
                    # Synthetic DOM events in the page cannot invoke the isolated binding.
                    activated=next(r for r in service.store.rows() if r['target']==a)['last_activation']
                    await rpc('Runtime.evaluate',{'expression':"window.dispatchEvent(new Event('focus'))"},sid)
                    self.assertEqual(next(r for r in service.store.rows() if r['target']==a)['last_activation'],activated)
                    coverage=service.store.coverage_since
                    assert service.rpc is not None
                    await service.rpc.ws.close()
                    for _ in range(150):
                        if service.store.healthy() and service.store.coverage_since>coverage: break
                        await asyncio.sleep(.05)
                    self.assertTrue(service.store.healthy())
                    self.assertGreater(service.store.coverage_since,coverage)
                    # Unix control output exposes only safe identities and timestamps.
                    reader,writer=await asyncio.open_unix_connection(str(service.socket_path))
                    writer.write(b'{"command":"health"}\n'); await writer.drain()
                    health=json.loads(await reader.readline())
                    writer.close(); await writer.wait_closed()
                    self.assertTrue(any(r['target_id']==a and r['last_tool']>0 for r in health['targets']))
                    self.assertNotIn('disposable',json.dumps(health))
                    # This isolated ledger uses a synthetic policy clock only. Never user state.
                    base=service.store.clock()
                    service.store.clock=lambda:base+90000
                    service.store.heartbeat()
                    dry=await service.sweep()
                    self.assertTrue(dry['candidates'])
                    self.assertEqual(dry['closed'],[])
                    service.store.lease_start('integration',60)
                    self.assertEqual((await service.sweep(True))['closed'],[])
                    service.store.lease_end('integration')
                    result=await service.sweep(True)
                    self.assertTrue(result['closed'],result)
                    remaining=(await rpc('Target.getTargets'))['targetInfos']
                    self.assertTrue(all(t['targetId'] not in result['closed'] for t in remaining))
                self.assertGreater(service.store.command_count,0)
            finally:
                if service: await service.stop()
                import signal
                try: os.killpg(proc.pid, signal.SIGTERM)
                except ProcessLookupError: pass
                try: await asyncio.to_thread(proc.wait,timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(proc.pid, signal.SIGKILL); await asyncio.to_thread(proc.wait)
                await asyncio.sleep(.5)

class ObserverRegressionTests(unittest.TestCase):
    """Synthetic-clock context-loss policy regressions, no live browser."""
    def setUp(self):
        from tabwatch_plugin.service import Service
        self.tmp = tempfile.TemporaryDirectory()
        self.service = Service(Path(self.tmp.name), 'http://127.0.0.1:1', 0)
        self.now = 1000000.
        s = self.service.store
        s.clock = lambda: self.now
        s.coverage('synthetic-browser')
        s.observe([{'targetId':t, 'type':'page', 'url':'https://' + t + '.example/'}
                   for t in ['a', 'b']])
        self.now += 90000
        s.heartbeat()
        self.service.sessions = {'sa':'a', 'sb':'b'}
        self.service.installed = {'a', 'b'}
        self.service.root_contexts = {'sa':1, 'sb':2}
        self.service.contexts = {('sa',1), ('sa',3), ('sb',2)}

    def tearDown(self):
        self.service.store.close()
        self.tmp.cleanup()

    def test_root_context_loss_fences_until_restore_with_target_only_grace(self):
        service = self.service; s = service.store
        coverage = s.coverage_since
        self.assertEqual(s.candidates(), [('a','idle'), ('b','idle')])
        service.observer_event({'method':'Runtime.executionContextDestroyed',
                                'sessionId':'sa', 'params':{'executionContextId':1}})
        self.assertNotIn('a', service.installed)
        self.assertIn('b', service.installed)
        self.assertFalse(s.healthy())
        s.heartbeat()
        self.assertFalse(s.healthy())
        self.assertEqual(s.candidates(), [])
        # Synthetic instrumentation-restored boundary.
        s.observer_missing.remove('a'); service.installed.add('a'); s.heartbeat()
        self.assertEqual(s.coverage_since, coverage)
        self.assertEqual(s.candidates(), [('b','idle')])
        self.now += 86401; s.heartbeat()
        self.assertEqual(s.candidates(), [('a','idle'), ('b','idle')])

    def test_subframe_teardown_does_not_reset_observer_or_grace(self):
        service = self.service; s = service.store
        service.observer_event({'method':'Runtime.executionContextDestroyed',
                                'sessionId':'sa', 'params':{'executionContextId':3}})
        self.assertTrue(s.healthy())
        self.assertEqual(service.installed, {'a','b'})
        self.assertEqual(s.observer_grace, {})
        self.assertEqual(s.candidates(), [('a','idle'), ('b','idle')])

    def test_oopif_detach_requires_removal_receipt_without_long_grace(self):
        service=self.service; s=service.store
        service.sessions['child']='a'
        service.child_sessions.add('child')
        service.session_targets['child']='frame'
        service.root_contexts['child']=4
        service.contexts.add(('child',4))
        service.covered_sessions.update({'sa','sb','child'})
        coverage=s.coverage_since
        service.observer_event({'method':'Target.detachedFromTarget',
            'sessionId':'sa','params':{'sessionId':'child'}})
        s.heartbeat()
        self.assertFalse(s.healthy())
        self.assertIn(('a','frame'),service.detached_frames)
        self.assertEqual(service.installed,{'a','b'})
        self.assertEqual(s.observer_grace,{})
        self.assertEqual(s.coverage_since,coverage)
        service.observer_event({'method':'Page.frameDetached','sessionId':'sa',
            'params':{'frameId':'frame','reason':'remove'}})
        self.assertFalse(service.detached_frames)
        self.assertEqual(service.root_contexts,{'sa':1,'sb':2})
        # Next complete inventory can clear the exact missing page, without
        # root reattachment or another idle/duplicate grace period.
        s.observer_missing.discard('a'); s.heartbeat()
        self.assertEqual(s.candidates(),[('a','idle'),('b','idle')])

    def test_contexts_cleared_invalidates_exact_session(self):
        service = self.service
        service.observer_event({'method':'Runtime.executionContextsCleared', 'sessionId':'sa'})
        self.assertFalse(service.store.healthy())
        self.assertEqual(service.installed, {'b'})
        self.assertEqual(service.contexts, {('sb',2)})
        self.assertEqual(service.store.observer_missing, {'a'})


class FrameInstallationTests(unittest.IsolatedAsyncioTestCase):
    async def test_existing_nested_frame_tree_and_failed_frame(self):
        from tabwatch_plugin.service import Service
        for fail in (False, True):
            with self.subTest(fail=fail), tempfile.TemporaryDirectory() as d:
                service=Service(Path(d),'http://127.0.0.1:1',0)
                service.store.coverage('synthetic-browser')
                service.store.observe([{'targetId':'a','type':'page','url':'https://example.org/'}])
                created=[]; evaluated=[]
                class SyntheticRPC:
                    async def call(self,method,params=None,session=None):
                        if method=='Target.attachToTarget': return {'sessionId':'sa'}
                        if method=='Page.addScriptToEvaluateOnNewDocument': return {'identifier':'script'}
                        if method=='Page.getFrameTree':
                            return {'frameTree':{'frame':{'id':'root'},'childFrames':[
                                {'frame':{'id':'child'},'childFrames':[{'frame':{'id':'nested'}}]}]}}
                        if method=='Page.createIsolatedWorld':
                            created.append(params['frameId'])
                            return {'executionContextId':len(created)}
                        if method=='Runtime.evaluate':
                            evaluated.append(params['contextId'])
                            if fail and params['contextId']==3: return {'exceptionDetails':{}}
                        return {}
                service.rpc=SyntheticRPC()
                try:
                    if fail:
                        with self.assertRaises(RuntimeError):
                            await service.install({'targetId':'a'})
                        service.store.heartbeat()
                        self.assertFalse(service.store.healthy())
                        self.assertNotIn('a',service.installed)
                        self.assertIn('a',service.store.observer_missing)
                    else:
                        await service.install({'targetId':'a'})
                        service.store.heartbeat()
                        self.assertTrue(service.store.healthy())
                    self.assertEqual(created,['root','child','nested'])
                    self.assertEqual(evaluated,[1,2,3])
                finally: service.store.close()


class DisposableObserverRegressions(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        import asyncio, os, subprocess
        from tabwatch_plugin.service import Service, RPC, fetch_json
        self.tmp = tempfile.TemporaryDirectory(dir=os.environ.get('TMPDIR'))
        self.proc = subprocess.Popen(['chromium', '--headless=new', '--no-sandbox',
            '--disable-gpu', '--remote-debugging-port=0',
            '--user-data-dir='+self.tmp.name+'/chrome', 'about:blank'],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
        self.service = None; self.client = None
        self.addAsyncCleanup(self._cleanup)
        active = Path(self.tmp.name)/'chrome'/'DevToolsActivePort'
        await self.wait(lambda: active.exists())
        port = int(active.read_text().splitlines()[0])
        upstream = 'http://127.0.0.1:'+str(port)
        self.service = Service(Path(self.tmp.name)/'state', upstream, 0)
        await self.service.start()
        await self.wait(lambda: self.service.store.healthy())
        import websockets
        endpoint = (await fetch_json(upstream+'/json/version'))['webSocketDebuggerUrl']
        self.client = RPC(await websockets.connect(endpoint), lambda msg: None)
        self.tid = self.service.store.rows()[0]['target']
        self.sid = (await self.client.call('Target.attachToTarget',
                    {'targetId':self.tid, 'flatten':True}))['sessionId']

    async def wait(self, predicate):
        import asyncio
        for _ in range(600):
            if predicate(): return
            await asyncio.sleep(.05)
        self.fail('disposable Chromium condition timed out')

    async def _cleanup(self):
        import asyncio, os, signal, subprocess
        if self.client: await self.client.close()
        if self.service: await self.service.stop()
        # Only this disposable browser's process group is terminated.
        try: os.killpg(self.proc.pid, signal.SIGTERM)
        except ProcessLookupError: pass
        try: await asyncio.to_thread(self.proc.wait, timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(self.proc.pid, signal.SIGKILL)
            await asyncio.to_thread(self.proc.wait)
        # Kill any surviving writers in the exact fixture-owned group.
        try: os.killpg(self.proc.pid, signal.SIGKILL)
        except ProcessLookupError: pass
        await asyncio.sleep(.5)
        self.tmp.cleanup()

    async def test_observer_install_is_not_user_activity(self):
        import asyncio
        await self.client.call('Emulation.setFocusEmulationEnabled', {'enabled':True}, self.sid)
        await asyncio.sleep(.1)
        before = self.activation()
        await self.service.install({'targetId':self.tid, 'type':'page', 'url':'about:blank'})
        await asyncio.sleep(.1)
        self.assertEqual(self.activation(), before)

    async def test_actual_six_hour_idle_close_with_busy_fence(self):
        import time
        now = [time.time()]
        self.service.store.clock = lambda: now[0]
        url = 'data:text/html,<p>disposable-idle-policy-probe</p>'
        await self.client.call('Page.navigate', {'url':url}, self.sid)
        await self.wait(lambda: self.service.store.healthy() and
                        self.service.store.rows()[0]['url_hash'] == self.service.store.digest(url))
        keeper = (await self.client.call('Target.createTarget', {'url':'about:blank'}))['targetId']
        await self.wait(lambda: self.service.store.healthy() and keeper in self.service.installed)
        now[0] += 21599
        self.service.store.heartbeat()
        self.assertEqual((await self.service.sweep(True))['closed'], [])
        now[0] += 1
        self.service.store.heartbeat()
        self.service.store.lease_start('fixture-busy', 60)
        self.assertEqual((await self.service.sweep(True))['closed'], [])
        self.service.store.lease_end('fixture-busy')
        result = await self.service.sweep(True)
        self.assertEqual(result['closed'], [self.tid])
        self.assertIn({'target_id':self.tid, 'reason':'idle'}, result['candidates'])
        remaining = (await self.client.call('Target.getTargets'))['targetInfos']
        self.assertNotIn(self.tid, [t['targetId'] for t in remaining])
        self.assertIn(keeper, [t['targetId'] for t in remaining])

    def activation(self):
        return next(r for r in self.service.store.rows() if r['target']==self.tid)['last_activation']

    async def test_trusted_same_tab_input_and_throttling(self):
        import asyncio
        rpc = self.client.call
        await rpc('Runtime.evaluate', {'expression':
            "document.body.innerHTML='<input autofocus style=\"width:500px;height:100px\">';document.querySelector('input').focus()"}, self.sid)
        await rpc('Target.activateTarget', {'targetId':self.tid})
        await asyncio.sleep(.2)
        before = self.activation()
        last_tool = self.service.store.rows()[0]['last_tool']
        # Direct upstream input is trusted Chromium input, not proxy/tool activity.
        await rpc('Input.dispatchKeyEvent', {'type':'keyDown', 'key':'a', 'code':'KeyA', 'text':'a'}, self.sid)
        await self.wait(lambda: self.activation()>before)
        first = self.activation()
        for _ in range(10):
            await rpc('Input.dispatchKeyEvent', {'type':'keyDown', 'key':'b', 'text':'b'}, self.sid)
        self.assertEqual(self.activation(), first)
        await asyncio.sleep(1.1)
        await rpc('Input.insertText', {'text':'trusted-input'}, self.sid)
        await self.wait(lambda: self.activation()>first)
        before = self.activation()
        await asyncio.sleep(1.1)
        await rpc('Input.dispatchMouseEvent', {'type':'mouseWheel','x':10,'y':10,
                                              'deltaX':0,'deltaY':10}, self.sid)
        await self.wait(lambda: self.activation()>before)
        before = self.activation()
        await asyncio.sleep(1.1)
        await rpc('Input.dispatchMouseEvent', {'type':'mousePressed','x':10,'y':10,
                                              'button':'left','clickCount':1}, self.sid)
        await self.wait(lambda: self.activation()>before)
        await rpc('Input.dispatchMouseEvent', {'type':'mouseReleased','x':10,'y':10,
                                              'button':'left','clickCount':1}, self.sid)
        await asyncio.sleep(1.1)
        before = self.activation()
        await rpc('Runtime.evaluate', {'expression':
            "for (const type of ['pointerdown','keydown','input','wheel']) window.dispatchEvent(new Event(type))"}, self.sid)
        await asyncio.sleep(.1)
        self.assertEqual(self.activation(), before)
        self.assertEqual(self.service.store.rows()[0]['last_tool'], last_tool)

    async def test_existing_editable_iframe_input(self):
        import asyncio
        # The document must load with NO prior observer hook. Merely restarting
        # after an observed iframe was created leaves its old listeners alive.
        await self.service.stop()
        from urllib.parse import quote
        html="<iframe srcdoc='<input autofocus>'></iframe>"
        await self.client.call('Page.navigate', {'url':'data:text/html,'+quote(html)}, self.sid)
        await asyncio.sleep(.3)
        from tabwatch_plugin.service import Service
        self.service = Service(Path(self.tmp.name)/'state', self.service.upstream, 0)
        await self.service.start()
        await self.wait(lambda: self.service.store.healthy())
        await self.client.call('Runtime.evaluate', {'expression':
            "document.querySelector('iframe').contentDocument.querySelector('input').focus()"}, self.sid)
        await self.client.call('Target.activateTarget', {'targetId':self.tid})
        await asyncio.sleep(1.1)
        before = self.activation()
        tool = self.service.store.rows()[0]['last_tool']
        await self.client.call('Input.insertText', {'text':'existing-iframe'}, self.sid)
        await self.wait(lambda: self.activation()>before)
        self.assertEqual(self.service.store.rows()[0]['last_tool'], tool)
        result = await self.client.call('Runtime.evaluate', {'expression':
            "document.querySelector('iframe').contentDocument.querySelector('input').value",
            'returnByValue':True}, self.sid)
        self.assertEqual(result['result']['value'], 'existing-iframe')

    async def test_existing_cross_origin_iframe_and_proxy_attribution(self):
        import asyncio, json
        from tabwatch_plugin.service import RPC, Service
        import websockets
        async def http(reader, writer):
            try:
                request=await reader.readuntil(b'\r\n\r\n')
                body=(b'<input autofocus>' if b'GET /child ' in request else
                      f'<iframe src="http://localhost:{port}/child"></iframe>'.encode())
                writer.write(b'HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: '+
                             str(len(body)).encode()+b'\r\nConnection: close\r\n\r\n'+body)
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()
        server=await asyncio.start_server(http,'0.0.0.0',0)
        port=server.sockets[0].getsockname()[1]
        proxy=None
        try:
            await self.service.stop()
            await self.client.call('Page.navigate', {'url':f'http://127.0.0.1:{port}/'}, self.sid)
            await asyncio.sleep(1)
            self.service=Service(Path(self.tmp.name)/'state',self.service.upstream,0)
            await self.service.start()
            await self.wait(lambda: self.service.store.healthy())
            await self.wait(lambda: bool(self.service.child_sessions))
            self.assertTrue(all(s in self.service.covered_sessions for s in self.service.sessions))
            child=next(iter(self.service.child_sessions))
            self.assertEqual(self.service.sessions[child],self.tid)
            # Use the observer's exact OOPIF session only for setup/input; it
            # bypasses the proxy and cannot masquerade as last_tool activity.
            await self.service.rpc.call('Runtime.evaluate', {'expression':
                "document.querySelector('input').focus()"}, child)
            await self.client.call('Target.activateTarget', {'targetId':self.tid})
            await asyncio.sleep(1.1)
            before=self.activation()
            await self.service.rpc.call('Input.insertText',{'text':'cross-origin'},child)
            await self.wait(lambda: self.activation()>before)
            events=[]
            version=await self.service.version()
            endpoint=version['webSocketDebuggerUrl']
            from urllib.parse import urlsplit
            proxy=RPC(await websockets.connect(f'ws://127.0.0.1:{self.service.port}'+urlsplit(endpoint).path),events.append)
            parent=(await proxy.call('Target.attachToTarget',{'targetId':self.tid,'flatten':True}))['sessionId']
            await proxy.call('Target.setAutoAttach',{'autoAttach':True,'flatten':True,
                                                   'waitForDebuggerOnStart':False},parent)
            await self.wait(lambda:any(e.get('method')=='Target.attachedToTarget' and
                e.get('sessionId')==parent and e['params']['targetInfo']['type']=='iframe' for e in events))
            routed=next(e['params']['sessionId'] for e in events if
                e.get('method')=='Target.attachedToTarget' and e.get('sessionId')==parent and
                e['params']['targetInfo']['type']=='iframe')
            coverage=self.service.store.coverage_since
            before_tool=self.service.store.rows()[0]['last_tool']
            await proxy.call('Runtime.evaluate',{'expression':'42'},routed)
            self.assertGreater(self.service.store.rows()[0]['last_tool'],before_tool)
            self.assertEqual(self.service.store.coverage_since,coverage)
            # Attaching the same iframe independently has NO enclosing page
            # session receipt. Neither target lists nor observer maps may fill
            # that attribution gap for this client connection.
            orphan=(await proxy.call('Target.attachToTarget', {
                'targetId':self.service.session_targets[child],'flatten':True}))['sessionId']
            before_tool=self.service.store.rows()[0]['last_tool']
            await proxy.call('Runtime.evaluate',{'expression':'43'},orphan)
            self.assertGreater(self.service.store.coverage_since,coverage)
            self.assertEqual(self.service.store.rows()[0]['last_tool'],before_tool)
            await self.wait(lambda: self.service.store.healthy())
            coverage=self.service.store.coverage_since
            # Normal OOPIF teardown must not restart coverage or page grace.
            grace=dict(self.service.store.observer_grace)
            observer_root=next(s for s,t in self.service.sessions.items()
                               if t == self.tid and s not in self.service.child_sessions)
            await self.client.call('Runtime.evaluate',{'expression':
                "document.querySelector('iframe').remove()"},self.sid)
            await self.wait(lambda: not self.service.child_sessions)
            await self.wait(lambda: self.service.store.healthy())
            self.assertEqual(self.service.store.coverage_since,coverage)
            self.assertEqual(self.service.store.observer_grace,grace)
            self.assertIn(observer_root,self.service.sessions)
        finally:
            if proxy: await proxy.close()
            server.close(); await server.wait_closed()

    async def test_root_navigation_fences_then_reinstalls_exact_session(self):
        service = self.service
        old_sid = next(s for s,t in service.sessions.items() if t==self.tid)
        coverage = service.store.coverage_since
        root = service.root_contexts[old_sid]
        observations = []
        original = service.observer_event
        def capture(msg):
            original(msg)
            if (msg.get('sessionId')==old_sid and
                (msg.get('method')=='Runtime.executionContextsCleared' or
                 (msg.get('method')=='Runtime.executionContextDestroyed' and
                  msg.get('params',{}).get('executionContextId')==root))):
                observations.append((service.store.healthy(), self.tid in service.installed))
        service.rpc.event = capture
        await self.client.call('Page.navigate', {'url':'data:text/html,<input autofocus>'}, self.sid)
        await self.wait(lambda: bool(observations))
        self.assertTrue(all(pair==(False,False) for pair in observations), observations)
        await self.wait(lambda: service.store.healthy() and self.tid in service.installed)
        self.assertNotIn(old_sid, service.sessions)
        self.assertNotIn(old_sid, service.root_contexts)
        self.assertFalse(any(c[0]==old_sid for c in service.contexts))
        self.assertEqual(service.store.coverage_since, coverage)
        self.assertGreater(service.store.observer_grace[self.tid], coverage)
        self.assertEqual(len([t for t in service.sessions.values() if t==self.tid]), 1)
        await self.client.call('Target.activateTarget', {'targetId':self.tid})
        before = self.activation()
        await self.client.call('Input.insertText', {'text':'after-navigation'}, self.sid)
        await self.wait(lambda: self.activation()>before)

if __name__ == '__main__': unittest.main()
