"""Loopback CDP activity proxy and fail-closed tab watchdog (websockets 15)."""
import asyncio
import contextlib
import json
import logging
import os
import time
import uuid
import urllib.request
from http import HTTPStatus
from pathlib import Path
from dataclasses import asdict
from urllib.parse import urlsplit

from websockets.asyncio.client import connect
from websockets.asyncio.server import serve
from websockets.datastructures import Headers
from websockets.http11 import Response
from .store import Store, page_target
from .control import fence_cleanup, socket_address, gap_reason

WORLD = '__tabwatch_focus_v1'
BINDING = '__tabwatch_activation_v1'
FOCUS_SCRIPT = '''(() => {
 if (globalThis.__tabwatchInstalled) return;
 globalThis.__tabwatchInstalled = true;
 let lastActivity = -Infinity;
 const report = (e) => {
   if (e && !e.isTrusted) return;
   if (document.visibilityState === 'visible' && document.hasFocus())
     globalThis.__tabwatch_activation_v1('active');
 };
 window.addEventListener('focus', report, true);
 document.addEventListener('visibilitychange', report, true);
 const activity = (e) => {
   if (!e.isTrusted || performance.now() - lastActivity < 1000) return;
   if (document.visibilityState !== 'visible' || !document.hasFocus()) return;
   lastActivity = performance.now();
   report(e);
 };
 for (const type of ['pointerdown', 'keydown', 'input', 'wheel'])
   window.addEventListener(type, activity, {capture: true, passive: true});
 report(null);
})();'''


async def fetch_json(url):
    """Cancellable loopback CDP HTTP with a total deadline, not a socket-idle timer."""
    async def fetch():
        parts = urlsplit(url)
        reader, writer = await asyncio.open_connection(parts.hostname, parts.port or 80)
        try:
            path = parts.path or '/'
            writer.write(f'GET {path} HTTP/1.1\r\nHost: {parts.netloc}\r\nConnection: close\r\n\r\n'.encode())
            await writer.drain()
            head = await reader.readuntil(b'\r\n\r\n')
            lines = head.split(b'\r\n')
            if lines[0].split()[1] != b'200':
                raise ConnectionError('cdp_discovery_http_error')
            headers = dict(line.split(b':', 1) for line in lines[1:] if b':' in line)
            headers = {key.lower(): value.strip() for key, value in headers.items()}
            if b'transfer-encoding' in headers:
                raise ValueError('unsupported_discovery_encoding')
            limit = 8 * 1024 * 1024
            if b'content-length' in headers:
                length = int(headers[b'content-length'])
                if not 0 <= length <= limit:
                    raise ValueError('discovery_response_too_large')
                body = await reader.readexactly(length)
            else:
                body = bytearray()
                while True:
                    chunk = await reader.read(65536)
                    if not chunk:
                        break
                    body.extend(chunk)
                    if len(body) > limit:
                        raise ValueError('discovery_response_too_large')
            return json.loads(body)
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(writer.wait_closed(), .25)
    async with asyncio.timeout(5):
        return await fetch()


class RPC:
    def __init__(self, ws, event):
        self.ws = ws
        self.event = event
        self.seq = 0
        self.waiters = {}
        self.task = asyncio.create_task(self.read())

    async def read(self):
        try:
            async for raw in self.ws:
                msg = json.loads(raw)
                if 'id' in msg:
                    fut = self.waiters.pop(msg['id'], None)
                    if fut and not fut.done():
                        if 'error' in msg:
                            fut.set_exception(RuntimeError('cdp_error'))
                        else:
                            fut.set_result(msg.get('result', {}))
                else:
                    self.event(msg)
        finally:
            self.event({'method':'__tabwatch.disconnected'})
            for fut in self.waiters.values():
                if not fut.done(): fut.set_exception(ConnectionError('cdp_disconnected'))
            self.waiters.clear()

    async def call(self, method, params=None, session=None):
        self.seq += 1
        ident = self.seq
        future = asyncio.get_running_loop().create_future()
        self.waiters[ident] = future
        msg = {'id':ident, 'method':method, 'params':params or {}}
        if session: msg['sessionId'] = session
        try:
            await self.ws.send(json.dumps(msg))
            return await asyncio.wait_for(future, 8)
        except Exception as exc:
            setattr(exc, 'cdp_method', method)
            raise
        finally:
            self.waiters.pop(ident, None)

    async def close(self):
        await self.ws.close()
        await asyncio.gather(self.task, return_exceptions=True)


SETUP_COMMANDS = {'enable','disable','getIsolateId','setDiscoverTargets','setAutoAttach',
                  'attachToTarget','detachFromTarget','setLifecycleEventsEnabled',
                  'runIfWaitingForDebugger','addBinding','removeBinding',
                  'addScriptToEvaluateOnNewDocument','removeScriptToEvaluateOnNewDocument',
                  'createIsolatedWorld'}


def activity(method, target_scoped=False):
    if not isinstance(method, str): return False
    domain, _, name = method.partition('.')
    if name in SETUP_COMMANDS:
        return False
    if method in {'Target.activateTarget','Target.closeTarget','Target.sendMessageToTarget'}:
        return True
    # Unknown page domains are conservatively activity, not silently ignored.
    # If no exact session/target route exists, outgoing() resets coverage grace.
    return target_scoped or domain not in {
        'Browser','Target','SystemInfo','Schema','Tracing','ServiceWorker','Tethering'}


class Service:
    def __init__(self, state_dir, upstream='http://127.0.0.1:9222', port=9223, policy=None,
                 host='127.0.0.1', cleanup_error=None):
        parts = urlsplit(upstream)
        if parts.scheme != 'http' or parts.hostname not in {'localhost','127.0.0.1'}:
            raise ValueError('upstream_must_be_loopback_http')
        self.upstream = upstream.rstrip('/')
        self.port = port
        self.host = host
        self.cleanup_error = cleanup_error
        self.incarnation = uuid.uuid4().hex
        self.policy = policy
        self.store = None
        # Elect BEFORE opening SQLite, generating keys, chmod or touching sockets.
        import fcntl
        state = Path(state_dir)
        state.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.lock = open(state / 'service.lock', 'a')
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            self.lock.close()
            raise
        try:
            self.store = Store(state, policy=policy)
            self.store.disabled = cleanup_error is not None
        except Exception:
            self.cleanup_error = 'ledger_unavailable'
        # A plugin-local logger: no global logging mutations and no raw frames.
        self.logger = logging.Logger('tabwatch.transport', level=logging.CRITICAL + 1)
        self.logger.addHandler(logging.NullHandler())
        self.logger.propagate = False
        self.server = None
        self.control = None
        self.observer_task = None
        self.rpc = None
        self.installed = set()
        self.install_errors = {}
        self.observer_error = None
        self.observer_disconnected = True
        self.sessions = {}
        self.contexts = set()
        self.root_contexts = {}
        self.scripts = {}
        self.session_targets = {}
        self.child_sessions = set()
        self.session_installs = {}
        self.covered_sessions = set()
        self.removed_frames = set()
        self.detached_frames = set()
        self.inventory_generation = 0
        self.inventory_hashes = {}
        self.sweep_lock = asyncio.Lock()
        self.tasks = set()
        self.stopping = False
        self.socket_path = state / 'control.sock'
        self.socket_owned = False

    async def start(self):
        try:
            self.server = await serve(self.proxy, self.host, self.port,
                                      process_request=self.http, max_size=None,
                                      compression=None, close_timeout=2, logger=self.logger)
            self.port = self.server.sockets[0].getsockname()[1]
            self.socket_path.unlink(missing_ok=True)
            with socket_address(self.socket_path) as address:
                self.control = await asyncio.start_unix_server(self.control_client, path=address, limit=8192)
            self.socket_owned = True
            os.chmod(self.socket_path, 0o600)
            if self.store is not None:
                self.observer_task = asyncio.create_task(self.observe_forever())
        except BaseException:
            await self.stop()
            raise

    async def stop(self):
        if self.stopping:
            return
        self.stopping = True
        self.cleanup_error = 'unloading'
        if self.store:
            self.store.disabled = True
        try:
            tasks = list(self.tasks)
            if self.observer_task:
                tasks.append(self.observer_task)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if self.rpc:
                with contextlib.suppress(Exception):
                    await self.rpc.close()
            if self.server:
                self.server.close()
                await self.server.wait_closed()
            if self.control:
                self.control.close()
                await self.control.wait_closed()
        finally:
            try:
                if self.socket_owned:
                    self.socket_path.unlink(missing_ok=True)
                    self.socket_owned = False
                if self.store:
                    self.store.close()
            finally:
                self.lock.close()

    def disable_cleanup(self, reason, *, cause=None):
        # Latch first, before fallible persistence. No exception text or payloads.
        self.cleanup_error = reason
        if self.store is not None:
            self.store.disabled = True
        with contextlib.suppress(Exception):
            fence_cleanup(self.socket_path.parent, reason=cause or reason)

    def safe_gap(self):
        try:
            self.store.gap('observer_error')
        except Exception:
            self.disable_cleanup('ledger_failure')

    async def http(self, conn, request):
        if request.headers.get_all('Origin'):
            return Response(403,'Forbidden', Headers(), b'Forbidden')
        path = request.path.split('?',1)[0]
        if path.startswith('/devtools/') and request.headers.get('Upgrade','').lower() == 'websocket':
            return None
        if path not in {'/json','/json/','/json/list','/json/version','/json/protocol'}:
            return Response(404,'Not Found',Headers(),b'Not Found')
        try:
            data = await fetch_json(self.upstream+path)
            def rewrite(obj):
                if isinstance(obj,dict):
                    return {k: ('ws://'+self.host+':'+str(self.port)+urlsplit(v).path
                                if k == 'webSocketDebuggerUrl' and isinstance(v,str)
                                else rewrite(v)) for k,v in obj.items()}
                if isinstance(obj,list): return [rewrite(x) for x in obj]
                return obj
            body = json.dumps(rewrite(data)).encode()
            return Response(200,'OK',Headers({'Content-Type':'application/json','Content-Length':str(len(body))}),body)
        except Exception:
            return Response(503,'Unavailable',Headers(),b'Unavailable')

    async def proxy(self, client):
        path = client.request.path
        if not (path.startswith('/devtools/browser/') or path.startswith('/devtools/page/')):
            await client.close(1008, 'invalid_path'); return
        direct = path.split('/devtools/page/',1)[1].split('?',1)[0] if '/devtools/page/' in path else None
        mapping = {}
        requests = {}
        tracked = {}
        upstream_ws = 'ws://' + urlsplit(self.upstream).netloc + path
        def finish(key, method=None):
            tracked_command = tracked.pop(key, None)
            if tracked_command:
                tid,touched=tracked_command
                if tid == '*': self.store.gap('unattributed_completion', method)
                elif touched: self.store.touch(tid,'tool')
                self.store.pending[tid] = max(0,self.store.pending.get(tid,0)-1)
        async def outgoing(up):
            async for raw in client:
                try:
                    msg = json.loads(raw)
                    method=msg.get('method'); params=msg.get('params',{})
                    key=(msg.get('sessionId'),msg.get('id'))
                    if 'id' in msg:
                        if key in requests or len(requests)>=10000:
                            raise RuntimeError('invalid_command_sequence')
                        requests[key]=(method,params.get('targetId'),
                                       params.get('url') if method == 'Target.createTarget' else None)
                    active=activity(method, bool(direct or msg.get('sessionId')))
                    tid = (params.get('targetId') if isinstance(method,str) and method.startswith('Target.') else None) or mapping.get(msg.get('sessionId')) or (direct if not msg.get('sessionId') else None)
                    known=tid and any(r['target']==tid for r in self.store.rows())
                    if active:
                        self.store.command_count += 1
                        if method == 'Target.sendMessageToTarget' or not known:
                            self.store.gap('unattributed_command', method)
                            if 'id' in msg:
                                tracked[key]=('*',False)
                                self.store.pending['*']=self.store.pending.get('*',0)+1
                        elif 'id' in msg:
                            tracked[key]=(tid,True)
                            self.store.pending[tid]=self.store.pending.get(tid,0)+1
                            self.store.touch(tid,'tool')
                        else: self.store.gap('unacknowledged_notification', method)
                    elif known and 'id' in msg:
                        tracked[key]=(tid,False)
                        self.store.pending[tid]=self.store.pending.get(tid,0)+1
                except Exception:
                    self.disable_cleanup('outgoing_telemetry_failure')
                await up.send(raw)
        async def incoming(up):
            async for raw in up:
                try:
                    msg=json.loads(raw)
                    if msg.get('method') == 'Target.attachedToTarget':
                        p=msg['params']; info=p['targetInfo']
                        # An iframe target ID is not a page. Only the enclosing
                        # attachedToTarget session proves its owning page.
                        owner = mapping.get(msg.get('sessionId')) or (direct if not msg.get('sessionId') else None)
                        mapping[p['sessionId']] = (owner if info.get('type') == 'iframe'
                                                   else info['targetId'])
                    if msg.get('method') == 'Target.detachedFromTarget':
                        mapping.pop(msg['params']['sessionId'],None)
                    if 'id' in msg:
                        key=(msg.get('sessionId'),msg['id'])
                        method,tid,url=requests.pop(key,(None,None,None))
                        if method == 'Target.createTarget' and 'result' in msg:
                            created=msg['result'].get('targetId')
                            if created and isinstance(url,str):
                                self.admit_new_page({'targetId':created,'type':'page','url':url})
                        if method == 'Target.attachToTarget' and 'result' in msg:
                            mapping.setdefault(msg['result']['sessionId'],tid)
                        finish(key, method)
                except Exception:
                    self.disable_cleanup('incoming_telemetry_failure')
                await client.send(raw)
        try:
            async with connect(upstream_ws,max_size=None,compression=None,
                               close_timeout=2, logger=self.logger) as up:
                tasks=[asyncio.create_task(outgoing(up)),asyncio.create_task(incoming(up))]
                try:
                    done,_=await asyncio.wait(tasks,return_when=asyncio.FIRST_COMPLETED)
                    for task in done: task.result()
                finally:
                    for task in tasks: task.cancel()
                    await asyncio.gather(*tasks,return_exceptions=True)
        except Exception:
            # The independent observer owns coverage. Each client command was
            # attributed before forwarding; a client transport failure alone is
            # not a browser-wide observation gap. Finalize uncertain commands
            # below: known targets are touched, unknown scope still gaps all.
            pass
        finally:
            for key in list(tracked):
                try:
                    finish(key, requests.get(key,(None,))[0])
                except Exception:
                    self.disable_cleanup('completion_telemetry_failure')
            await client.close()

    def observer_event(self, msg):
        try:
            self._observer_event(msg)
        except Exception:
            self.disable_cleanup('observer_telemetry_failure')

    def _observer_event(self, msg):
        method=msg.get('method'); p=msg.get('params',{}); sid=msg.get('sessionId')
        # Retired sessions can still have queued receipts. They no longer own
        # contexts, frames, child installs, or readiness.
        if sid is not None and sid not in self.sessions:
            return
        if method == '__tabwatch.disconnected':
            self.observer_disconnected = True
            self.store.gap('observer_disconnect')
        elif method == 'Target.attachedToTarget':
            child=p.get('sessionId'); info=p.get('targetInfo',{})
            if sid in self.sessions and info.get('type') == 'iframe':
                tid=self.sessions[sid]
                self.sessions[child]=tid
                self.session_targets[child]=info['targetId']
                self.child_sessions.add(child)
                self.store.observer_missing.add(tid)
                self.store.ready=False
                task=asyncio.create_task(self.install_session(child))
                self.session_installs[child]=task
                self.tasks.add(task)
                def finished(task):
                    self.tasks.discard(task)
                    if (self.session_installs.get(child) is task and child in self.sessions
                            and not task.cancelled() and task.exception() is not None):
                        self.invalidate_observer(child)
                task.add_done_callback(finished)
        elif method == 'Runtime.executionContextCreated':
            ctx=p['context']
            if ctx.get('name') == WORLD: self.contexts.add((sid,ctx['id']))
        elif method == 'Runtime.executionContextDestroyed':
            context = (sid,p.get('executionContextId'))
            self.contexts.discard(context)
            if self.root_contexts.get(sid) == context[1]:
                self.invalidate_observer(sid)
        elif method == 'Runtime.executionContextsCleared':
            self.contexts={c for c in self.contexts if c[0]!=sid}
            if sid in self.root_contexts:
                self.invalidate_observer(sid)
        elif method == 'Runtime.bindingCalled' and p.get('name') == BINDING:
            if (sid,p.get('executionContextId')) in self.contexts and p.get('payload') == 'active':
                tid=self.sessions.get(sid)
                if tid: self.store.touch(tid,'activation')
        elif method == 'Page.frameDetached' and p.get('reason') == 'remove':
            tid=self.sessions.get(sid)
            frame=p.get('frameId')
            key=(tid,frame)
            if key in self.detached_frames:
                self.detached_frames.discard(key)
            elif any(t == frame and self.sessions.get(s) == tid
                     for s,t in self.session_targets.items() if s in self.child_sessions):
                self.removed_frames.add(key)
        elif method == 'Target.detachedFromTarget':
            detached = p.get('sessionId')
            if detached not in self.sessions:
                return
            if detached in self.child_sessions:
                tid=self.sessions.get(detached)
                key=(tid,self.session_targets.get(detached))
                if key not in self.removed_frames:
                    self.detached_frames.add(key)
                    self.store.observer_missing.add(tid)
                    self.store.ready=False
                    self.inventory_generation += 1
                self.removed_frames.discard(key)
                self.root_contexts.pop(detached,None)
                self.contexts={c for c in self.contexts if c[0] != detached}
            else:
                self.invalidate_observer(detached)
            self.sessions.pop(detached,None)
            self.scripts.pop(detached,None)
            self.session_targets.pop(detached,None)
            self.covered_sessions.discard(detached)
            self.child_sessions.discard(detached)
            task=self.session_installs.pop(detached,None)
            if task and not task.done(): task.cancel()
        elif method in {'Target.targetCreated','Target.targetInfoChanged','Target.targetDestroyed'}:
            info=p.get('targetInfo',{})
            tid=p.get('targetId') or info.get('targetId')
            old=self.inventory_hashes.get(tid)
            if method == 'Target.targetCreated' and page_target(info):
                self.admit_new_page(info)
            new=(self.store.digest(info.get('url',''))
                 if method != 'Target.targetDestroyed' and page_target(info) else None)
            if new != old:
                if new is None:
                    self.inventory_hashes.pop(tid,None)
                else:
                    self.inventory_hashes[tid]=new
                self.store.ready=False
                self.inventory_generation += 1

    def admit_new_page(self, info):
        """Native creation receipts admit only missing pages, never stale URLs."""
        if page_target(info) and not any(r['target'] == info['targetId'] for r in self.store.rows()):
            self.store.observe_target(info)
            self.store.observer_missing.add(info['targetId'])
            self.store.ready=False

    async def retire_sessions(self, tids):
        """Forget exact observer sessions before awaiting their cleanup receipts."""
        retired={sid:self.scripts.pop(sid,None) for sid,tid in list(self.sessions.items()) if tid in tids}
        pending=[]
        for sid in retired:
            self.sessions.pop(sid,None)
            self.covered_sessions.discard(sid)
            self.root_contexts.pop(sid,None)
            self.session_targets.pop(sid,None)
            self.child_sessions.discard(sid)
            task=self.session_installs.pop(sid,None)
            if task and not task.done():
                task.cancel()
                pending.append(task)
        self.contexts={c for c in self.contexts if c[0] not in retired}
        self.removed_frames={k for k in self.removed_frames if k[0] not in tids}
        self.detached_frames={k for k in self.detached_frames if k[0] not in tids}
        self.installed.difference_update(tids)
        await asyncio.gather(*pending,return_exceptions=True)
        for sid,script in retired.items():
            if script is not None:
                with contextlib.suppress(Exception):
                    await self.rpc.call('Page.removeScriptToEvaluateOnNewDocument',{'identifier':script},sid)
            with contextlib.suppress(Exception):
                await self.rpc.call('Target.detachFromTarget',{'sessionId':sid})

    def invalidate_observer(self, sid):
        tid = self.sessions.get(sid)
        self.root_contexts.pop(sid, None)
        self.covered_sessions.discard(sid)
        self.contexts = {c for c in self.contexts if c[0] != sid}
        if tid is not None:
            if sid not in self.child_sessions:
                self.installed.discard(tid)
            self.store.observer_missing.add(tid)
            if sid not in self.child_sessions:
                self.store.observer_grace[tid] = self.store.clock()
            self.store.ready = False
            self.inventory_generation += 1

    async def install(self, target):
        tid=target['targetId']
        initial = tid not in self.store.observer_grace
        self.store.observer_missing.add(tid)
        self.detached_frames={k for k in self.detached_frames if k[0] != tid}
        self.removed_frames={k for k in self.removed_frames if k[0] != tid}
        # Remove attribution before detach events so retiring old observers does
        # not manufacture navigation grace (especially for churning ad frames).
        await self.retire_sessions({tid})
        result=await self.rpc.call('Target.attachToTarget',{'targetId':tid,'flatten':True})
        sid=result['sessionId']; self.sessions[sid]=tid
        self.session_targets[sid]=tid
        await self.install_session(sid)
        if (any(s not in self.covered_sessions for s,t in self.sessions.items() if t == tid)
                or any(k[0] == tid for k in self.detached_frames)):
            raise RuntimeError('observer_install_failed')
        if initial: self.store.observer_grace[tid]=self.store.clock()
        self.installed.add(tid)
        self.store.observer_missing.discard(tid)

    async def install_session(self, sid):
        await self.rpc.call('Runtime.enable',session=sid)
        await self.rpc.call('Page.enable',session=sid)
        await self.rpc.call('Runtime.addBinding',{'name':BINDING,'executionContextName':WORLD},sid)
        script=await self.rpc.call('Page.addScriptToEvaluateOnNewDocument',{'source':FOCUS_SCRIPT,'worldName':WORLD},sid)
        if sid not in self.sessions:
            raise RuntimeError('observer_session_retired')
        self.scripts[sid]=script['identifier']
        # Auto-attach is session-scoped; recurse for nested OOPIFs. Events carry
        # the authoritative parent session, never infer ownership from URLs.
        await self.rpc.call('Target.setAutoAttach', {'autoAttach':True,
                            'waitForDebuggerOnStart':False,'flatten':True,
                            'filter':[{'type':'iframe'}]}, sid)
        tree=(await self.rpc.call('Page.getFrameTree',session=sid))['frameTree']
        def frames(node):
            yield node['frame']['id']
            for child in node.get('childFrames',[]): yield from frames(child)
        for frame in frames(tree):
            # OOPIF documents belong to their own attached session.
            if any(s != sid and t == frame for s,t in self.session_targets.items()):
                continue
            world=await self.rpc.call('Page.createIsolatedWorld',{'frameId':frame,'worldName':WORLD},sid)
            if sid not in self.sessions:
                raise RuntimeError('observer_session_retired')
            ctx=world['executionContextId']
            if frame == tree['frame']['id']: self.root_contexts[sid]=ctx
            self.contexts.add((sid,ctx))
            evaluated=await self.rpc.call('Runtime.evaluate',{'expression':FOCUS_SCRIPT,'contextId':ctx},sid)
            if 'exceptionDetails' in evaluated or (sid,ctx) not in self.contexts:
                raise RuntimeError('observer_install_failed')
        # setAutoAttach receipts precede its response, so existing child installs
        # are now known. The page root joins their recursively discovered tasks.
        while True:
            children=[task for s,task in self.session_installs.items()
                      if self.sessions.get(s) == self.sessions.get(sid)
                      and s != sid and not task.done()]
            # Avoid siblings awaiting one another; only the root joins all tasks.
            if sid in self.child_sessions or not children: break
            await asyncio.gather(*children, return_exceptions=True)
        if sid not in self.sessions or sid not in self.root_contexts:
            raise RuntimeError('observer_install_failed')
        self.covered_sessions.add(sid)

    async def version(self):
        return await fetch_json(self.upstream+'/json/version')

    async def observe_forever(self):
        while True:
            stage = 'discovery'
            try:
                version=await self.version()
                endpoint=version['webSocketDebuggerUrl']
                epoch=urlsplit(endpoint).path.rsplit('/',1)[-1]
                self.store.coverage(epoch)
                for task in list(self.session_installs.values()): task.cancel()
                await asyncio.gather(*self.session_installs.values(), return_exceptions=True)
                self.session_installs.clear(); self.covered_sessions.clear()
                self.session_targets.clear(); self.child_sessions.clear()
                self.removed_frames.clear(); self.detached_frames.clear()
                self.installed.clear(); self.sessions.clear(); self.contexts.clear()
                self.root_contexts.clear(); self.scripts.clear()
                self.inventory_hashes.clear()
                stage = 'connect'
                async with connect(endpoint,max_size=None,compression=None,
                                   close_timeout=2, logger=self.logger) as ws:
                    self.observer_disconnected = False
                    self.rpc=RPC(ws,self.observer_event)
                    await self.rpc.call('Target.setDiscoverTargets',{'discover':True})
                    previous=time.monotonic()
                    previous_wall=time.time()
                    while True:
                        stage = 'inventory'
                        current=time.monotonic()
                        wall=time.time()
                        if current-previous > 20 or abs((wall-previous_wall)-(current-previous)) > 5:
                            self.store.gap('observation_clock_gap')
                        previous=current
                        previous_wall=wall
                        gap_generation = self.store.gap_count
                        targets=(await self.rpc.call('Target.getTargets'))['targetInfos']
                        self.store.observe(targets)
                        ids={t['targetId'] for t in targets if page_target(t)}
                        hashes={t['targetId']:self.store.digest(t.get('url',''))
                                for t in targets if page_target(t)}
                        self.inventory_hashes=hashes.copy()
                        await self.retire_sessions(set(self.sessions.values()) - ids)
                        self.install_errors={t:e for t,e in self.install_errors.items() if t in ids}
                        self.installed.intersection_update(ids)
                        # A proven removal needs no reattachment. An unexplained
                        # detach or a lost surviving child context does: fence
                        # this page until a full fresh installation succeeds.
                        for tid in ids & self.installed:
                            uncovered=any(s not in self.covered_sessions and
                                (s not in self.session_installs or self.session_installs[s].done())
                                for s,t in self.sessions.items() if t == tid)
                            if uncovered or any(k[0] == tid for k in self.detached_frames):
                                self.installed.discard(tid)
                        for target in targets:
                            if page_target(target) and target['targetId'] not in self.installed:
                                try:
                                    await self.install(target)
                                    self.install_errors.pop(target['targetId'], None)
                                except Exception as exc:
                                    self.install_errors[target['targetId']] = {'kind':type(exc).__name__, 'method':getattr(exc,'cdp_method',None)}
                                    # A navigating/removed subframe can race the
                                    # snapshot. Retain the page, retry coverage;
                                    # do not restart browser-wide idle grace.
                                    self.installed.discard(target['targetId'])
                                    self.store.observer_missing.add(target['targetId'])
                        for tid in ids & self.installed:
                            if (all(s in self.covered_sessions for s,t in self.sessions.items() if t == tid)
                                    and not any(k[0] == tid for k in self.detached_frames)):
                                self.store.observer_missing.discard(tid)
                        # Slow installs can discover legitimate child coverage.
                        # Validate a fresh inventory AFTER those installs, rather
                        # than demanding a quiet browser throughout the batch.
                        generation=self.inventory_generation
                        stage = 'validation'
                        fresh=(await self.rpc.call('Target.getTargets'))['targetInfos']
                        fresh_hashes={t['targetId']:self.store.digest(t.get('url',''))
                                      for t in fresh if page_target(t)}
                        if (not self.observer_disconnected and gap_generation == self.store.gap_count
                                and generation == self.inventory_generation and hashes == fresh_hashes
                                and ids <= self.installed and not self.store.observer_missing
                                and all(s in self.covered_sessions and s in self.root_contexts
                                        for s in self.sessions)
                                and all(any(t == tid and s not in self.child_sessions
                                            for s,t in self.sessions.items()) for tid in ids)):
                            self.store.heartbeat()
                            self.observer_error = None
                        else:
                            self.store.ready=False
                        await asyncio.sleep(2)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Never expose raw exception text: discovery errors contain URLs.
                error = {'kind':type(exc).__name__, 'method':getattr(exc,'cdp_method',None),
                         'stage':stage}
                cause = getattr(exc, 'reason', None)
                if isinstance(cause, BaseException):
                    error['cause_kind'] = type(cause).__name__
                number = getattr(cause if isinstance(cause, BaseException) else exc, 'errno', None)
                if type(number) is int:
                    error['errno'] = number
                status = getattr(exc, 'code', None)
                if type(status) is int and 100 <= status <= 599:
                    error['http_status'] = status
                self.observer_error = error
                self.safe_gap()
                if self.rpc:
                    with contextlib.suppress(Exception):
                        await self.rpc.close()
                self.rpc=None
                await asyncio.sleep(2)

    async def sweep(self, apply=False):
        async with self.sweep_lock:
            result={'candidates':[], 'closed':[], 'errors':[]}
            if self.cleanup_error or not self.store or not self.store.healthy() or not self.rpc:
                result['errors'].append({'reason':'cleanup_disabled'})
                return result
            planned=self.store.candidates()[:5]
            result['candidates']=[{'target_id':tid,'reason':reason} for tid,reason in planned]
            if not apply: return result
            for tid,reason in planned:
                try:
                    snapshot=next((r for r in self.store.rows() if r['target']==tid),None)
                    if snapshot is None: continue
                    version=await self.version()
                    if urlsplit(version['webSocketDebuggerUrl']).path.rsplit('/',1)[-1] != self.store.epoch:
                        self.store.gap('browser_epoch_changed'); break
                    targets=(await self.rpc.call('Target.getTargets'))['targetInfos']
                    target=next((t for t in targets if t['targetId']==tid),None)
                    if not target or not page_target(target): continue
                    if self.store.digest(target.get('url','')) != snapshot['url_hash']:
                        self.store.observe(targets); continue
                    # Recompute every duplicate group, not just this target's URL:
                    # its previous winner may have navigated/disappeared meanwhile.
                    self.store.observe(targets)
                    latest=next((r for r in self.store.rows() if r['target']==tid),None)
                    fields=('epoch','url_hash','revision','last_tool','last_activation',
                            'duplicate_since','duplicate_winner')
                    if not latest or any(latest[k]!=snapshot[k] for k in fields): continue
                    if (tid,reason) not in self.store.candidates(): continue
                    pages=[t for t in targets if page_target(t)]
                    if len(pages) <= 1: continue
                    # Retain a neutral survivor even while mission pages remain open.
                    if target.get('url') == 'about:blank' and sum(
                            t.get('url') == 'about:blank' for t in pages) <= 1: continue
                    # No awaits between final activity/fence check and sending the close.
                    closed=await self.rpc.call('Target.closeTarget',{'targetId':tid})
                    if not closed.get('success'): raise RuntimeError('close_refused')
                    disappeared=False
                    for _ in range(10):
                        remaining=(await self.rpc.call('Target.getTargets'))['targetInfos']
                        if not any(t['targetId']==tid for t in remaining):
                            disappeared=True; break
                        await asyncio.sleep(.1)
                    if not disappeared: raise RuntimeError('close_unverified')
                    result['closed'].append(tid)
                    self.store.observe(remaining)
                    if tid != planned[-1][0]:
                        # Our own close invalidates inventory. Wait for the real
                        # observer to revalidate it; never manufacture readiness.
                        for _ in range(50):
                            if self.store.healthy():
                                break
                            await asyncio.sleep(.1)
                        if not self.store.healthy():
                            break
                except Exception:
                    result['errors'].append({'target_id':tid,'reason':'close_failed'})
                    self.disable_cleanup('cleanup_failure'); break
            return result

    def health(self):
        base = {'healthy':False, 'cleanup_error':self.cleanup_error, 'runtime_pid':os.getpid(),
                'proxy_port':self.port, 'policy':None,
                'runtime_incarnation':self.incarnation,
                'cleanup_gap_reason':gap_reason(self.socket_path.parent),
                'configuration_signature':getattr(self, 'configuration_signature', None)}
        if self.store is None:
            return base
        try:
            health = self._health()
            base['cleanup_error'] = self.cleanup_error
            return dict(health, **{key:value for key,value in base.items()
                        if key not in {'healthy', 'policy'}})
        except Exception:
            self.disable_cleanup('health_telemetry_failure')
            base['cleanup_error'] = self.cleanup_error
            return base

    def _health(self):
        healthy = self.store.healthy()
        if self.store.disabled and not self.cleanup_error:
            self.cleanup_error = 'untracked_activity_gap'
        return {'healthy':healthy and not self.cleanup_error,
                'policy':asdict(self.store.policy) if self.cleanup_error != 'invalid_settings' else None,
                'observer_freshness':max(0,self.store.clock()-self.store.fresh) if self.store.fresh else None,
                'observer_missing': sorted(self.store.observer_missing),
                'installed_count':len(self.installed), 'install_errors':self.install_errors,
                'observer_error':self.observer_error,
                'last_healthy_at':self.store.last_healthy_at, 'gap_count':self.store.gap_count,
                'recent_gaps':list(self.store.gap_events),
                'active_leases':sum(v>self.store.clock() for v in self.store.leases.values()),
                'browser_epoch':self.store.epoch,'page_count':len(self.store.rows()),
                'observed_command_count':self.store.command_count,
                'coverage_since':self.store.coverage_since,
                'targets':[{'target_id':r['target'],'first_seen':r['first_seen'],
                            'last_tool':r['last_tool'],'last_activation':r['last_activation'],
                            'pending_commands':self.store.pending.get(r['target'],0)}
                           for r in sorted(self.store.rows(),key=lambda r:r['target'])]}

    async def control_client(self, reader, writer):
        task = asyncio.current_task()
        self.tasks.add(task)
        command = None
        try:
            line=await asyncio.wait_for(reader.readline(),5)
            msg=json.loads(line)
            command=msg.get('command',msg.get('cmd'))
            if command=='health': result=self.health()
            elif command == 'fence':
                self.disable_cleanup('untracked_activity_gap', cause=msg.get('cause'))
                result={'ok':True}
            elif command in {'lease_start','lease_end'}:
                ident=msg['call_id']
                if not isinstance(ident,str) or not 0<len(ident)<=256: raise ValueError('bad_call_id')
                if command=='lease_start': self.store.lease_start(ident,msg.get('ttl',1900))
                else: self.store.lease_end(ident)
                result={'ok':True}
            elif command=='sweep':
                apply=msg.get('apply',False)
                if not isinstance(apply,bool): raise ValueError('bad_apply')
                result=await self.sweep(apply)
            else: result={'error':'unknown_command'}
            writer.write(json.dumps(result).encode()+b'\n')
            await writer.drain()
        except Exception:
            # A disconnected reader of a parsed health request cannot lose tool
            # activity. Health telemetry faults already latch inside health().
            if command != 'health':
                self.disable_cleanup('control_failure')
            with contextlib.suppress(Exception):
                writer.write(b'{"error":"invalid_request"}\n'); await writer.drain()
        finally:
            writer.close()
            with contextlib.suppress(Exception): await writer.wait_closed()
            self.tasks.discard(task)
