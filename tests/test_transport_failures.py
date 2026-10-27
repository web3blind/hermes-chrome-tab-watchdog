import asyncio
import json
import sqlite3

import pytest
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve

import tabwatch_plugin.service as module
from tabwatch_plugin.service import Service


@pytest.mark.parametrize('failure', ['outgoing', 'incoming', 'ledger_start', 'observer', 'invalid_policy'])
async def test_observation_failure_preserves_raw_receipts_and_continuing_transport(tmp_path, monkeypatch, caplog, failure):
    sent = []
    response = ' {"id":7, "result":{"value":"PRIVATE-URL-MESSAGE"}} '

    async def upstream(ws):
        async for raw in ws:
            sent.append(raw)
            await ws.send(response)

    def broken(*args, **kw):
        raise sqlite3.OperationalError('PRIVATE-URL-MESSAGE')

    async with serve(upstream, '127.0.0.1', 0) as server:
        port = server.sockets[0].getsockname()[1]
        if failure == 'ledger_start':
            monkeypatch.setattr(module, 'Store', broken)
        service = Service(tmp_path, f'http://127.0.0.1:{port}', 0,
                          cleanup_error='invalid_settings' if failure == 'invalid_policy' else None)
        # Unavailable discovery is real; the forwarding websocket is still usable.
        await service.start()
        try:
            if service.store:
                service.store.coverage('fixture')
                service.store.observe([dict(targetId='tab', type='page', url='about:blank')])
            if failure == 'outgoing':
                monkeypatch.setattr(service.store, 'rows', broken)
            elif failure == 'incoming':
                original = service.store.touch
                touches = 0
                def fail_completion(*args):
                    nonlocal touches
                    touches += 1
                    if touches == 2:
                        broken()
                    return original(*args)
                monkeypatch.setattr(service.store, 'touch', fail_completion)
            elif failure == 'observer':
                monkeypatch.setattr(service, '_observer_event', broken)
                service.observer_event({'method':'fixture'})
            wire = ' {"id":7,"method":"Runtime.evaluate","params":{"expression":"PRIVATE-URL-MESSAGE"}} '
            async with connect(f'ws://127.0.0.1:{service.port}/devtools/page/tab') as client:
                for _ in range(2):
                    await client.send(wire)
                    assert await asyncio.wait_for(client.recv(), 3) == response
            assert sent == [wire, wire]
            assert service.health()['healthy'] is False
            assert (await service.sweep(True))['closed'] == []
            assert (tmp_path / 'control.sock').exists()
            assert 'PRIVATE-URL-MESSAGE' not in caplog.text
            if failure in {'outgoing', 'incoming', 'ledger_start', 'observer'}:
                assert (tmp_path / 'cleanup.gap').exists()
        finally:
            await service.stop()


async def test_final_revalidation_rechecks_external_gap(tmp_path):
    from tabwatch_plugin.control import fence_cleanup
    service = Service(tmp_path, port=0)
    now = [100000.]
    store = service.store
    store.clock = lambda: now[0]
    store.coverage('fixture')
    target = dict(targetId='tab', type='page', url='about:blank')
    store.observe([target])
    now[0] += 1801
    store.heartbeat()
    class RPC:
        closes = 0
        async def call(self, method, params=None):
            if method == 'Target.closeTarget':
                self.closes += 1
                return {'success':True}
            fence_cleanup(tmp_path)
            return {'targetInfos':[target]}
        async def close(self):
            pass
    service.rpc = RPC()
    async def version():
        return {'webSocketDebuggerUrl':'ws://127.0.0.1/devtools/browser/fixture'}
    service.version = version
    try:
        assert (await service.sweep(True))['closed'] == []
        assert service.rpc.closes == 0
    finally:
        await service.stop()
