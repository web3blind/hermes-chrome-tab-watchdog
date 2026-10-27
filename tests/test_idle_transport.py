import asyncio
import json
import pytest
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed
from tabwatch_plugin.service import Service


@pytest.mark.parametrize('scope,ack,method', [
    ('known', True, 'Runtime.evaluate'), ('known', False, 'Runtime.evaluate'),
    ('known', False, 'Runtime.enable'), ('unknown', False, 'Runtime.evaluate')])
async def test_disconnect_preserves_attributed_idle_age(tmp_path, monkeypatch, scope, ack, method):
    async def upstream(ws):
        raw = await ws.recv()
        if ack:
            await ws.send(json.dumps({'id': json.loads(raw)['id'], 'result': {}}))
        await ws.close(1011, 'fixture transport loss')
    async def quiet(self):
        await asyncio.Event().wait()
    monkeypatch.setattr(Service, 'observe_forever', quiet)
    async with serve(upstream, '127.0.0.1', 0) as server:
        s = Service(tmp_path, f'http://127.0.0.1:{server.sockets[0].getsockname()[1]}', 0)
        await s.start()
        try:
            now = [100000.]
            s.store.clock = lambda: now[0]
            s.store.coverage('fixture')
            s.store.observe([{'targetId': t, 'type': 'page', 'url': f'https://example.invalid/{t}'} for t in ['old', 'busy']])
            baseline = s.store.coverage_since
            now[0] += 21601
            s.store.heartbeat()
            path = 'page/busy' if scope == 'known' else 'browser/fixture'
            async with connect(f'ws://127.0.0.1:{s.port}/devtools/{path}') as client:
                await client.send(json.dumps({'id': 1, 'method': method, 'params': {}}))
                if ack:
                    assert json.loads(await client.recv())['id'] == 1
                with pytest.raises(ConnectionClosed):
                    await client.recv()
            await asyncio.sleep(.1)
            s.store.heartbeat()
            if scope == 'unknown':
                assert s.store.coverage_since == now[0]
                assert s.store.candidates() == []
            else:
                assert s.store.coverage_since == baseline
                assert s.store.gap_count == 0
                expected = [('old', 'idle')] if method == 'Runtime.evaluate' else [('busy', 'idle'), ('old', 'idle')]
                assert s.store.candidates() == expected
        finally:
            await s.stop()
