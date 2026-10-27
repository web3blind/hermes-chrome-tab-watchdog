import asyncio
import time

import pytest

from tabwatch_plugin.service import fetch_json
from tabwatch_plugin.runtime import Runtime
from tabwatch_plugin.policy import Policy
from test_runtime import settings


@pytest.mark.asyncio
async def test_trickle_discovery_is_cancellable_on_unload(tmp_path):
    accepted, closed = asyncio.Event(), asyncio.Event()
    async def trickle(reader, writer):
        try:
            await reader.readuntil(b'\r\n\r\n')
            writer.write(b'HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n')
            accepted.set()
            while True:
                writer.write(b' ')
                await writer.drain()
                await asyncio.sleep(.03)
        except (ConnectionError, asyncio.CancelledError):
            pass
        finally:
            writer.close()
            closed.set()
    server = await asyncio.start_server(trickle, '127.0.0.1', 0)
    values = settings()
    values['upstream'] = f'http://127.0.0.1:{server.sockets[0].getsockname()[1]}'
    runtime = Runtime(tmp_path, values, Policy(), None)
    runtime.start()
    try:
        await asyncio.wait_for(accepted.wait(), 5)
        start = time.monotonic()
        await asyncio.wait_for(asyncio.to_thread(runtime.stop), 3)
        assert time.monotonic() - start < 3
        assert not runtime.thread.is_alive() and runtime.loop is None
        assert not (runtime.state / 'control.sock').exists()
        await asyncio.wait_for(closed.wait(), 2)
    finally:
        runtime.stop()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_discovery_absolute_deadline_even_with_continuous_data():
    closed = asyncio.Event()
    async def trickle(reader, writer):
        try:
            await reader.readuntil(b'\r\n\r\n')
            writer.write(b'HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n')
            while True:
                writer.write(b' ')
                await writer.drain()
                await asyncio.sleep(.03)
        except (ConnectionError, asyncio.CancelledError):
            pass
        finally:
            writer.close()
            closed.set()
    server = await asyncio.start_server(trickle, '127.0.0.1', 0)
    try:
        with pytest.raises(asyncio.TimeoutError):
            await fetch_json(f'http://127.0.0.1:{server.sockets[0].getsockname()[1]}/json/version')
        await asyncio.wait_for(closed.wait(), 2)
    finally:
        server.close()
        await server.wait_closed()
