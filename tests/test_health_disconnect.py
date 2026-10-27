import asyncio
import time

import pytest

from tabwatch_plugin.service import Service
from tabwatch_plugin.control import request


async def test_disconnected_health_reader_does_not_fence_service(tmp_path, monkeypatch):
    service = Service(tmp_path, port=0)
    async def quiet():
        await asyncio.Event().wait()
    monkeypatch.setattr(service, 'observe_forever', quiet)
    await service.start()
    try:
        service.store.coverage('fixture')
        service.store.heartbeat()
        original = service.health
        def slow_health():
            time.sleep(.15)  # synthetic busy owner, real disconnected Unix client
            return original()
        monkeypatch.setattr(service, 'health', slow_health)
        with pytest.raises(TimeoutError):
            await asyncio.to_thread(request, tmp_path, 'health', timeout=.02)
        await asyncio.sleep(.05)
        monkeypatch.setattr(service, 'health', original)
        service.store.heartbeat()
        reply = await asyncio.to_thread(request, tmp_path, 'health')
        assert reply['healthy'] is True
        assert reply['cleanup_error'] is None
        assert not (tmp_path / 'cleanup.gap').exists()
    finally:
        await service.stop()


async def test_disconnected_mutating_control_still_fences_service(tmp_path, monkeypatch):
    service = Service(tmp_path, port=0)
    await service.start()
    async def slow_sweep(apply=False):
        await asyncio.sleep(.15)
        return {'candidates': [], 'closed': [], 'errors': []}
    monkeypatch.setattr(service, 'sweep', slow_sweep)
    try:
        with pytest.raises(TimeoutError):
            await asyncio.to_thread(request, tmp_path, 'sweep', timeout=.02, apply=True)
        await asyncio.sleep(.25)
        reply = await asyncio.to_thread(request, tmp_path, 'health')
        assert reply['healthy'] is False
        assert reply['cleanup_error'] == 'control_failure'
        assert reply['cleanup_gap_reason'] == 'control_failure'
    finally:
        await service.stop()
