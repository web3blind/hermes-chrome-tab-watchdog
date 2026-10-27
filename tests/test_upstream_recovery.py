"""Real disconnect/recovery boundary; only a disposable unauthenticated browser."""
import asyncio
import os
import signal
import socket
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tabwatch_plugin.service import Service


class UpstreamRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_refused_disconnect_and_recovery(self):
        with tempfile.TemporaryDirectory() as directory:
            with socket.socket() as reservation:
                reservation.bind(('127.0.0.1', 0))
                port = reservation.getsockname()[1]
            service = Service(Path(directory) / 'state', f'http://127.0.0.1:{port}', 0)
            process = None

            async def until(predicate):
                for _ in range(250):
                    if predicate():
                        return
                    await asyncio.sleep(.1)
                self.fail('disposable upstream recovery deadline')

            def launch():
                return subprocess.Popen([
                    'chromium', '--headless=new', '--no-sandbox', '--disable-gpu',
                    '--remote-debugging-address=127.0.0.1',
                    f'--remote-debugging-port={port}',
                    '--user-data-dir=' + str(Path(directory) / 'browser'), 'about:blank',
                ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)

            async def terminate(process):
                # PID belongs only to the Popen above, never the real browser.
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                await asyncio.to_thread(process.wait, timeout=10)

            try:
                await service.start()
                await until(lambda: service.observer_error is not None)
                self.assertFalse(service.store.healthy())
                self.assertEqual(service.observer_error.get('errno'), 111)
                self.assertEqual((await service.sweep(apply=True))['closed'], [])
                process = launch()
                await until(service.store.healthy)
                self.assertIsNone(service.observer_error)
                coverage = service.store.coverage_since
                epoch = service.store.epoch
                await terminate(process)
                process = None
                await until(lambda: not service.store.healthy() and
                            (service.observer_error or {}).get('errno') == 111)
                self.assertEqual((await service.sweep(apply=True))['closed'], [])
                process = launch()
                await until(service.store.healthy)
                self.assertIsNone(service.observer_error)
                self.assertGreater(service.store.coverage_since, coverage)
                self.assertNotEqual(service.store.epoch, epoch)
                self.assertEqual(service.store.candidates(), [])
            finally:
                await service.stop()
                if process is not None:
                    await terminate(process)
