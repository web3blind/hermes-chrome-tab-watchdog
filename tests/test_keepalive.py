"""Final-close guards exercised against disposable Chromium, never user Chrome."""
import time
import unittest

import test_service as integration
from tabwatch_plugin.service import fetch_json
from tabwatch_plugin.store import page_target


class KeepaliveTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = integration.DisposableObserverRegressions.asyncSetUp
    _cleanup = integration.DisposableObserverRegressions._cleanup
    wait = integration.DisposableObserverRegressions.wait

    async def pages(self):
        targets = (await self.client.call('Target.getTargets'))['targetInfos']
        return {t['targetId']: t for t in targets if page_target(t)}

    async def create(self, url):
        tid = (await self.client.call('Target.createTarget', {'url': url}))['targetId']
        await self.wait(lambda: self.service.store.healthy()
                        and tid in self.service.installed)
        return tid

    def expire(self, seconds):
        self.service.store.clock = lambda: time.time() + seconds
        self.service.store.heartbeat()

    async def assert_endpoint_up(self):
        self.assertIsNone(self.proc.poll())
        self.assertIn('webSocketDebuggerUrl', await fetch_json(
            self.service.upstream + '/json/version'))
        port = self.service.server.sockets[0].getsockname()[1]
        self.assertIn('webSocketDebuggerUrl', await fetch_json(
            f'http://127.0.0.1:{port}/json/version'))

    async def test_expired_blank_batch_keeps_one_across_sweeps(self):
        original = {self.tid}
        for _ in range(3):
            original.add(await self.create('about:blank'))
        self.expire(1801)
        self.assertEqual(len(self.service.store.candidates()), 4)
        result = await self.service.sweep(True)
        self.assertEqual(result['errors'], [])
        self.assertEqual(len(result['closed']), 3)
        remaining = await self.pages()
        self.assertEqual(set(remaining), original - set(result['closed']))
        self.assertEqual(len(remaining), 1)
        self.assertEqual(next(iter(remaining.values()))['url'], 'about:blank')
        await self.assert_endpoint_up()
        await self.wait(lambda: self.service.store.healthy())
        again = await self.service.sweep(True)
        self.assertTrue(again['candidates'])  # Planning remains unchanged.
        self.assertEqual(again['closed'], [])
        self.assertEqual(again['errors'], [])
        self.assertEqual(await self.pages(), remaining)
        await self.assert_endpoint_up()

    async def test_final_blank_survives_active_then_closed_mission(self):
        mission = await self.create('data:text/html,<title>fixture mission</title>')
        self.expire(90001)
        self.service.store.pending[mission] = 1
        result = await self.service.sweep(True)
        self.assertEqual(result['errors'], [])
        self.assertEqual(result['closed'], [])
        self.assertEqual(set(await self.pages()), {self.tid, mission})
        self.service.store.pending.clear()
        result = await self.service.sweep(True)
        self.assertEqual(result['errors'], [])
        self.assertEqual(result['closed'], [mission])
        self.assertEqual(set(await self.pages()), {self.tid})
        await self.assert_endpoint_up()

    async def test_last_nonblank_page_is_retained_without_creating_blank(self):
        await self.client.call('Page.navigate', {
            'url': 'data:text/html,<title>unrelated fixture</title>'}, self.sid)
        await self.wait(lambda: self.service.store.healthy()
                        and self.service.store.rows()[0]['url_hash']
                        != self.service.store.digest('about:blank'))
        self.expire(90001)
        self.assertEqual(self.service.store.candidates(), [(self.tid, 'idle')])
        result = await self.service.sweep(True)
        self.assertEqual(result['closed'], [])
        self.assertEqual(result['errors'], [])
        remaining = await self.pages()
        self.assertEqual(set(remaining), {self.tid})
        self.assertNotEqual(remaining[self.tid]['url'], 'about:blank')
        await self.assert_endpoint_up()

    async def test_fences_hold_while_other_expired_tabs_still_close(self):
        busy = await self.create('about:blank')
        extra = await self.create('about:blank')
        self.expire(1801)
        store = self.service.store
        store.pending[busy] = 1
        store.lease_start('fixture', 60)
        self.assertEqual((await self.service.sweep(True))['closed'], [])
        store.lease_end('fixture')
        store.fresh = store.clock() - 21
        stale = await self.service.sweep(True)
        self.assertEqual(stale['closed'], [])
        self.assertEqual(stale['errors'], [{'reason': 'cleanup_disabled'}])
        store.heartbeat()
        store.observer_missing.add(extra)
        self.assertEqual((await self.service.sweep(True))['closed'], [])
        store.observer_missing.discard(extra)
        store.heartbeat()
        result = await self.service.sweep(True)
        self.assertEqual(result['errors'], [])
        self.assertEqual(set(result['closed']), {self.tid, extra})
        self.assertEqual(set(await self.pages()), {busy})
        store.pending.clear()
        await self.wait(lambda: store.healthy())
        self.assertEqual((await self.service.sweep(True))['closed'], [])
        await self.assert_endpoint_up()


if __name__ == '__main__':
    unittest.main()
