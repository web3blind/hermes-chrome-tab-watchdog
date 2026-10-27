import sys,time,unittest
from pathlib import Path
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from tabwatch_plugin.store import Store
import test_service as integration

@pytest.fixture
def policy(tmp_path):
    now=[100000.0];s=Store(tmp_path,clock=lambda:now[0]);s.coverage('test-browser')
    s.observe([{'targetId':'blank','type':'page','url':'about:blank'}]);s.heartbeat()
    yield s,now
    s.close()

@pytest.mark.parametrize('age,eligible',[(1799,False),(1800,True),(7201,True)])
def test_exact_blank_30_minute_age(policy,age,eligible):
    s,now=policy;now[0]+=age;s.heartbeat()
    assert (('blank','blank')in s.candidates())is eligible

def test_blank_age_is_not_idle_or_duplicate_winner_time(policy):
    s,now=policy;now[0]+=1801;s.touch('blank','tool');s.touch('blank','activation')
    s.coverage('test-browser');s.observer_grace['blank']=now[0];s.heartbeat()
    assert s.candidates()==[('blank','blank')]

def test_busy_and_unhealthy_blank_are_preserved(policy):
    s,now=policy;now[0]+=1801;s.heartbeat();s.pending['blank']=1
    assert s.candidates()==[]
    s.pending.clear();s.lease_start('tool',60)
    assert s.candidates()==[]
    s.lease_end('tool');s.ready=False
    assert s.candidates()==[]

def test_navigated_or_fragment_page_is_not_plain_blank(policy):
    s,now=policy;now[0]+=1801
    for url in ['https://example.test/','about:blank#draft','chrome://newtab/']:
        s.observe([{'targetId':'blank','type':'page','url':url}]);s.heartbeat()
        assert s.candidates()==[]

class BlankBatch(unittest.IsolatedAsyncioTestCase):
    asyncSetUp=integration.DisposableObserverRegressions.asyncSetUp
    _cleanup=integration.DisposableObserverRegressions._cleanup
    wait=integration.DisposableObserverRegressions.wait

    async def test_actual_blank_batch_close_and_busy_fence(self):
        targets=[self.tid]
        for _ in range(3):
            targets.append((await self.client.call('Target.createTarget',{'url':'about:blank'}))['targetId'])
        await self.wait(lambda:self.service.store.healthy() and len(self.service.store.rows())==4)
        self.service.store.clock=lambda:time.time()+1801
        self.service.store.heartbeat()
        self.service.store.pending[self.tid]=1
        result=await self.service.sweep(True)
        assert not result['errors'] and set(result['closed'])==set(targets)-{self.tid}
        remaining=(await self.client.call('Target.getTargets'))['targetInfos']
        assert {t['targetId']for t in remaining}.intersection(targets)=={self.tid}
