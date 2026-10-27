import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import tabwatch_plugin.cli as cli
from tabwatch_plugin.store import Store


def test_transient_navigation_health_is_retried(monkeypatch,tmp_path):
    replies=iter([{'healthy':False},{'healthy':True}]);sleeps=[]
    monkeypatch.setattr(cli,'request',lambda *a,**k:next(replies))
    monkeypatch.setattr(cli.time,'monotonic',lambda:0)
    monkeypatch.setattr(cli.time,'sleep',sleeps.append)
    assert cli.observation_health(tmp_path)['healthy']is True
    assert sleeps==[.5]


def test_creation_receipt_does_not_drop_other_pages_or_reset_grace(tmp_path):
    clock=[100.0];s=Store(tmp_path,clock=lambda:clock[0])
    try:
        s.coverage('epoch');s.observe([{'targetId':'old','type':'page','url':'about:blank'}]);s.heartbeat()
        coverage=s.coverage_since
        clock[0]=104
        s.observe_target({'targetId':'new','type':'page','url':'about:blank'})
        s.touch('new','tool')
        assert {r['target']for r in s.rows()}=={'old','new'}
        assert s.coverage_since==coverage
        assert next(r for r in s.rows()if r['target']=='old')['last_tool']==0
        assert next(r for r in s.rows()if r['target']=='new')['last_tool']==104
        assert s.observe_target({'targetId':'worker','type':'worker','url':'https://example.test'})is False
    finally:s.close()
