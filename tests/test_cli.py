import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import tabwatch_plugin.cli as cli


def test_unhealthy_observer_never_sweeps(monkeypatch,capsys):
    calls=[]
    clock=iter([0,16])
    monkeypatch.setattr(cli.time,'monotonic',lambda:next(clock))
    monkeypatch.setattr(sys,'argv',['tabwatch_cli','--apply'])
    monkeypatch.setattr(cli,'request',lambda state,cmd,**kw:calls.append(cmd)or {'healthy':False})
    assert cli.main()==1 and calls==['health']
    assert 'cleanup skipped' in capsys.readouterr().out


def test_healthy_empty_apply_is_silent(monkeypatch,capsys):
    calls=[]
    def reply(state,cmd,**kw):
        calls.append((cmd,kw))
        return {'healthy':True} if cmd=='health' else {'closed':[],'errors':[],'candidates':[]}
    monkeypatch.setattr(sys,'argv',['tabwatch_cli','--apply']);monkeypatch.setattr(cli,'request',reply)
    assert cli.main()==0 and capsys.readouterr().out==''
    assert calls[-1][1]['apply']is True


def test_default_mode_is_dry_run(monkeypatch,capsys):
    calls=[]
    def reply(state,cmd,**kw):
        calls.append((cmd,kw));return {'healthy':True} if cmd=='health' else {'closed':[],'errors':[],'candidates':[]}
    monkeypatch.setattr(sys,'argv',['tabwatch_cli']);monkeypatch.setattr(cli,'request',reply)
    assert cli.main()==0 and calls[-1][1]['apply']is False
    assert 'candidates' in capsys.readouterr().out
