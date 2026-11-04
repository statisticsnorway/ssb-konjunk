import ssb_konjunk.vuln_check as mod
import pytest # for å raise errors
import subprocess # for å raise timeoutexpired error, kjører ikke faktisk kode


def lag_data(*vulns_per_pakke):
    """lager et liksom-pip-audit-resultat. Hver vuln er (id, fix_versions)."""
    return {
        "dependencies": [
            {
                "name": f"pakke{i}",
                "version": "1.0",
                "vulns": [{"id": vid, "fix_versions": fix} for vid, fix in vulns],
            }
            for i, vulns in enumerate(vulns_per_pakke)
        ]
    }


def test_manglende_pip_audit_gir_importerror(monkeypatch): #monkeypatch er noe magiske innebygde pytest-greier
    monkeypatch.setattr(mod.importlib.util, "find_spec", lambda name: None)
    with pytest.raises(ImportError):
        mod.check_vulnerable_packages()
 
 
def test_timeout_gir_warning_og_none(monkeypatch):
    monkeypatch.setattr(mod.importlib.util, "find_spec", lambda name: object())
 
    def timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd="pip_audit", timeout=60)
 
    monkeypatch.setattr(mod.subprocess, "run", timeout)
    with pytest.warns(UserWarning):
        assert mod.check_vulnerable_packages() is None

def test_report_ingen_svakheter(capsys):
    mod._report(lag_data([]))
    assert "Alle pakker ok" in capsys.readouterr().out
 
 
def test_report_med_fiks_thrower_error():
    data = lag_data([("PYSEC-1", ["1.1"])])
    with pytest.raises(mod.VulnerablePackagesError, match="PYSEC-1"):
        mod._report(data) # skal throwe error
 
 
def test_report_uten_fiks_gir_bare_warning():
    data = lag_data([("PYSEC-2", [])])
    with pytest.warns(UserWarning, match="PYSEC-2"):
        mod._report(data)  # skal ikke throwe error (men skal gi warning)
        