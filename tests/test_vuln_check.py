import ssb_konjunk.vuln_check as mod
import pytest
import subprocess  # for å "late som" subprocess gir TimeoutExpired error.


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


def test_manglende_pip_audit_gir_importerror(monkeypatch):
    # monkeypatch er noe magiske innebygde pytest-greier

    def none_funksjon(whatever):
        # vi midlertidig erstatter importlib.util.find_spec med dette.
        return None  # importlib.util.find_spec tar inn ett (obligatorisk) argument, så da må denne gjøre det også

    monkeypatch.setattr(
        mod.importlib.util, "find_spec", none_funksjon
    )  # dette bascially gjør så find_spec returnerer None (men bare akkurat her, monkeypatch resetter det automatisk), som det jo vil gjøre hvis pip-audit ikke er installert
    with pytest.raises(
        ImportError, match="pip-audit trengs for å sjekke svakheter"
    ):  # bascially "assert at det kom en warning, og at denne inneholdt den custom teksten jeg lagde"
        mod.check_vulnerable_packages()


def test_timeout_gir_warning_og_none(monkeypatch):
    def noe_funksjon(whatever):
        return object()

    monkeypatch.setattr(
        mod.importlib.util, "find_spec", noe_funksjon
    )  # dette basically gjør så find_spec *ikke* returnerer None (men bare akkurat her, monkeypatch resetter det automatisk).
    # med andre ord spoofer vi at pip-audit *er* installert.
    # hvorfor spoofer vi, hvorfor lar vi ikke bare den faktiske find_spec sjekke? mest fordi testen ikke skal avhenge av miljøet den kjører i

    def timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd="pip_audit", timeout=60)

    monkeypatch.setattr(
        mod.subprocess, "run", timeout
    )  # "lat som" subprocess gir TimeoutExpired error.
    # i hvilket tilfelle funksjonen skal gi en warning:
    with pytest.warns(
        UserWarning, match="aiai jeg fikk ikke til dette, enda jeg prøvde"
    ):
        assert (
            mod.check_vulnerable_packages() is None
        )  # og gitt at det kom en warning, også at funksjonen returnerte None


def test_report_ingen_svakheter(capsys):
    mod._report(lag_data([]))
    assert "Alle pakker ok" in capsys.readouterr().out


def test_report_med_fiks_thrower_error():
    data = lag_data([("PYSEC-1", ["1.1"])])
    with pytest.raises(  # skal throwe error, med den custom teksten
        mod.VulnerablePackagesError, match="Oppdater disse før du fortsetter!"
    ):
        mod._report(data)


def test_report_uten_fiks_gir_bare_warning():
    data = lag_data([("PYSEC-2", [])])
    # skal ikke throwe error (men skal gi warning, som inneholder warning-teksten)
    with pytest.warns(
        UserWarning,
        match="OBS: fant pakker med svakheter, men det fins ingen fiks for dem",
    ):
        mod._report(data)
