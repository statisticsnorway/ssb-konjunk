import importlib.util
import json
import subprocess
import sys
import warnings


class VulnerablePackagesError(RuntimeError):
    """raised når det fins pakker med svakheter *og* det fins en fiks for dem."""


def _report(data: dict[str, list]) -> None:
    """Lag rapport på eventuelle svakheter."""
    fixable, unfixable = [], []
    for dep in data.get("dependencies", []):
        for v in dep.get("vulns", []):
            fixes = v.get("fix_versions") or []
            if fixes:
                fixable.append(
                    f"  {dep['name']}=={dep['version']}: {v['id']} "
                    f"(fixed in {', '.join(fixes)})"
                )
            else:
                unfixable.append(f"  {dep['name']}=={dep['version']}: {v['id']}")

    if unfixable:
        warnings.warn(
            "OBS: fant pakker med svakheter, men det fins ingen fiks for dem ennå. Vurder å vente med å kjøre resten av koden.\n"
            + "\n".join(unfixable),
            stacklevel=3,
        )

    if fixable:
        raise VulnerablePackagesError(
            "OBS! 🚨🚨🚨🚨🚨🚨🚨🚨 Fant pakker med svakheter:\n"
            + "\n".join(fixable)
            + "\nOppdater disse før du fortsetter!"
            + "\nÅpne terminal, skriv poetry update [pakkenavn]"
        )

    if not fixable or unfixable:
        print("Alle pakker ok!👍")
    return None


def check_vulnerable_packages() -> dict[str, list] | None:
    """Funksjon for å sjekke om python-miljøet ditt har pakker som har kjente svakheter."""
    timeout_limit = 60
    spec = importlib.util.find_spec("pip_audit")
    if spec is None:
        raise ImportError(  # bruker du denne funksjonen som pakke skal den jo alltid være installert, men i tilfelle noen eks. bare kopierer dette til eget repo elns
            "pip-audit trengs for å sjekke svakheter."  # subprocess gir nemlig ikke feilmelding hvis pip-audit ikke er installert
            "Kjør `poetry add pip-audit`, restart kernel og prøv igjen."
        )
    try:
        r = subprocess.run(
            [
                sys.executable,
                "-m",
                "pip_audit",
                "--format",
                "json",  # kjør noe tilsvarende python -m pip_audit --format json --progress-spinner off
                "--progress-spinner",
                "off",  # (pip-audit har et python-bibliotek, men det er ikke godt dokumentert og er visst ikke garantert å være stabilt, ifølge klåd.
            ],  # terminalkjøring er derfor riktig måte å bruke det på)
            capture_output=True,
            text=True,
            timeout=timeout_limit,  # og kopier outputet (som vi spesifiserte til å være i json-format)
        )
        data = json.loads(r.stdout)

    except subprocess.TimeoutExpired:
        warnings.warn(
            f"aiai jeg fikk ikke til dette, enda jeg prøvde i {timeout_limit} sekunder. gir opp livet ass",
            stacklevel=2,
        )
        return None
    except Exception as e:
        print("funka ikke, idk why")
        print(e)
        return None

    _report(data)
