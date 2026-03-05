from functools import cache
from typing import Any

from ssb_konjunk.dash.calculations.calc_data import DataManager
from ssb_konjunk.dash.calculations.calc_data import get_data_manager

_config = None

def setup(config: type[Any]) -> None:
    """Setter global konfigurasjon for modulen.

    Args:
        config (type[Config]): Klassen som inneholder konfigurasjonsinnstillinger.
    """
    global _config
    _config = config


@cache
def get_data(file: str | None = None) -> DataManager:
    """Henter en DataManager-instans basert på valgt datasett.

    Returnerer enten det nyeste datasettet eller et eldre alternativ, avhengig av
    parameterverdien. Resultatet caches for å unngå gjentatte lasting av samme fil.

    Args:
        file (Optional[str]): Hvis satt til "old", hentes eldre datasett. Standard er None,
            som gir det nyeste datasettet.

    Returns:
        DataManager: En instans med forhåndslastet data klar for videre analyse.
    """
    if _config is None:
        raise RuntimeError(
            "Package not configured. Call setup(config) in start_dash first."
        )

    if file == "old":
        return get_data_manager(_config.select_file(1))
    else:
        return get_data_manager(_config.data_path())


@cache
def dropdown_getter(file: str | None = None) -> list[dict[str, str]]:
    """Oppretter en liste over perioder for nedtrekksmeny basert på tilgjengelige data.

    Henter perioder fra en `DataManager` og returnerer dem som en liste av ordbøker
    med 'id' og 'title'-felter, sortert i synkende rekkefølge. Brukes typisk som
    datakilde til grensesnitt-komponenter som dropdowns.

    Args:
        file (Optional[str]): Hvis "old", hentes eldre datasett. Hvis None, brukes standarddatasett.

    Returns:
        list[dict[str, str]]: Liste med perioder, hver som et ordbokelement med 'id' og 'title'.
    """
    datas = get_data(file)
    dropdown_data = [
        {"id": item, "title": item}
        for item in sorted(datas.get_all_periods(), reverse=True)
    ]
    return dropdown_data
