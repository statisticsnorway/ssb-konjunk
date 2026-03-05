from functools import cache

from ssb_konjunk.dash.calculations.calc_data import get_data_manager

_config = None


def setup(config) -> None:
    global _config
    _config = config


@cache
def get_data(file: str | None = None):
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

    
@cache
def get_assets_folder() -> str:
    """
    Lager en temp mappe som har assets fra pakken og assets lokalt.

    lokale assets overskriver pakke assets.

    Returns:
        str: path til temp mappe med assets.
    """
    package_assets = Path(ssb_konjunk.__file__).parent / "dash" / "assets"
    local_assets = Path("dash/assets")
    
    combined = Path(tempfile.mkdtemp(prefix="dash_assets_"))

    if package_assets.exists():
        shutil.copytree(package_assets, combined, dirs_exist_ok=True)

    if local_assets.exists():
        shutil.copytree(local_assets, combined, dirs_exist_ok=True)
        
    return str(combined)