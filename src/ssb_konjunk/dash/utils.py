import shutil
import tempfile
from functools import cache
from pathlib import Path

@cache
def get_assets_folder() -> str:
    """Lager en temp mappe som har assets fra pakken og assets lokalt.

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
