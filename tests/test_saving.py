"""Test read and write functionality."""

import pytest

from ssb_konjunk.saving import _find_version_number
from ssb_konjunk.saving import _remove_edge_slashes
from ssb_konjunk.saving import _structure_ssb_filepath
from ssb_konjunk.saving import _verify_base_filename
from ssb_konjunk.saving import _verify_datatilstand


def test_remove_edge_slashes() -> None:
    """Test of function _remove_edge_slashes."""
    assert _remove_edge_slashes("/test_string/test_mer") == "test_string/test_mer"
    assert _remove_edge_slashes("/test_string/test_mer/") == "test_string/test_mer"
    assert _remove_edge_slashes("/test_string/test_mer") != "/test_string/test_mer"


def test_structure_ssb_filepath() -> None:
    """Test of function _structure_ssb_filepath."""
    filename_1 = _structure_ssb_filepath(
        (2023, 2024),
        "Y",
        "ssb-reiseliv-korttid-data-produkt-prod",
        "overnatting",
        "inndata",
        "min_fil",
        "",
        1,
        "parquet",
    )

    assert (
        filename_1
        == "ssb-reiseliv-korttid-data-produkt-prod/overnatting/inndata/min_fil_p2023_p2024_v1.parquet"
    ), filename_1

    filename_2 = _structure_ssb_filepath(
        (2023, 2024),
        "Y",
        "ssb-reiseliv-korttid-data-produkt-prod",
        "overnatting",
        "inndata",
        "min_fil",
        folder="",
        version_number=None,
        filetype="parquet",
    )

    assert (
        filename_2
        == "ssb-reiseliv-korttid-data-produkt-prod/overnatting/inndata/min_fil_p2023_p2024"
    ), filename_2

    filename_3 = _structure_ssb_filepath(
        (2023, 2024),
        "Y",
        "ssb-reiseliv-korttid-data-produkt-prod",
        "overnatting",
        "inndata",
        "min_fil",
        folder="mellommappe/",
        filetype="csv",
    )

    assert (
        filename_3
        == "ssb-reiseliv-korttid-data-produkt-prod/overnatting/inndata/mellommappe/min_fil_p2023_p2024"
    ), filename_3


def test_find_version_number() -> None:
    """Test function _find_version_number."""
    assert (
        _find_version_number(["minfil_p2023_p2024_v0.parquet"], stable_version=False)
        == "0"
    )
    assert (
        _find_version_number(["minfil_p2023_p2024_v0.parquet"], stable_version=True)
        == "1"
    )

    files = [
        "minfil_p2023_p2024_v0.parquet",
        "minfil_p2023_p2024_v1.parquet",
        "minfil_p2023_p2024_v2.parquet",
    ]

    assert _find_version_number(files, stable_version=False) == "0"

    # Need to pass input, first n and then y. For running: pytest -s
    # assert _find_version_number(files, stable_version=True) == '3'


def test_verify_base_filename() -> None:
    """Test function _verify_base_filename."""
    assert _verify_base_filename("MINFIL") == "minfil"

    with pytest.raises(
        ValueError,
        match=r"Basefilnavnet kan ikke starte med et siffer. Dette må endres. \nNåværende basefilnavn: 1minfil",
    ):
        _verify_base_filename("1minfil")

    with pytest.raises(
        ValueError,
        match=r"Filformatet 'parquet' skal ikke være en del av basefilnavnet, fjern det.",
    ):
        _verify_base_filename("minfilparquet")

    with pytest.raises(
        ValueError,
        match=r"Filformatet 'csv' skal ikke være en del av basefilnavnet, fjern det.",
    ):
        _verify_base_filename("minfilcsv")


def test_verify_datatilstand() -> None:
    """Test function _verify_datatilstand."""
    assert _verify_datatilstand("utdata") == "utdata"
    assert _verify_datatilstand("temp") == "temp"

    # Need to pass input, inndata. For running: pytest -s
    # assert _verify_datatilstand('overnatting') == 'inndata'


def test_verify_list_filtypes() -> None:
    """Test of function _structure_ssb_filepath without version number."""
    filenames = [
        "ssb-vare-tjen-korttid-data-produkt-test/vhi/inndata/utvalg/ra-0187/ra-0187_p2024-01-01_p2024-01-31.csv",
        "ssb-vare-tjen-korttid-data-produkt-test/vhi/inndata/utvalg/ra-0187/ra-0187_p2024-01-01_p2024-01-31.parquet",
        "ssb-vare-tjen-korttid-data-produkt-test/vhi/inndata/utvalg/ra-0187/ra-0187_p2024-01-01_p2024-01-31_69.parquet",
        "ssb-vare-tjen-korttid-data-produkt-test/vhi/inndata/utvalg/ra-0187/ra-0187_p2024-01-01_p2024-01-31_v69.parquet",
        "ssb-vare-tjen-korttid-data-produkt-test/vhi/inndata/utvalg/ra-0187/ra-0187_p2024-01-01_p2024-01-31_v70.csv",
    ]

    filetype = "parquet"

    # Only include files with the relevant file extension
    filenames = [i for i in filenames if i.endswith(filetype)]

    # Sorts the filenames according to version numbers.
    filenames.sort()

    assert all(item.endswith(filetype) for item in filenames)
