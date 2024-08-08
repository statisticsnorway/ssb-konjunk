"""Test of function timestamp."""

import pytest

from ssb_konjunk.timestamp import check_even
from ssb_konjunk.timestamp import get_ssb_timestamp
from ssb_konjunk.timestamp import get_timestamp_daily
from ssb_konjunk.timestamp import get_timestamp_special
from ssb_konjunk.timestamp import get_timestamp_yearly


def test_check_even() -> None:
    """Test of function check_even."""
    # Testing True
    assert check_even([2, 1]) is True
    # Testing False
    assert check_even([3, 2, 1]) is False


def test_get_timestamp_daily() -> None:
    """Test of function get_timestamp_daily."""
    # Testing True
    assert get_timestamp_daily(2020, 1, 1, 2020, 1, 31) == "p2020-01-01_p2020-01-31"


def test_get_timestamp_yearly() -> None:
    """Test of function get_timestamp_yearly."""
    # Testing True
    assert get_timestamp_yearly(2020, 2021) == "p2020_p2021"


def test_test_get_timestamp_special() -> None:
    """Test of function get_timestamp_special."""
    # Testing month
    assert get_timestamp_special(2020, 1, 2021, 2, frequency="M") == "p2020-01_p2021-02"
    # Testing quarter
    assert get_timestamp_special(2020, 1, 2021, 2, frequency="Q") == "p2020Q1_p2021Q2"
    # Testing term
    assert get_timestamp_special(2020, 1, 2021, 2, frequency="B") == "p2020B1_p2021B2"


def test_get_ssb_timestamp() -> None:
    """Test of function get_ssb_timestamp."""
    # Testing month
    assert get_ssb_timestamp(2020, 1, 2021, 2, frequency="M") == "p2020-01_p2021-02"
    assert get_ssb_timestamp(2020, 1) == "p2020-01"
    # Testing quarter
    assert get_ssb_timestamp(2020, 1, 2021, 2, frequency="Q") == "p2020Q1_p2021Q2"
    assert get_ssb_timestamp(2020, 1, frequency="Q") == "p2020Q1"
    # Testing term
    assert get_ssb_timestamp(2020, 1, 2021, 2, frequency="B") == "p2020B1_p2021B2"
    assert get_ssb_timestamp(2020, 1, frequency="B") == "p2020B1"
    # Testing day
    assert (
        get_ssb_timestamp(2020, 1, 1, 2020, 1, 31, frequency="D")
        == "p2020-01-01_p2020-01-31"
    )
    assert get_ssb_timestamp(2020, 1, 1, frequency="D") == "p2020-01-01"
    # Testing year
    assert get_ssb_timestamp(2020, 2021, frequency="Y") == "p2020_p2021"
    assert get_ssb_timestamp(2020, frequency="Y") == "p2020"

    # Test more than 6 args
    with pytest.raises(
        ValueError,
        match="Du kan ikke ha flere enn seks argumenter for å lage en ssb timestamp. Du har: 7",
    ):
        get_ssb_timestamp(1, 2, 3, 4, 5, 6, 7)

    # Test for to small year
    with pytest.raises(
        ValueError,
        match="Any valid year should be length 4, you have: 1, maybe you should check if year: 1 is correct.",
    ):
        get_ssb_timestamp(1, frequency="Y")
