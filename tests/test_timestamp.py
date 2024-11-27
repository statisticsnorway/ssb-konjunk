"""Test of function timestamp."""

import pytest

from ssb_konjunk.timestamp import _check_even
from ssb_konjunk.timestamp import _check_valid_day
from ssb_konjunk.timestamp import _check_valid_half_year
from ssb_konjunk.timestamp import _check_valid_month
from ssb_konjunk.timestamp import _check_valid_quarter
from ssb_konjunk.timestamp import _check_valid_term
from ssb_konjunk.timestamp import _check_valid_trimester
from ssb_konjunk.timestamp import _check_valid_week
from ssb_konjunk.timestamp import _check_valid_year
from ssb_konjunk.timestamp import _get_timestamp_daily
from ssb_konjunk.timestamp import _get_timestamp_special
from ssb_konjunk.timestamp import _get_timestamp_yearly
from ssb_konjunk.timestamp import check_periodic_year
from ssb_konjunk.timestamp import get_ssb_timestamp


def test_check_even() -> None:
    """Test of function _check_even."""
    # Testing True
    assert _check_even([2, 1]) is True
    # Testing False
    assert _check_even([3, 2, 1]) is False


def test_get_timestamp_daily() -> None:
    """Test of function _get_timestamp_daily."""
    # Testing True
    assert _get_timestamp_daily(2020, 1, 1, 2020, 1, 31) == "p2020-01-01_p2020-01-31"


def test_get_timestamp_yearly() -> None:
    """Test of function _get_timestamp_yearly."""
    # Testing True
    assert _get_timestamp_yearly(2020, 2021) == "p2020_p2021"


def test_test_get_timestamp_special() -> None:
    """Test of function _get_timestamp_special."""
    # Testing month
    assert (
        _get_timestamp_special(2020, 1, 2021, 2, frequency="M") == "p2020-01_p2021-02"
    )
    # Testing quarter
    assert (
        _get_timestamp_special(2020, 1, 2021, 2, frequency="Q") == "p2020-Q1_p2021-Q2"
    )
    # Testing term
    assert (
        _get_timestamp_special(2020, 1, 2021, 2, frequency="B") == "p2020-B1_p2021-B2"
    )


def test_get_ssb_timestamp() -> None:
    """Test of function get_ssb_timestamp."""
    # Testing week
    assert get_ssb_timestamp(2020, 1, 2021, 2, frequency="W") == "p2020-W01_p2021-W02"
    assert get_ssb_timestamp(2020, 1, frequency="W") == "p2020-W01"
    # Testing month
    assert get_ssb_timestamp(2020, 1, 2021, 2, frequency="M") == "p2020-01_p2021-02"
    assert get_ssb_timestamp(2020, 1) == "p2020-01"
    # Testing quarter
    assert get_ssb_timestamp(2020, 1, 2021, 2, frequency="Q") == "p2020-Q1_p2021-Q2"
    assert get_ssb_timestamp(2020, 1, frequency="Q") == "p2020-Q1"
    # Testing term
    assert get_ssb_timestamp(2020, 1, 2021, 2, frequency="B") == "p2020-B1_p2021-B2"
    assert get_ssb_timestamp(2020, 1, frequency="B") == "p2020-B1"
    # Testing trimester
    assert get_ssb_timestamp(2020, 1, 2021, 2, frequency="T") == "p2020-T1_p2021-T2"
    assert get_ssb_timestamp(2020, 1, frequency="T") == "p2020-T1"
    # Testing half year
    assert get_ssb_timestamp(2020, 1, 2021, 2, frequency="H") == "p2020-H1_p2021-H2"
    assert get_ssb_timestamp(2020, 1, frequency="H") == "p2020-H1"
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


def test_check_valid_day() -> None:
    """Test of function _check_valid_day."""
    day = 32
    with pytest.raises(
        ValueError,
        match=f"The arg for day is bigger than possible max is 31 you have: {day}.",
    ):
        _check_valid_day(day)


def test_check_valid_week() -> None:
    """Test of function _check_valid_week."""
    week = 53
    with pytest.raises(
        ValueError,
        match=f"The arg for week is bigger than possible max is 52 you have: {week}.",
    ):
        _check_valid_week(week)


def test_check_valid_month() -> None:
    """Test of function _check_valid_month."""
    month = 13
    with pytest.raises(
        ValueError,
        match=f"The arg for month is bigger than possible max is 12 you have: {month}.",
    ):
        _check_valid_month(month)


def test_check_valid_term() -> None:
    """Test of function _check_valid_term."""
    term = 7
    with pytest.raises(
        ValueError,
        match=f"The arg for term is bigger than possible max is 6 you have: {term}.",
    ):
        _check_valid_term(term)


def test_check_valid_quarter() -> None:
    """Test of function _check_valid_quarter."""
    quarter = 5
    with pytest.raises(
        ValueError,
        match=f"The arg for quarter is bigger than possible max is 4 you have: {quarter}.",
    ):
        _check_valid_quarter(quarter)


def test_check_valid_trimester() -> None:
    """Test of function _check_valid_trimester."""
    trimester = 5
    with pytest.raises(
        ValueError,
        match=f"The arg for trimester is bigger than possible max is 3 you have: {trimester}.",
    ):
        _check_valid_trimester(trimester)


def test_check_valid_half_year() -> None:
    """Test of function _check_valid_half_year."""
    half_year = 5
    with pytest.raises(
        ValueError,
        match=f"The arg for half_year is bigger than possible max is 2 you have: {half_year}.",
    ):
        _check_valid_half_year(half_year)


def test_check_valid_year() -> None:
    """Test of function _check_valid_year."""
    year1 = 2030
    year2 = 2020
    with pytest.raises(
        ValueError,
        match=f"The order of args is start date and then end date. Therefore first year arg can not be bigger than the last. Your args are start year:{year1}  end year:{year2}.",
    ):
        _check_valid_year(year1, year2)


def test_check_periodic_year() -> None:
    """Test of function check_periodic_year."""
    assert check_periodic_year(2024, 2021, 3)
    assert not check_periodic_year(2024, 2021, 2)
    assert check_periodic_year(2021, 2024, 3)
    assert check_periodic_year(2021, 2021, 3)
    assert check_periodic_year(2021, 2022, 1)
