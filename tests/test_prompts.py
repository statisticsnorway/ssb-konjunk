from datetime import datetime

import pytest

from ssb_konjunk.prompts import days_in_month
from ssb_konjunk.prompts import extract_start_end_dates
from ssb_konjunk.prompts import iterate_years_months
from ssb_konjunk.prompts import validate_month

"""Test of function days in month"""


def test_days_in_month_valid() -> None:
    # Test with a valid year and month
    year = 2024
    month = 1
    expected_days = [
        "2024-01-01",
        "2024-01-02",
        "2024-01-03",
        "2024-01-04",
        "2024-01-05",
        "2024-01-06",
        "2024-01-07",
        "2024-01-08",
        "2024-01-09",
        "2024-01-10",
        "2024-01-11",
        "2024-01-12",
        "2024-01-13",
        "2024-01-14",
        "2024-01-15",
        "2024-01-16",
        "2024-01-17",
        "2024-01-18",
        "2024-01-19",
        "2024-01-20",
        "2024-01-21",
        "2024-01-22",
        "2024-01-23",
        "2024-01-24",
        "2024-01-25",
        "2024-01-26",
        "2024-01-27",
        "2024-01-28",
        "2024-01-29",
        "2024-01-30",
        "2024-01-31",
    ]

    assert days_in_month(year, month) == expected_days


def test_days_in_month_invalid() -> None:
    # Test with an invalid month
    year = 2022
    month = 13  # Invalid month
    with pytest.raises(ValueError, match="bad month number 13; must be 1-12"):
        days_in_month(year, month)


"""Test of function extract_start_end_dates"""


def test_extract_start_end_dates_valid() -> None:
    # Test with a valid file name
    file_name = "data_p2022-01-01_p2022-01-31_report.csv"
    expected_start_date = datetime(2022, 1, 1)
    expected_end_date = datetime(2022, 1, 31)

    start_date, end_date = extract_start_end_dates(file_name)

    assert start_date == expected_start_date
    assert end_date == expected_end_date


"""Test of function iterate_years_months"""


def test_iterate_years_months_full_range() -> None:
    # Test when providing a full range of years and months
    expected_output = [
        (2024, 1),
        (2024, 2),
        (2024, 3),
        (2024, 4),
        (2024, 5),
        (2024, 6),
        (2024, 7),
        (2024, 8),
        (2024, 9),
        (2024, 10),
        (2024, 11),
        (2024, 12),
        (2025, 1),
    ]
    assert list(iterate_years_months(2024, 2025, 1, 1)) == expected_output


def test_iterate_years_months_partial_range() -> None:
    # Test when providing a partial range of years and months
    expected_output = [(2023, 11), (2023, 12), (2024, 1), (2024, 2)]
    assert list(iterate_years_months(2023, 2024, 11, 2)) == expected_output


def test_iterate_years_months_one_period() -> None:
    # Test when providing a partial range of years and months
    expected_output = [(2024, 2)]
    assert list(iterate_years_months(2024, 2024, 2, 2)) == expected_output


def test_iterate_years_months_invalid_range() -> None:
    # Test when providing an invalid range where start year > end year
    with pytest.raises(ValueError):
        list(iterate_years_months(2024, 2022, 1, 12))

    # Test when providing an invalid range where start month > end month
    with pytest.raises(ValueError):
        list(iterate_years_months(2024, 2024, 6, 1))


def test_iterate_years_months_invalid_month() -> None:
    # Test when providing an invalid month (greater than 12)
    with pytest.raises(ValueError):
        list(iterate_years_months(2022, 2024, 1, 13))

    # Test when providing an invalid month (less than 1)
    with pytest.raises(ValueError):
        list(iterate_years_months(2022, 2024, 0, 12))


def test_bump_quarter() -> None:
    # Test bump quarter
    assert bump_quarter(year = 2023, quarter = 4) == (2024, 1), bump_quarter(year = 2023, quarter = 4)
    assert bump_quarter(year = 2023, quarter = 1) == (2023, 2), bump_quarter(year = 2023, quarter = 1)


def test_validate_month() -> None:
    # Test for variations of january
    assert validate_month(1) == "01"
    assert validate_month("1") == "01"
    assert validate_month("01") == "01"

    assert validate_month(10) == "10"
    assert validate_month("10") == "10"
