from datetime import datetime

import pytest

from ssb_konjunk_fagfunksjoner.prompts import days_in_month
from ssb_konjunk_fagfunksjoner.prompts import extract_start_end_dates
from ssb_konjunk_fagfunksjoner.prompts import next_month

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


"""Tests of function next_month"""


def test_next_month_valid() -> None:
    # Test with a valid year and month
    desired_year = 2023
    desired_month = 11
    expected_next_month = "2023-12"

    assert next_month(desired_year, desired_month) == expected_next_month


def test_next_month_edge_case() -> None:
    # Test with December, expecting January of the next year
    desired_year = 2022
    desired_month = 12
    expected_next_month = "2023-01"

    assert next_month(desired_year, desired_month) == expected_next_month


def test_next_month_invalid() -> None:
    # Test with an invalid month (13), expecting an error
    with pytest.raises(ValueError, match="month must be in 1..12"):
        next_month(2022, 13)
