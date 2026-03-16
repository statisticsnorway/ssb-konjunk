from datetime import datetime

import polars as pl

from ssb_konjunk.dash.calculations import helper_functions


def test_monthdelta():
    assert helper_functions.monthdelta(datetime(2026, 3, 10), datetime(2026, 3, 1)) == 0
    assert (
        helper_functions.monthdelta(datetime(2026, 6, 15), datetime(2026, 3, 10)) == 3
    )
    assert (
        helper_functions.monthdelta(datetime(2026, 1, 5), datetime(2026, 4, 10)) == -3
    )
    assert (
        helper_functions.monthdelta(datetime(2027, 5, 10), datetime(2026, 3, 10)) == 14
    )
    assert (
        helper_functions.monthdelta(datetime(2026, 12, 31), datetime(2026, 1, 1)) == 11
    )


def test_parse_period():
    assert helper_functions.parse_period("2026-03") == datetime(2026, 3, 1)
    assert helper_functions.parse_period("1999-12") == datetime(1999, 12, 1)
    assert helper_functions.parse_period("2000-01") == datetime(2000, 1, 1)


def test_multi_join():
    df1 = pl.DataFrame({"id": [1, 2, 3], "value1": ["A", "B", "C"]})
    df2 = pl.DataFrame({"id": [2, 3, 4], "value2": ["X", "Y", "Z"]})
    df3 = pl.DataFrame({"id": [3, 4, 5], "value3": ["M", "N", "O"]})

    result = helper_functions.multi_join([df1, df2, df3], on="id", how="left")

    expected = pl.DataFrame(
        {
            "id": [1, 2, 3],
            "value1": ["A", "B", "C"],
            "value2": [None, "X", "Y"],
            "value3": [None, None, "M"],
        }
    )

    assert result.equals(expected)


def test_DataSource_init(test_df_datasource):

    assert test_df_datasource._date is not None
    assert test_df_datasource._col is not None
    assert test_df_datasource._group is not None
    assert test_df_datasource._avg is not None
    assert test_df_datasource._dt_out_format is not None


def test_latest_date(test_df_datasource):
    test = test_df_datasource.latest_date()
    assert test == datetime(2024, 12, 1)


def test_percent_change(test_df_datasource):
    df = pl.DataFrame(
        {
            "s1": [110, 200, 300],
            "s2": [100, 150, 250],
        }
    )

    expected = pl.DataFrame({"pct": [10.0, 33.333333, 20.0]})

    result = df.select(
        pct=test_df_datasource._percent_change(pl.col("s1"), pl.col("s2"))
    )
    assert result.select(pl.all().round(6)).equals(expected.select(pl.all().round(6)))


def test_create_date(test_df_datasource):
    expected = [
        "Jan 2024",
        "Feb 2024",
        "Mar 2024",
        "Apr 2024",
        "May 2024",
        "Jun 2024",
        "Jul 2024",
        "Aug 2024",
        "Sep 2024",
        "Oct 2024",
        "Nov 2024",
        "Dec 2024",
    ]
    for i, exp in enumerate(expected, start=1):
        result = test_df_datasource._create_date(datetime(2024, i, 1))
        assert result == exp


def test_gen_header(test_df_datasource):
    header_1 = test_df_datasource._gen_header(1)
    header_3 = test_df_datasource._gen_header(3)
    header_12 = test_df_datasource._gen_header(12)
    assert header_1 == "Nov 2024 - Dec 2024"
    assert header_3 == "Sep 2024 - Dec 2024"
    assert header_12 == "Dec 2023 - Dec 2024"
