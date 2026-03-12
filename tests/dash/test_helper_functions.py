from datetime import datetime

import pendulum
import polars as pl
import pytest

from ssb_konjunk.dash.calculations import helper_functions
from ssb_konjunk.dash.calculations.helper_functions import DataSource
from ssb_konjunk.dash.calculations.calc_data import DataManager

def test_monthdelta():
    assert helper_functions.monthdelta(datetime(2026, 3, 10), datetime(2026, 3, 1)) == 0
    assert helper_functions.monthdelta(datetime(2026, 6, 15), datetime(2026, 3, 10)) == 3
    assert helper_functions.monthdelta(datetime(2026, 1, 5), datetime(2026, 4, 10)) == -3
    assert helper_functions.monthdelta(datetime(2027, 5, 10), datetime(2026, 3, 10)) == 14
    assert helper_functions.monthdelta(datetime(2026, 12, 31), datetime(2026, 1, 1)) == 11

def test_parse_period():
    assert helper_functions.parse_period("2026-03") == datetime(2026, 3, 1)
    assert helper_functions.parse_period("1999-12") == datetime(1999, 12, 1)
    assert helper_functions.parse_period("2000-01") == datetime(2000, 1, 1)

def test_multi_join():
    df1 = pl.DataFrame({
        "id": [1, 2, 3],
        "value1": ["A", "B", "C"]
    })
    df2 = pl.DataFrame({
        "id": [2, 3, 4],
        "value2": ["X", "Y", "Z"]
    })
    df3 = pl.DataFrame({
        "id": [3, 4, 5],
        "value3": ["M", "N", "O"]
    })

    result = helper_functions.multi_join([df1, df2, df3], on="id", how="left")

    expected = pl.DataFrame({
        "id": [1, 2, 3],
        "value1": ["A", "B", "C"],
        "value2": [None, "X", "Y"],
        "value3": [None, None, "M"] 
    })

    assert result.equals(expected)

def test_latest_date(test_df):
    data = DataSource(
    pl.from_pandas(test_df).with_columns(
            pl.col("periode").str.strptime("%Y-%m", datatype=pl.Date, strict=False)
        ),
        "periode",
        "verdi",
        "nar"
    )
    #print(data.latest_date())
    assert 1 == 0