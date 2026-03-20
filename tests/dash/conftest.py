import numpy as np
import pandas as pd
import polars as pl
import pytest

from ssb_konjunk.dash.calculations.calc_data import DataManager
from ssb_konjunk.dash.calculations.helper_functions import DataSource


@pytest.fixture(scope="module")
def test_df():
    np.random.seed(0)
    periods = pd.date_range("2021-11", "2024-12", freq="MS").strftime("%Y-%m")

    nars = ["H", "49.1", "49.2", "K", "64"]

    rows = []
    for nar in nars:
        for p in periods:
            rows.append(
                {
                    "nar": nar,
                    "periode": p,
                    "jus": np.random.uniform(80, 120),
                    "korr": np.random.uniform(80, 120),
                    "ujust": np.random.uniform(80, 120),
                    "verdi": np.random.uniform(10, 50),
                }
            )

    return pd.DataFrame(rows)


@pytest.fixture(scope="module")
def data(test_df):
    return DataManager(test_df)


@pytest.fixture
def test_df_datasource(test_df):
    polars_data = pl.from_pandas(test_df)
    polars_data = polars_data.with_columns(
        ujust=pl.col("ujust").cast(pl.Float64),
        jus=pl.col("jus").cast(pl.Float64),
        korr=pl.col("korr").cast(pl.Float64),
        periode=pl.col("periode").str.strptime(pl.Date, "%Y-%m", strict=False),
    )
    return DataSource(polars_data, "periode", "jus", "nar")
