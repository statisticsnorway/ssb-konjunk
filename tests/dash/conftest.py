import numpy as np
import pandas as pd
import pytest

from ssb_konjunk.dash.calculations.calc_data import DataManager


@pytest.fixture(scope="module")
def test_df_data():
    np.random.seed(0)

    periods = pd.date_range("2021-11", "2024-12", freq="MS").strftime("%Y-%m")

    nars = ["H", "49.1", "49.2", "J", "59"]

    sesongjusteringer = ["N", "T", "W", "Y"]

    rows = []

    for nar in nars:
        for periode in periods:
            for sesongjustering in sesongjusteringer:
                rows.append(
                    {
                        "nar": nar,
                        "periode": periode,
                        "sesongjustering": sesongjustering,
                        "volumindeks": np.random.uniform(80, 120),
                    }
                )

    return pd.DataFrame(rows)


@pytest.fixture(scope="module")
def test_df_vekt():
    np.random.seed(0)

    periods = pd.date_range("2021-11", "2024-12", freq="MS").strftime("%Y-%m")

    nars = ["H", "49.1", "49.2", "J", "59"]
    rows = []

    for nar in nars:
        for periode in periods:
            rows.append(
                {
                    "nar": nar,
                    "periode": periode,
                    "vekt": (
                        np.random.uniform(40, 60)
                        if nar in ["H", "J"]
                        else np.random.uniform(10, 30)
                    ),
                }
            )

    return pd.DataFrame(rows)


@pytest.fixture(scope="module")
def test_df_vekt_marked():
    np.random.seed(0)

    periods = pd.date_range("2021-11", "2024-12", freq="MS").strftime("%Y-%m")
    nars = ["H", "49.1", "49.2", "J", "59"]
    markeder = ["eksport", "import"]

    rows = []
    for nar in nars:
        for periode in periods:
            for marked in markeder:
                rows.append(
                    {
                        "nar": nar,
                        "periode": periode,
                        "marked": marked,
                        "vekt": (
                        np.random.uniform(40, 60)
                        if nar in ["H", "J"]
                        else np.random.uniform(10, 30)
                    ),
                    }
                )

    return pd.DataFrame(rows)

@pytest.fixture(scope="module")
def data(test_df_data, test_df_vekt):
    return DataManager(
        data=test_df_data,
        weight_data=test_df_vekt,
        period_column="periode",
        group_column="nar",
        value_columns=["volumindeks"],
        categorical_columns=["sesongjustering"],
    )

@pytest.fixture
def data_with_marked(test_df_data, test_df_vekt_marked):
    return DataManager(
        data=test_df_data,
        weight_data=test_df_vekt_marked,
        period_column="periode",
        group_column="nar",
        value_columns=["volumindeks"],
        categorical_columns=["sesongjustering"],
    )
    
@pytest.fixture
def test_df_datasource(data):
    return data.get_source(
        value_column="volumindeks",
        categorical_columns_filter={
            "sesongjustering": "Y",
        },
    )
