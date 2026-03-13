import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def test_df():
    np.random.seed(0)
    periods = pd.date_range("2023-12", "2024-12", freq="MS").strftime("%Y-%m")

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
