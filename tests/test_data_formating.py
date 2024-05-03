import pandas as pd

from ssb_konjunk.data_formating import bytte_koder

"""Test av funksjon bytte_koder """


def test_bytte_koder() -> None:
    kode_dict = {"a": "1", "b": "2"}

    df = pd.DataFrame(
        {
            "bokstaver": ["a", "b"],
            "tall": [1, 2],
        }
    )

    df = bytte_koder(df, kode_dict, "bokstaver")

    assert df["bokstaver"].to_list() == ["1", "2"]

    assert df["tall"].to_list() == [1, 2]
