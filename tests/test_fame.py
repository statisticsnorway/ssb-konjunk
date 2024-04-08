import pandas as pd

from ssb_konjunk.fame import change_date_format_fame

"""Test of function change_date_format_fame"""


def test_change_date_format_fame() -> None:
    # Test with a valid year and month
    series = pd.Series(
        [
            "2024-01-01",
            "2024-01-02",
            "2024-01-03",
        ]
    )

    fame_series = pd.Series(
        [
            "2024:1:1",
            "2024:1:2",
            "2024:1:3",
        ]
    )

    assert change_date_format_fame(series).equals(fame_series)
