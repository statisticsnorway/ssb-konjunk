"""Test of script rounding."""

import pandas as pd

from ssb_konjunk.rounding import round_half_up
from ssb_konjunk.rounding import round_half_up_float


def test_round_half_up_float() -> None:
    """Test of function round_half_up_float."""
    assert round_half_up_float(2.223) == 2.0
    assert round_half_up_float(2.49) == 2.0
    assert round_half_up_float(2.499, 2) == 2.5


def test_round_half_up() -> None:
    """Test of function round_half_up."""
    df = pd.DataFrame({"a": [1.45, 1.51]})

    assert round_half_up(df, "a")["a"][0] == 1.0
    assert round_half_up(df, "a")["a"][1] == 2.0

    assert round_half_up(df, "a", ".1")["a"][0] == 1.5
    assert round_half_up(df, "a", ".1")["a"][1] == 1.5
