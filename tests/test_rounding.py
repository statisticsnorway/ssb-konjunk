"""Test of script rounding."""

import pytest
import pandas as pd

from ssb_konjunk.rounding import round_half_up_float
from ssb_konjunk.rounding import round_half_up

def test_round_half_up_float() -> None:
    """Test of function round_half_up_float."""
    assert round_half_up_float(2.223) == 2
    assert round_half_up_float(2.49) == 2
    assert round_half_up_float(2.499, 2) == 2.5
    
def test_round_half_up() -> None:
    """Test of function round_half_up."""
    df = pd.DataFrame({'a': [2.2, 2.49, 2.51], 'b': [3.3, 4.3, 0.9]})

    assert round_half_up(df['a'], '1.')[0] == 2
    assert round_half_up(df['a'], '1.')[2] == 3
    
    assert round_half_up(df['a'], '.1')[1] == 2.5
    assert round_half_up(df['a'], '.1')[2] == 2.5