"""Test of script rounding."""

import pytest

from ssb_konjunk.rounding import round_half_up_float
from ssb_konjunk.rounding import round_half_up

def test_round_half_up_float() -> None:
    """Test of function round_half_up_float."""
    assert round_half_up_float(2.223) == 2
    assert round_half_up_float(2.49) == 2
    assert round_half_up_float(2.499, 2) == 2.5