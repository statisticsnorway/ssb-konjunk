import pytest
import pandas as pd
import numpy as np

from ssb_konjunk.general_utils import round_df
from ssb_konjunk.general_utils import round_column
from ssb_konjunk.general_utils import round_number

"""Test of rounding functions"""

def test_round_df() -> None:
    # Test a df
    data = {
        'A': [1.234, 5.6789, 0.001, 3.14, 2.0],
        'B': [0.000123, 456.78, 123.4567, 0.12, 78.9]
    }
    df = pd.DataFrame(data)
    
    two_digits_data = {
        'A': [1.23, 5.68, 0.0, 3.14, 2.0],
        'B': [0.0, 456.78, 123.46, 0.12, 78.9]
    }
    two_digits_df = pd.DataFrame(two_digits_data)
    
    no_digits_data = {
        'A': [1.0, 6.0, 0.0, 3.0, 2.0],
        'B': [0.0, 457.0, 123.0, 0.0, 79.0]
    }
    no_digits_df = pd.DataFrame(no_digits_data)
    
    assert (round_df(df, 2)).equals(two_digits_df)
    assert (round_df(df, 0)).equals(no_digits_df)

    
def test_round_column() -> None:
    # Test a df
    data = {
        'A': [1.234, 5.6789, 0.001, 3.14, 2.0],
    }
    df = pd.DataFrame(data)
    
    two_digits_data = {
        'A': [1.23, 5.68, 0.0, 3.14, 2.0],
    }
    two_digits_df = pd.DataFrame(two_digits_data)
    
    no_digits_data = {
        'A': [1.0, 6.0, 0.0, 3.0, 2.0],
    }
    no_digits_df = pd.DataFrame(no_digits_data)
    
    assert (round_column(df['A'], 2)).equals(two_digits_df['A'])
    assert (round_column(df['A'], 0)).equals(no_digits_df['A'])
    
def test_round_number() -> None:
    # Test a df
    data =  123.4567
    
    two_digits_data =  123.46
    no_digits_data = 123.0
    
    assert (round_number(data, 2)) == (two_digits_data)
    assert (round_number(data, 0)) == (no_digits_data)
    