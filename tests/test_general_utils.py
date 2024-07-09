import pytest
import pandas as pd
import numpy as np

#from ssb_konjunk.general_utils import round_df
#from ssb_konjunk.general_utils import round_column
#from ssb_konjunk.general_utils import round_number

def round_df(df: pd.DataFrame, n_digits: int) -> pd.DataFrame:
    """Round off all possible columns in a dataframe.
    
    Args:
        df: dataframe with columns to round off.
        n_digits: the number of digits to keep.
    
    Returns:
        pd.DataFrame: a df with the rounded columns. 
    """
    df = df.copy()
    
    for col in df.columns:
        try:
            df[col] = round_column(df[col], n_digits)
        except Exception as e:
            print(f"Column {col} are not rounded off, due to {e}.")
    
    return df

def round_column(column: pd.Series, n_digits: int) -> pd.Series:
    """Round off a column in a dataframe.
    
    Args:
        column: dataframe with columns to round off.
        n_digits: the number of digits to keep.
    
    Returns:
        pd.Series: the rounded column. 
    """
    column = np.round(column, n_digits)
    
    return column

def round_number(number: float, n_digits: int) -> float:
    """Round off a single number.
    
    Args:
        number: number to round off.
        n_digits: the number of digits to keep.
    
    Returns:
        float: the rounded number. 
    """
    number = np.round(number, n_digits)
    
    return number

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
    