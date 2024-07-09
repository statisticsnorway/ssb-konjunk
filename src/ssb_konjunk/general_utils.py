"""Functions for general utils in python.

This script includes rounding functions with the half-up method.
"""

import pandas as pd
import numpy as np


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