import math
from decimal import ROUND_HALF_UP
from decimal import Decimal

import pandas as pd


def round_half_up_float(n: float, decimals: int = 0) -> float | int:
    """Round a float half up.

    Function from https://realpython.com/python-rounding/.

    Args:
        n: the float to round off.
        decimals: the number of decimals to keep.

    Returns:
        float|int: the rounded off number.
    """
    multiplier = 10**decimals
    return float(math.floor(n * multiplier + 0.5) / multiplier)


def round_half_up(df: pd.DataFrame, column: str, digits: str = "1.") -> pd.DataFrame:
    """Round a pandas column half up.

    The "normal" (half up) rounding should be used.

    Args:
        df: a column in a data frame where all values will be rounded off.
        column: name of the column to round off values in.
        digits: number of digits after . gives the number of digits rounded off to. Default: no digits.

    Returns:
        Series: a column in a data frame where all values are rounded off
    """
    df = df.copy()

    df[column] = df[column].map(
        lambda x: (
            float(Decimal(str(x)).quantize(Decimal(digits), rounding=ROUND_HALF_UP))
            if pd.notna(x)
            else x
        )
    )
    return df
