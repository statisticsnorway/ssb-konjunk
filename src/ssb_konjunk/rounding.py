import math
from decimal import ROUND_HALF_UP
from decimal import Decimal
from typing import Any
import pandas as pd

def round_half_up_float(n: float, decimals: int = 0) -> Any:
    """Round a float half up.

    Function from https://realpython.com/python-rounding/.

    Args:
        n: the float to round off.
        decimals: the number of decimals to keep.

    Returns:
        float|int: the rounded off number.
    """
    multiplier = 10**decimals
    return math.floor(n * multiplier + 0.5) / multiplier


def round_half_up(df_col: pd.Series[float], digits: str = "1.") -> pd.Series[float]:
    """Round a pandas column half up.

    The "normal" (half up) rounding should be used.

    Args:
        df_col: a column in a data frame where all values will be rounded off.
        digits: number of digits after . gives the number of digits rounded off to. Default: no digits.

    Returns:
        Series: a column in a data frame where all values are rounded off
    """
    df_col = df_col.copy()
    df_col = df_col.map(
        lambda x: Decimal(x).quantize(Decimal(digits), rounding=ROUND_HALF_UP)
    )

    return df_col
