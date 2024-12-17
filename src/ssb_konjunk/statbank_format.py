import pandas as pd

from ssb_konjunk import prompts


def format_time_period(
    df: pd.DataFrame,
    year: int,
    quarter: int | str = "",
    col_name: str = "periode",
    month: int | str = "",
) -> pd.DataFrame:
    """Add column with time period.

    Args:
        df: dataframe.
        year: the year.
        quarter: optional, default ''.
        col_name: optional, default 'periode'. The name of the column for the time period.
        month: optional, default ''.

    Returns:
        pd.DataFrame: dataframe with a column with time period.
    """
    if quarter != "" and month == "":
        df[col_name] = f"{year}K{int(quarter)}"
    elif quarter == "" and month != "":
        df[col_name] = f"{year}M{prompts.validate_month(month)}"
    else:
        df[col_name] = f"{year}"

    return df


def remove_suppressed_numbers(
    df: pd.DataFrame, colname_value: str, colname_suppressed: str = "prikka"
) -> pd.DataFrame:
    """Remove values in column if marked as prikka (04).

    Args:
        df: dataframe.
        colname_value: the name of the column with potential values to remove.
        colname_suppressed: the name of the column that contains the code '04' if the row should be suppressed.

    Returns:
        pd.DataFrame: dataframe with removed values if suppressed.
    """
    df = df.copy()
    df.loc[df[colname_suppressed] == "04", colname_value] = ""
    df[colname_value] = df[colname_value].astype(str)

    return df
