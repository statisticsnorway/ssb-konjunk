"""A collection of useful functions.

The template and this example uses Google style docstrings as described at:
https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html

"""

import pandas as pd


def example_function(number1: int, number2: int) -> str:
    """Compare two integers.

    This is merely an example function can be deleted. It is used to show and test generating
    documentation from code, type hinting, testing, and testing examples
    in the code.


    Args:
        number1: The first number.
        number2: The second number, which will be compared to number1.

    Returns:
        A string describing which number is the greatest.

    Examples:
        Examples should be written in doctest format, and should illustrate how
        to use the function.

        >>> example_function(1, 2)
        1 is less than 2

    """
    if number1 < number2:
        return f"{number1} is less than {number2}"

    return f"{number1} is greater than or equal to {number2}"


def remove_dot_nace(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """Function to remove dot from nace. Vof uses for example 47.111 where as som bases have 47111, so its sometimes nessecary to remove the dot.

    Parameters
    ----------
        df (pd.DataFrame): Pandas df to work on.
        column_name (str): String of column name for containing nace with dot.

    Returns:
    -------
        df (pd.DataFrame): Pandas df without dot in nace column.
    """
    df = df.copy()
    df[column_name] = df[column_name].str.replace(r"^(..).(.*)$", r"\1\2", regex=True)
    return df
