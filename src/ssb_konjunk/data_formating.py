"""Funksjoner for å gjøre dataformartering."""

import pandas as pd


def bytte_koder(
    df: pd.DataFrame, kode_dict: dict[str, str], kolonnenavn: str
) -> pd.DataFrame:
    """Bytter koder.

    Funksjonen for å bytte kode i en kolonne.


    Args:
        df: Pandas dataramme som vi skal sende inn.
        kode_dict: Ordbok med gammel og ny kode.
        kolonnenavn: Navn på kolonnen som skal byttes ut.

    Returns:
        Dataramme med ny kode.

    """
    df_func = df.copy()

    for old, new in kode_dict.items():

        df_func.loc[df_func[kolonnenavn] == old, kolonnenavn] = str(new)

    return df_func
