from datetime import date
from typing import Literal

import polars as pl

from .loading_test import AGG_TYPES


class GenVisData:
    """Representerer en datakilde tilrettelagt for tidsbasert analyse og gruppering."""
    def __init__(self, filename: str, index_col: str, date_pattern: str) -> None:
        """Representerer en datakilde tilrettelagt for tidsbasert analyse og gruppering.

        Klassen leser et datasett fra en Parquet-fil og oppretter en standardisert
        datokolonne (`dt`) basert på en eksisterende kolonne. Den tilbyr metoder for:
    
        - filtrering på datointervall
        - uthenting av unike datoer eller grupperingsverdier
        - filtrering av delmengder basert på grupper
        - indeksberegning med et valgt basisår (base = 100)
        """
        self.dt_colname = "dt"
        self.data = pl.read_parquet(filename).with_columns(
            dt=pl.col(index_col).cast(pl.String).str.to_date(date_pattern)
        )

    def filter_dt(self, lower: date, highest: date) -> pl.DataFrame:
        """Filtrerer datasettet til rader med dato mellom to grenser.

        Args:
            lower: Nedre datogrense (inkludert).
            highest: Øvre datogrense (inkludert).
    
        Returns:
            pl.DataFrame: Datasett filtrert til angitt datointervall.
        """
        return self.data.filter(pl.col(self.dt_colname).is_between(lower, highest))

    def get_unique_dates(self) -> list[date]:
        """Returnerer en liste med alle unike datoer i datasettet.

        Returns:
            list[date]: Unike datoer fra datokolonnen.
        """
        return self.data.get_column(self.dt_colname).unique().to_list()

    def get_unique_groupby(self, groupby_col: str) -> list:
        """Returnerer unike verdier fra en kolonne brukt til gruppering.

        Args:
            groupby_col: Navn på kolonnen.
    
        Returns:
            list: Liste med unike verdier i kolonnen.
        """
        return self.data.get_column(groupby_col).unique().to_list()

    def subset_group(self, groupby_col: str, filter_val) -> pl.DataFrame:
        """Returnerer et delsett av datasettet filtrert på en kolonneverdi.

        Args:
            groupby_col: Kolonnen det filtreres på.
            filter_val: Verdien kolonnen må være lik.
    
        Returns:
            pl.DataFrame: Filtrert datasett.
        """
        return self.data.filter(pl.col(groupby_col) == filter_val)

    def _set_base_year(
        self,
        data: pl.DataFrame,
        year: str,
        col: str,
        method: Literal["discrete", "none"],
        agg_type: AGG_TYPES,
    ) -> pl.DataFrame:
        """Setter et basisår for en kolonne slik at nivået i basisåret blir 100.
    
        Indeksfaktoren beregnes fra verdiene i det angitte året. Avhengig av
        metode og aggregering brukes enten gjennomsnitt eller sum.
    
        Args:
            data: Datasettet som skal indekseres.
            year: Året som skal brukes som basisår.
            col: Kolonnen som skal indekseres.
            method: Metode for beregning av indeksfaktor.
            agg_type: Aggregeringstype brukt ved beregning av indeksfaktor.
    
        Returns:
            pl.DataFrame: Datasettet der kolonnen er omregnet til en indeks
            med basisår = 100.
        """
        filtered = data.filter(
            (pl.col(self.dt_colname) < date(int(year), 12, 31))
            & (pl.col(self.dt_colname) >= date(int(year), 1, 1))
        )

        selected_col = filtered.get_column(col).cast(pl.Float64)
        if method == "discrete":
            if agg_type == "AVERAGE":
                index_factor = selected_col.mean()
            else:
                index_factor = selected_col.sum()
        else:
            index_factor = selected_col.mean()

        altered = data.with_columns(
            **{col: (pl.col(col).cast(pl.Float64) / index_factor) * 100}
        )
        return altered

    def set_base_year(
        self,
        data: pl.DataFrame,
        year: str,
        col: str,
        group_col: str | None,
        method: Literal["discrete", "none"],
        agg_mapping: AGG_TYPES | dict[str, AGG_TYPES],
    ) -> pl.DataFrame:
        """Setter basisår for en kolonne, eventuelt per gruppe.

        Metoden beregner en indeks slik at verdinivået i basisåret blir 100.
        Hvis `group_col` er satt, beregnes indeksen separat for hver gruppe.
    
        Args:
            data: Datasettet som skal indekseres.
            year: Året som skal brukes som basisår.
            col: Kolonnen som skal indekseres.
            group_col: Kolonne brukt til gruppering. Hvis None brukes hele datasettet.
            method: Metode for beregning av indeksfaktor.
            agg_mapping: Aggregeringstype eller mapping fra kolonnenavn til aggregeringstype.
    
        Returns:
            pl.DataFrame: Datasettet med kolonnen omregnet til indeks med basisår = 100.
        """
        if isinstance(agg_mapping, dict):
            agg_type = agg_mapping.get(col, "AVERAGE")
        elif isinstance(agg_mapping, str):
            agg_type = agg_mapping
        else:
            raise ValueError("")

        if group_col is not None:
            return data.group_by(group_col).map_groups(
                lambda x: self._set_base_year(x, year, col, method, agg_type)
            )
        else:
            return self._set_base_year(data, year, col, method, agg_type)
