from datetime import date
from datetime import datetime
from typing import Literal

import pandas as pd
import pendulum
import polars as pl


def monthdelta(d1: datetime, d2: datetime) -> int:
    """Finner differansen mellom to måneder."""
    d1_ = pendulum.instance(d1)
    d2_ = pendulum.instance(d2)
    return (d1_ - d2_).in_months()


def parse_period(period: str) -> datetime:
    """Parser en periode til datetime."""
    return datetime.strptime(period, "%Y-%m")


def multi_join(
    dfs: list[pl.DataFrame],
    on: str,
    how: Literal["left", "right", "inner", "cross", "semi", "anti"] = "left",
) -> pl.DataFrame:
    """Slår sammen flere Polars DataFrames sekvensielt på en spesifisert kolonne.

    Utfører en kjedet sammenslåing (join) av flere DataFrames i en gitt liste basert på én felles kolonne.
    Hver DataFrame legges til én etter én, med spesifisert join-type og automatisk suffiks for overlappende kolonnenavn.

    Args:
        dfs (list[pl.DataFrame]): Liste over DataFrames som skal slås sammen. Første element brukes som base.
        on (str): Navn på kolonnen det skal joines på.
        how (Literal): Join-strategi. Kan være en av 'left', 'right', 'inner', 'cross', 'semi', eller 'anti'. Standard er 'left'.

    Returns:
        pl.DataFrame: Ett samlet DataFrame etter sekvensiell join av alle inputtabellene.
    """
    df = dfs[0]

    for idx, i in enumerate(dfs[1:]):
        df = df.join(i, on=on, how=how, suffix=str(idx))
    return df


class DataSource:
    """Representerer en datakilde tilrettelagt for tidsbasert analyse og gruppering."""

    def __init__(
        self,
        data: pl.DataFrame,
        date_col: str,
        col_name: str,
        group_by: str,
        internal_col: str = "avg",
        dt_out_format: str = "%b %Y",
    ) -> None:
        """Representerer en datakilde tilrettelagt for tidsbasert analyse og gruppering.

        Denne klassen organiserer et Polars DataFrame ved å sortere på en datokolonne
        og gjør det mulig å gruppere og analysere utvalgte kolonner.

        Attributes:
            data (pl.DataFrame): Det sorterte inndata-DataFrame.
            _date (str): Navn på kolonnen som inneholder datoer.
            _col (str): Navn på kolonnen som skal analyseres eller visualiseres.
            _group (str): Navn på kolonnen som brukes til å gruppere data.
            _avg (str): Internt kolonnenavn brukt for aggregering. Standard er "avg".
            _dt_out_format (str): Datoutskriftsformat for visning. Standard er "%b %Y".

        Args:
            data (pl.DataFrame): Dataen som skal behandles.
            date_col (str): Navn på kolonnen som inneholder datoer.
            col_name (str): Navn på kolonnen det skal gjøres analyser på.
            group_by (str): Kolonnen som brukes til gruppering.
            internal_col (str, valgfritt): Internt kolonnenavn brukt for aggregering. Standard er "avg".
            dt_out_format (str, valgfritt): Format for datoer ved visning. Standard er "%b %Y".
        """
        self.data = data.sort(date_col)
        self._date = date_col
        self._col = col_name
        self._group = group_by
        self._avg = internal_col
        self._dt_out_format = dt_out_format

    def latest_date(self) -> None | datetime:
        """Henter den siste datoen fra datakolonnen.

        Returns:
            datetime | None: Den siste datoen som finnes i datasettet,
            eller None dersom verdien ikke er en gyldig datetime.
        """
        latest = self.data.get_column(self._date).last()
        print(latest)
        if isinstance(latest, datetime):
            return latest
        elif isinstance(latest, date):  # <- use date directly
            return datetime.combine(latest, datetime.min.time())
        else:
            return None

    def _percent_change(self, series_1: pl.Expr, series_2: pl.Expr):
        """Beregner prosentvis endring mellom to serier.

        Args:
            series_1 (pl.Expr): Første uttrykk / serie.
            series_2 (pl.Expr): Andre uttrykk / serie som sammenlignes mot.

        Returns:
            pl.Expr: Et uttrykk som representerer prosentvis endring.
        """
        return ((series_1 - series_2) / series_2) * 100

    def _create_date(self, date: date) -> str:
        """Formaterer en datetime til en str med definert utdataformat.

        Args:
            date (datetime): Datoen som skal formateres.

        Returns:
            str: Formatert og kapitalisert dato som tekst.
        """
        return date.strftime(self._dt_out_format).capitalize()

    def _gen_header(self, n: int, skip: int = 0):
        """Genererer en overskrift som viser datoperioden basert på antall perioder og hopp.

        Args:
            n (int): Antall måneder per periode.
            skip (int, valgfritt): Antall perioder som skal hoppes over bakover i tid. Standard er 0.

        Returns:
            str: En str som representerer datoperioden (eks. "Jan 2023 - Mar 2023").
        """
        dates = self.data.get_column(self._date).unique().sort()
        latest: date = dates[-1 - (n * skip)]
        oldest: date = dates[-1 - ((n * skip) + n)]
        return f"{self._create_date(oldest)} - {self._create_date(latest)}"

    def _base(self, n: int, *agg: pl.Expr, **named_aggs: pl.Expr):
        """Utfører aggregering over dynamiske tidsvinduer og grupper.

        Args:
            n (int): Størrelsen på tidsvinduet i måneder.
            *agg: Ikke-navngitte aggregasjonsoperasjoner.
            **named_aggs: Navngitte aggregasjonsoperasjoner.

        Returns:
            pl.DataFrame: En DataFrame med grupperte og aggregerte verdier.
        """
        return (
            self.data.group_by_dynamic(
                self._date, every=f"{n}mo", group_by=self._group, closed="right"
            )
            .agg(**{self._avg: pl.col(self._col).mean().round(1)})
            .group_by(self._group)
            .agg(*agg, **named_aggs)
        )

    def _base_w_header(
        self, n: int, skip: int = 0, *agg: pl.Expr, **named_aggs: pl.Expr
    ):
        """Genererer en overskrift for perioden og returnerer resultatet fra baseaggregering.

        Kombinerer datoperiode-headeren med aggregerte resultater fra `_base`.

        Args:
            n (int): Antall måneder i hver gruppeperiode.
            skip (int, valgfritt): Antall perioder som skal hoppes over fra siste periode. Standard er 0.
            *agg: Ikke-navngitte aggregeringsuttrykk.
            **named_aggs: Navngitte aggregasjonsuttrykk.

        Returns:
            tuple[str, pl.DataFrame]: En tuple bestående av overskrift og det aggregerte datasettet.
        """
        header = self._gen_header(n, skip)
        result = self._base(n, *agg, **named_aggs)
        return header, result

    def n_month(self, n: int, skip: int = 0) -> tuple[str, pl.DataFrame]:
        """Henter siste tilgjengelige verdi for gjennomsnittskolonnen for en periode.

        Args:
            n (int): Antall måneder i perioden.
            skip (int, valgfritt): Antall perioder som skal hoppes over bakover i tid. Standard er 0.

        Returns:
            tuple[str, pl.DataFrame]: En tuple med overskrift og filtrert datasett med siste verdi.
        """
        return self._base_w_header(
            n, skip, **{self._avg: pl.col(self._avg).get(-1 - skip)}
        )

    def n_month_percent(self, n: int, skip: int = 0) -> tuple[str, pl.DataFrame]:
        """Beregner prosentvis endring mellom to perioder og returnerer med overskrift.

        Args:
            n (int): Antall måneder i perioden.
            skip (int, valgfritt): Antall perioder som skal hoppes over bakover i tid. Standard er 0.

        Returns:
            tuple[str, pl.DataFrame]: En tuple med periodebeskrivelse og datasett med prosentendringer.
        """
        return self._base_w_header(
            n,
            skip,
            **{
                self._avg: self._percent_change(
                    pl.col(self._avg), pl.col(self._avg).shift(1)
                ).get(-1 - skip)
            },
        )

    def n_percent_rolling(self, n: int, skip: int = 0) -> tuple[str, pl.DataFrame]:
        """Beregner rullerende prosentvis endring over en periode og returnerer med datoperiode-header.

        Denne metoden bruker en rullerende tidsvinduanalyse for å beregne prosentvis endring
        mellom første og siste verdi i hvert vindu, og gir samtidig en datoperiodebeskrivelse.
        Håndterer manglende verdier ved å fylle dem bakover dersom vinduet er ufullstendig.

        Args:
            n (int): Lengden på rullevinduet i måneder.
            skip (int, valgfritt): Antall perioder som skal hoppes over fra nyeste dato. Standard er 0.

        Returns:
            tuple[str, pl.DataFrame]: En tuple med en tekstlig overskrift for datoperioden og et DataFrame
            med prosentvis endring for hver gruppe.
        """

        def _gen_header(n: int, skip: int = 0):
            """Lager overskrift for hver perioden."""
            dates = self.data.get_column(self._date).unique().sort()
            latest: date = dates[-1 - (skip)]
            oldest: date = dates[-1 - ((skip) + n - 1)]
            return f"{self._create_date(oldest)} - {self._create_date(latest)}"

        def map_test(x: pl.DataFrame):
            """Lager det rullende gjennomsnittet for hver periodegruppe."""
            if x.shape[0] != n:
                x = x.with_columns(
                    **{self._avg: pl.col(self._col).fill_null(strategy="backward")}
                )
            else:
                x = x.with_columns(
                    **{
                        self._avg: self._percent_change(
                            pl.col(self._col), pl.col(self._col).shift(n - 1)
                        ).get(-1)
                    }
                )
            return x

        return _gen_header(n, skip), (
            self.data.rolling(
                pl.col(self._date),
                period=f"{n}mo",
                closed="right",
                group_by=self._group,
                offset=f"-{skip+n}mo",
            )
            .map_groups(map_test, None)
            .group_by(self._group)
            .agg(pl.col(self._avg).tail(1))
            .explode(self._avg)
        )

    def n_mean_rolling(self, n: int, skip: int = 0) -> tuple[str, pl.DataFrame]:
        """Beregner et rullerende gjennomsnitt for hver gruppe i datasettet og returnerer med datoperiode-header.

        Denne metoden beregner gjennomsnittet av verdiene innenfor et rullerende vindu på `n` måneder.
        Hvis et vindu inneholder færre enn `n` datapunkter, blir manglende verdier fylt bakover.
        Returnerer resultatene sammen med en overskrift som beskriver datoperioden.

        Args:
            n (int): Lengden på rullevinduet i måneder.
            skip (int, valgfritt): Antall perioder som skal hoppes over fra nyeste tilgjengelige dato. Standard er 0.

        Returns:
            tuple[str, pl.DataFrame]: En tuple bestående av datoperiode-header og et DataFrame
            med rullerende gjennomsnittsverdier for hver gruppe.
        """

        def _gen_header(n: int, skip: int = 0):
            """Lager overskrift for hver perioden."""
            dates = self.data.get_column(self._date).unique().sort()
            latest: date = dates[-1 - (skip)]
            oldest: date = dates[-1 - ((skip) + n - 1)]
            return f"{self._create_date(oldest)} - {self._create_date(latest)}"

        def map_test(x: pl.DataFrame):
            """Lager det rullende gjennomsnittet for hver periodegruppe."""
            if x.shape[0] != n:
                x = x.with_columns(
                    **{self._avg: pl.col(self._col).fill_null(strategy="backward")}
                )
            else:
                x = x.with_columns(**{self._avg: pl.col(self._col).mean()})
            return x

        return _gen_header(n, skip), (
            self.data.rolling(
                pl.col(self._date),
                period=f"{n}mo",
                closed="right",
                group_by=self._group,
                offset=f"-{skip+n}mo",
            )
            .map_groups(map_test, None)
            .group_by(self._group)
            .agg(pl.col(self._avg).tail(1))
            .explode(self._avg)
        )

    def n_month_rolling_percent_compare(
        self, n: int, skip: int = 0, skip_1: int = 1
    ) -> tuple[str, pl.DataFrame]:
        """Sammenligner rullerende gjennomsnitt mellom to perioder og beregner prosentvis endring.

        Denne metoden sammenligner det rullerende gjennomsnittet for én periode mot en annen forskjøvet
        periode, og returnerer en prosentvis endring for hver gruppe. Resultatet inkluderer også en
        overskrift som beskriver begge periodene.

        Args:
            n (int): Lengden på hver rullerende periode i måneder.
            skip (int, valgfritt): Antall perioder å hoppe over for den nyeste perioden. Standard er 0.
            skip_1 (int, valgfritt): Antall perioder å hoppe over for den sammenlignende perioden. Standard er 1.

        Returns:
            tuple[str, pl.DataFrame]: En tuple bestående av en sammensatt overskrift og et DataFrame
            med prosentvis endring mellom de to periodene for hver gruppe.
        """
        header_1, series_1 = self.n_mean_rolling(n, skip)
        header_2, series_2 = self.n_mean_rolling(n, skip_1)

        header = f"{header_2} / {header_1}"
        joined = series_1.join(series_2, on=self._group).select(
            **{
                self._group: pl.col(self._group),
                self._avg: self._percent_change(
                    pl.col(self._avg), pl.col(f"{self._avg}_right")
                ),
            }
        )
        return header, joined

    def n_month_percent_compare(
        self, n: int, skip: int = 0, skip_1: int = 1
    ) -> tuple[str, pl.DataFrame]:
        """Sammenligner prosentvis endring i verdier mellom to definerte perioder.

        Denne metoden henter to distinkte perioder (hver på `n` måneder), basert på ulike skipverdier,
        og beregner den prosentvise forskjellen i verdier mellom dem for hver gruppe i datasettet.
        Returnerer en overskrift som viser hvilke perioder som sammenlignes, samt et datasett med endringene.

        Args:
            n (int): Lengde på hver periode målt i måneder.
            skip (int, valgfritt): Antall perioder som hoppes over fra nyeste dato for første datasett. Standard er 0.
            skip_1 (int, valgfritt): Antall perioder som hoppes over for sammenligningsdatasettet. Standard er 1.

        Returns:
            tuple[str, pl.DataFrame]: En tuple med beskrivelse av periodeparene og et DataFrame
            med prosentvis endring mellom verdiene for hver gruppe.
        """
        header_1, series_1 = self.n_month(n, skip)
        header_2, series_2 = self.n_month(n, skip_1)

        header = f"{header_2} / {header_1}"

        joined = series_1.join(series_2, on=self._group).select(
            **{
                self._group: pl.col(self._group),
                self._avg: self._percent_change(
                    pl.col(self._avg), pl.col(f"{self._avg}_right")
                ),
            }
        )
        return header, joined


def rounded_average(df: pd.DataFrame, ordered_columns: list[str]) -> pd.Series:
    """Beregner gjennomsnitt per rad for utvalgte kolonner, avrundet til én desimal.

    Args:
        df (pd.DataFrame): DataFrame med data.
        ordered_columns (list[str]): Liste med kolonnenavn som skal inkluderes i gjennomsnittet.

    Returns:
        pd.Series: Gjennomsnitt per rad, avrundet til én desimal.
    """
    df_copy = df[ordered_columns]
    df_copy = df_copy.round(1)
    res = df_copy.sum(axis="columns").div(len(ordered_columns))
    return res.round(1)


def calc_change_rate(
    df: pd.DataFrame, ordered_columns: list[str], n: int = 1
) -> pd.DataFrame:
    """Beregner prosentvis endring mellom kolonner over n perioder.

    Args:
        df (pd.DataFrame): DataFrame med data.
        ordered_columns (list[str]): Liste med kolonnenavn i rekkefølge.
        n (int, optional): Antall perioder tilbake for endringsberegning. Defaults to 1.

    Returns:
        pd.DataFrame: Prosentvis endring per rad for hver kolonne (fra n. kolonne og fremover).
    """
    return _percent_change_columns(df, ordered_columns, step=n)


def rolling_change_rate(df: pd.DataFrame, step: int = 1) -> pd.DataFrame:
    """Beregner rullende prosentvis endring mellom kolonner med gitt steg.

    Args:
        df (pd.DataFrame): DataFrame med kolonner som representerer perioder.
        step (int, optional): Antall kolonner å hoppe over for å beregne endring. Defaults to 1.

    Returns:
        pd.DataFrame: Prosentvis endring per rad, med kolonner fra `step` og fremover.
    """
    return _percent_change_columns(df, list(df.columns), step=step)


def _percent_change_columns(
    df: pd.DataFrame, columns: list[str], step: int = 1
) -> pd.DataFrame:
    """Beregner rullende prosentvis endring mellom kolonner med gitt steg.

    Args:
        df (pd.DataFrame): DataFrame med kolonner som representerer perioder.
        columns (list[str]): Liste med kolonnenavn i rekkefølge.
        step (int, optional): Antall kolonner å hoppe over for å beregne endring. Defaults to 1.

    Returns:
        pd.DataFrame: Prosentvis endring per rad, med kolonner fra `step` og fremover.
    """
    results = []
    for i in range(step, len(columns), step):
        col_present = columns[i]
        previous_period = columns[i - step]
        chg_rate = (df[col_present] - df[previous_period]) * 100 / df[previous_period]
        results.append(chg_rate)
    return pd.concat(results, axis="columns", keys=columns[step:])
