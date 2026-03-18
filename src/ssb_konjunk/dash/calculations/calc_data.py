from collections import defaultdict
from functools import cache

import pandas as pd
import polars as pl
from klass import KlassClassification

from ssb_konjunk.dash.calculations.helper_functions import DataSource
from ssb_konjunk.dash.calculations.helper_functions import monthdelta
from ssb_konjunk.dash.calculations.helper_functions import multi_join
from ssb_konjunk.dash.calculations.helper_functions import parse_period
from ssb_konjunk.dash.calculations.period_utils import AllPeriods
from ssb_konjunk.dash.calculations.period_utils import Period
from ssb_konjunk.dash.components.page_aio import ReturnData


class DataManager:
    """Klasse for analyse og visning av næringsdata med ulike justeringer og aggregeringer.

    Denne klassen håndterer import, filtrering, strukturering og presentasjon av økonomiske
    tidsserier basert på klassifikasjonssystemer (NACE eller lignende). Den kombinerer sesongjusterte,
    kalenderjusterte og ujusterte tall sammen med vekter for å muliggjøre avansert analyse og
    rapportgenerering.

    Hovedfunksjonalitet:
    ---------------------
    - Leser og forbereder data for videre analyse.
    - Henter og kobler til klassifikasjonskoder for næringsaggregering.
    - Bygger egne datastrukturer for sesongjustert, kalenderjustert, rå og vektet serie.
    - Genererer ulike tabell- og figurformater (returnert som `ReturnData`) for analyse og presentasjon.
    - Støtter aggregering på ulike nivåer samt filtrering på detaljnivå.

    Hovedkomponenter:
    ------------------
    - `self.raw_source`, `self.calendar_source`, `self.season_source`, `self.weight_source`:
    Datastrukturer for ulike måter å representere eller justere tidsserier.
    - `get_table_X_v2(...)`: Metoder for å hente ulike tabellvarianter (1-7), hver tilpasset spesifikke analysetyper.
    - Statiske hjelpefunksjoner for sortering, prosentberegning og visuell formatering.
    - Intern periodehåndtering for fleksibel datoanalyse.

    Forventet bruk:
    ----------------
    Kan brukes i interaktive dashboards, automatiserte rapporter eller API-endepunkt
    der strukturert økonomisk statistikk kreves.

    Eksempel:
    ---------
        instans = Næringsanalyse(dat<e)
        tabell = instans.get_table_1_v2(period="2024M12")
        vis(tabell.res_data)

    """

    def __init__(self, data: pd.DataFrame) -> None:
        """Initialiserer klassen med behandlet og strukturert tidsseriedata.

        Filtrerer bort spesifikke 'nar'-verdier, sorterer data, henter klassifikasjonskoder,
        og oppretter datastrukturer for sesongjustert, kalenderjustert, rådata og vekter.
        Konverterer deretter data til Polars og oppretter tilknyttede datakilder.

        Args:
            data (pd.DataFrame): Inndata som inneholder tidsseriedata, med blant annet
                kolonnene 'nar', 'periode', 'jus', 'korr', 'ujust' og 'verdi'.
        """
        nus = KlassClassification(
            classification_id="6", language="nb", include_future=False
        )
        self.class_codes = nus.get_codes(from_date="2023-01-01").data[["name", "code"]]
        data = data[~data["nar"].isin(["CC1.I.IVL.U.M", "CC2.I.IVL.U.M"])]

        data = data.sort_values("nar", key=self.sort_aggregates)
        data = data.reset_index(drop=True)

        self.periods = AllPeriods(data["periode"].unique().tolist())  # pyright: ignore

        self.data = data

        cols = ["jus", "korr", "ujust"]
        data[cols] = data[cols].apply(pd.to_numeric, errors="coerce", axis=1)

        self.season_adjusted_series = data[["nar", "periode", "jus"]].pivot(
            index="nar", columns="periode", values="jus"
        )

        self.calender_adjusted_series = data[["nar", "periode", "korr"]].pivot(
            index="nar", columns="periode", values="korr"
        )
        self.raw_series = data[["nar", "periode", "ujust"]].pivot(
            index="nar", columns="periode", values="ujust"
        )

        self.weight_series = data[["nar", "periode", "verdi"]].pivot(
            index="nar", columns="periode", values="verdi"
        )
        polars_data = pl.from_dataframe(data)

        polars_data = polars_data.with_columns(
            ujust=pl.col("ujust").cast(pl.Float64),
            jus=pl.col("jus").cast(pl.Float64),
            korr=pl.col("korr").cast(pl.Float64),
            periode=pl.col("periode").str.strptime(pl.Date, "%Y-%m", strict=False),
        )
        self.weight_source = DataSource(
            polars_data, "periode", "verdi", "nar", internal_col="weight"
        )
        self.season_source = DataSource(
            polars_data, "periode", "jus", "nar", internal_col="season"
        )
        self.raw_source = DataSource(
            polars_data, "periode", "ujust", "nar", internal_col="raw"
        )
        self.calendar_source = DataSource(
            polars_data, "periode", "korr", "nar", internal_col="calendar"
        )

    @staticmethod
    def pad_single(x: str) -> str:
        """Legger inn innrykk basert på nivå i hierarkisk kode.

        Gitt en streng der nivåer er adskilt med bindestrek (f.eks. '42.1 - Bygging av veier og jernbane'),
        returneres strengen med innrykk tilsvarende hierarkinivået på den første veriden.

        Args:
            x (str): En streng som representerer en hierarkisk kode.

        Returns:
            str: Strengen med passende innrykk.
        """
        first_item_len = len(x.split("-")[0]) - 1
        return ("  " * first_item_len) + x

    @staticmethod
    def calc_indirect(df: pd.DataFrame, col: str) -> float:
        """Beregner summen av en kolonne på høyeste hierarkinivå.

        Finner den høyeste nivåverdien i kolonnen 'nar' og summerer verdiene i
        spesifisert kolonne for disse radene.

        Args:
            df (pd.DataFrame): DataFrame som inneholder kolonnen 'nar' og den spesifiserte kolonnen.
            col (str): Navnet på kolonnen som skal summeres.

        Returns:
            float: Summen av verdiene for høyeste hierarkinivå.
        """
        max_splitted = df["nar"].str.split(" - ").apply(lambda x: len(x[0])).max()
        return df[col][
            df["nar"].str.split(" - ").apply(lambda x: len(x[0])) == max_splitted
        ].sum()

    @staticmethod
    def to_percent(
        weights: pl.DataFrame | pl.Series, chg_rate: pl.Series
    ) -> pl.DataFrame | pl.Series:
        """Omregner endring og vekt til prosentvis bidrag.

        Multipliserer vekt med endring og dividerer på 100 for å gi prosentandeler.

        Args:
            weights (pd.DataFrame or pd.Series): Vekttall.
            chg_rate (pd.Series): Prosentvise endringer.

        Returns:
            pd.DataFrame or pd.Series: Prosentvis påvirkning.
        """
        return (weights * chg_rate) / 100

    @property
    def header_1(self) -> list[str]:
        """Lister standardkolonner for visning i tabelloversikt.

        Returns:
            list[str]: Overskrifter som inkluderer vekt, indeks og endringer.
        """
        return ["", "Vekt %", "Indeks", "% Endring", "% Endring vektet"]

    def get_nacer(self) -> list[str]:
        """Returnerer en liste over unike næringskoder fra datasettet.

        Henter alle unike verdier fra kolonnen 'nar' i selve datasettet.

        Returns:
            list[str]: Liste med unike næringskoder.
        """
        return self.data["nar"].unique().tolist()  # pyright: ignore

    def add_klass_codes(self, data: pd.DataFrame, on: str) -> pd.DataFrame:
        """Legger til klassifikasjonsnavn til et datasett basert på en spesifisert kolonne.

        Slår opp koder fra `self.class_codes` og legger til fullstendige navn i en ny kolonne,
        deretter slår den dette sammen med det opprinnelige datasettet og rydder opp
        i kolonnenavn.

        Args:
            data (pd.DataFrame): Datasett som skal berikes med klassifikasjonsnavn.
            on (str): Navnet på kolonnen i `data` som inneholder kodeverdier som skal matches.

        Returns:
            pd.DataFrame: Det utvidede datasettet med nye klassifikasjonsnavn.
        """
        data_copy = data[on].to_frame()
        data_copy = data_copy.merge(
            self.class_codes, left_on=on, right_on="code", how="inner"
        )
        data_copy["complete"] = data_copy[on] + " - " + data_copy["name"]
        data_copy = data_copy.drop("name", axis="columns")
        data = data_copy.merge(data, right_on=on, left_on="code")
        data = data.drop([on + "_x", on + "_y", "code"], axis="columns")
        data = data.rename(columns={"complete": on})
        return data

    @staticmethod
    def sort_aggregates(index: pd.Series) -> pd.Series:
        """Returnerer en sorteringsnøkkel basert på næringskoders hierarki.

        Grupperer og sorterer koder alfabetisk og numerisk, inkludert undernivåer (f.eks. '1', '1.1', '1.2')
        slik at dataserier får ønsket rekkefølge.

        Args:
            index (pd.Series): En serie med næringskoder som strenger.

        Returns:
            pd.Series: En serie med heltallsverdier som representerer sorteringsrekkefølge.
        """
        main_aggregate = []
        hierarchal_aggregates = defaultdict(list)

        for item in map(str, index):
            if item.isalpha():
                main_aggregate.append(item)
            else:
                key, *sub = item.split(".")
                if sub:
                    hierarchal_aggregates[key].append(sub[0])
                else:
                    hierarchal_aggregates[key]

        for key_int in sorted(map(int, hierarchal_aggregates.keys())):
            main_aggregate.append(f"{key_int}")
            main_aggregate.extend(
                f"{key_int}.{sub}"
                for sub in sorted(map(int, hierarchal_aggregates[str(key_int)]))
            )

        mapper = {item: idx for idx, item in enumerate(main_aggregate)}
        return index.map(mapper)

    @staticmethod
    def _normalize_weight(
        table_data: pl.DataFrame,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Normaliserer vektene til et datasett.

        Sørger for at vektene som blir brukt summerer opp til 100, og at vektet endring stemmer.

        Args:
            table_data (pl.DataFrame): Et datasett med vekt data og prosentendringsdata.

        Returns:
            table_data (pl.DataFrame): Et datasett med normalisert vekt data og ny prosentendringsdata.
            weighted_pct (pl.DataFrame): Samme datasett, men bare med vektet prosentendringsdata.
        """
        table_data = table_data.with_columns(
            ((pl.col("weight") / table_data["weight"].sum() * 100).round(2)).alias(
                "weight"
            )
        )
        col = next(
            (c for c in ["season1", "calendar1"] if c in table_data.columns), None
        )
        if col is None:
            raise ValueError("Wrong column name in table_data")

        table_data = table_data.with_columns(
            (0.01 * pl.col(col) * pl.col("weight")).round(2).alias("weighted")
        )

        weighted_pct = table_data.select(["nar", "weighted"])
        return table_data, weighted_pct

    def get_all_periods(self) -> list[str]:
        """Oppretter en liste over alle periodeobjekter som tekstform.

        Basert på interne periodeobjekter returneres en liste av alle 12 perioder
        i et år, typisk brukt for visualisering eller sammenligning.

        Returns:
            list[str]: Liste med periodeverdier som strenger.
        """
        return [item.as_period() for item in self.periods.create_period_range(12)]

    def format_aggregates(self, data: pd.Series) -> pd.Series:
        """Formaterer hierarkiske aggregeringskoder med innrykk.

        Bruker `pad_single` for å legge til visuelt innrykk basert på hierarkinivå
        for hver streng i serien.

        Args:
            data (pd.Series): Serie med nærings- eller klassifikasjonskoder.

        Returns:
            pd.Series: Serie med innrykkede koder.
        """
        return data.apply(self.pad_single)

    def _prep_skip(self, period: str | None) -> int:
        """Beregner antall måneder mellom gitt periode og siste tilgjengelige dato i vektdatasettet.

        Brukes for å definere "skip"-verdi, dvs. hvor langt bakover i tid man må hente data.

        Args:
            period (str or None): Periode i tekstformat, f.eks. '2023-03'. Kan også være None.

        Returns:
            int: Antall måneder mellom siste dato og ønsket periode.
        """
        skip = 0
        latest = self.weight_source.latest_date()
        if (period is not None) and (latest is not None) and (period != "None"):
            skip = monthdelta(latest, parse_period(period))
        return skip

    def _prep_df(self, df: pl.DataFrame, sort_by: str, format_len: int = 40):
        """Forbereder og formaterer datasett for visning eller eksport.

        Konverterer Polars-datasett til Pandas, sorterer etter valgt kolonne, legger til klassifikasjonsnavn,
        formaterer aggregeringskoder med innrykk og trunkerer tekst til angitt lengde.

        Args:
            df (pl.DataFrame): Inndata i Polars-format.
            sort_by (str): Navn på kolonnen som skal brukes til sortering og berikelse.
            format_len (int, optional): Maksimal lengde på formaterte koder. Standard er 40.

        Returns:
            pd.DataFrame: Formattet datasett klart for videre bruk.
        """
        formatted = df.to_pandas().sort_values(by=sort_by, key=self.sort_aggregates)
        formatted = self.add_klass_codes(formatted, on=sort_by)
        formatted[sort_by] = self.format_aggregates(formatted[sort_by])
        formatted[sort_by] = formatted[sort_by].str[:format_len]
        return formatted

    def create_periods_and_latest(
        self, period: str | None, periods: int
    ) -> tuple[list[Period], Period]:
        """Oppretter en liste med perioder og returnerer samtidig den nyeste perioden.

        Hvis en spesifikk periode oppgis, brukes den som referansepunkt. Ellers hentes
        den nyeste perioden automatisk fra tilgjengelige data.

        Args:
            period (str or None): Referanseperiode i format som '2023M03'. Hvis None, brukes siste tilgjengelige.
            periods (int): Antall perioder som skal opprettes.

        Returns:
            tuple[list[Period], Period]: En liste over perioder og den nyeste perioden som ble brukt som referanse.
        """
        if (period is not None) and period != "None":
            latest = Period(period)
            all_periods = self.periods.create_period_range(
                periods, year=latest.year, month=latest.month
            )
        else:

            latest = self.periods.get_latest()
            all_periods = self.periods.create_period_range(periods)

        return all_periods, latest

    def create_period_range(
        self, period: str | None, periods: int, step: int = 1
    ) -> list[Period]:
        """Genererer en liste av perioder bakover i tid fra en referanseperiode.

        Starter fra oppgitt periode (eller siste tilgjengelige hvis None) og trekker seg bakover
        i tid med et valgfritt steg i antall måneder.

        Args:
            period (str or None): Startperiode i format som '2023M03'. Hvis None, brukes siste tilgjengelige.
            periods (int): Antall perioder som skal genereres.
            step (int, optional): Antall måneder mellom hver periode. Standard er 1.

        Returns:
            list[Period]: Liste med perioder i synkende rekkefølge.
        """
        if (period is not None) and period != "None":
            latest = Period(period)
        else:
            latest = self.periods.get_latest()
        all_periods = [latest]
        while len(all_periods) < periods:
            latest = latest.subtract(months=step)
            all_periods.append(latest)
        return all_periods

    def get_sesonal_adjusted_3_mth_change(
        self,
        period: str | None = None,
        max_nace_level: int | None = None,
        nace_filter: list[str] | None = None,
    ) -> ReturnData:
        """Henter sesongjusterte 3-måneders endringer og beregner vektede bidrag.

        Utfører rullerende 3-måneders gjennomsnitt på sesongjusterte serier og vekter,
        samt beregner prosentvis endring og vektet påvirkning. Returnerer dataene som
        tabell, figurer og sparkline-format for rapportering eller visualisering.

        Args:
            period (str or None): Valgfri referanseperiode som '2023M03'. Hvis None,
                brukes siste tilgjengelige periode.
            max_nace_level (int or None): Maksimalt tillatt lengde på NACE-koder (antall tegn).
                Brukes til å filtrere detaljeringsnivå i resultatene.
            nace_filter: (list[str] or None): Filter for hvilke nacer du ønsker å ha med i tabellen.

        Returns:
            ReturnData: Et objekt som inneholder overskrifter, formaterte resultatdata,
            figursøyledata og sparkline-serier for de valgte periodene.
        """
        skip = self._prep_skip(period)
        header, weight = self.weight_source.n_mean_rolling(3, skip=skip)
        _, comparison_weight = self.weight_source.n_mean_rolling(3, skip=3)
        header_i, index_season = self.season_source.n_mean_rolling(3, skip=skip)

        header_i_pct, index_season_pct = (
            self.season_source.n_month_rolling_percent_compare(3, skip, skip + 3)
        )
        index_season_pct = index_season_pct.with_columns(
            season=pl.col("season").round(1)
        )
        weighted_pct = (
            index_season_pct.join(comparison_weight, on="nar")
            .with_columns(
                weighted=(pl.col("season") * (pl.col("weight") / 100)).round(2)
            )
            .drop("weight", "season")
        )
        sparkline_data = [
            self.season_source.n_mean_rolling(3, i * 3)[1] for i in range(4)
        ]
        sparkline_data.reverse()

        table_data = multi_join(
            [weight, index_season, index_season_pct, weighted_pct], on="nar"
        )
        if max_nace_level is not None:
            numeric_mask = (
                pl.col("nar").str.replace(".", "", literal=True).str.contains(r"^\d+$")
            )
            table_data = table_data.filter(
                (
                    numeric_mask
                    & (
                        pl.col("nar").str.replace(".", "").str.len_chars()
                        <= max_nace_level
                    )
                )
                | (~numeric_mask)
            )

            weighted_pct = weighted_pct.filter(
                (
                    numeric_mask
                    & (
                        pl.col("nar").str.replace(".", "").str.len_chars()
                        <= max_nace_level
                    )
                )
                | (~numeric_mask)
            )
        if nace_filter:
            table_data = table_data.filter(pl.col("nar").is_in(nace_filter))
            table_data, weighted_pct = self._normalize_weight(table_data)

        return ReturnData(
            header_1=self.header_1,
            header_2=["", header, header_i, header_i_pct, header_i_pct],
            res_data=(
                self._prep_df(table_data, "nar")
                .round(1)
                .sort_values(
                    by="nar",
                    key=lambda col: col.map(
                        lambda x: (str(x).lstrip()[0].isdigit(), str(x).lstrip())
                    ),
                    ignore_index=True,
                )
            ),
            figure_data=self._prep_df(weighted_pct, "nar")
            .set_index("nar")["weighted"]
            .iloc[::-1],
            sparkline_data=multi_join(sparkline_data, on="nar")
            .to_pandas()
            .sort_values(by="nar", key=self.sort_aggregates),
            indirect=None,
        )

    def get_sesonal_adjusted_mth_change(
        self,
        period: str | None = None,
        max_nace_level: int | None = None,
        nace_filter: list[str] | None = None,
    ) -> ReturnData:
        """Beregner månedlig sesongjustert endring og vektet bidrag for valgt periode.

        Utfører rullerende beregninger for én måned frem i tid på sesongjusterte data
        og vekter. Returnerer strukturerte data for tabellvisning, figurer og sparkline-grafer.

        Args:
            period (str or None): Valgfri referanseperiode, som '2023M03'. Hvis None, brukes siste tilgjengelige.
            max_nace_level (int or None): Maksimalt tillatt lengde på NACE-koder (antall tegn). Brukes for å filtrere detaljeringsnivå.
            nace_filter: (list[str] or None): Filter for hvilke nacer du ønsker å ha med i tabellen.

        Returns:
            ReturnData: Et objekt med kolonneoverskrifter, formaterte tabeller, figursøyledata
            og sparkline-serier for visualisering eller rapportering.
        """
        skip = self._prep_skip(period)
        header, weight = self.weight_source.n_month(1, skip=skip)
        _, comparison_weight = self.weight_source.n_mean_rolling(1, skip=1)
        header_i, index_season = self.season_source.n_month(1, skip=skip)
        header_i_pct, index_season_pct = self.season_source.n_month_percent(
            1, skip=skip
        )
        index_season_pct = index_season_pct.with_columns(
            season=pl.col("season").round(1)
        )
        weighted_pct = (
            index_season_pct.join(comparison_weight, on="nar")
            .with_columns(
                weighted=(pl.col("season") * (pl.col("weight") / 100)).round(2)
            )
            .drop("weight", "season")
        )
        sparkline_data = [self.season_source.n_month(1, i)[1] for i in range(6)]
        sparkline_data.reverse()
        table_data = multi_join(
            [weight, index_season, index_season_pct, weighted_pct], on="nar"
        )
        if max_nace_level is not None:
            numeric_mask = (
                pl.col("nar").str.replace(".", "", literal=True).str.contains(r"^\d+$")
            )
            table_data = table_data.filter(
                (
                    numeric_mask
                    & (
                        pl.col("nar").str.replace(".", "").str.len_chars()
                        <= max_nace_level
                    )
                )
                | (~numeric_mask)
            )

            weighted_pct = weighted_pct.filter(
                (
                    numeric_mask
                    & (
                        pl.col("nar").str.replace(".", "").str.len_chars()
                        <= max_nace_level
                    )
                )
                | (~numeric_mask)
            )
        if nace_filter:
            table_data = table_data.filter(pl.col("nar").is_in(nace_filter))
            table_data, weighted_pct = self._normalize_weight(table_data)
        return ReturnData(
            header_1=self.header_1,
            header_2=["", header, header_i, header_i_pct, header_i_pct],
            res_data=(
                self._prep_df(table_data, "nar")
                .round(1)
                .sort_values(
                    by="nar",
                    key=lambda col: col.map(
                        lambda x: (str(x).lstrip()[0].isdigit(), str(x).lstrip())
                    ),
                    ignore_index=True,
                )
            ),
            figure_data=self._prep_df(weighted_pct, "nar")
            .set_index("nar")["weighted"]
            .iloc[::-1],
            sparkline_data=multi_join(sparkline_data, on="nar")
            .to_pandas()
            .sort_values(by="nar", key=self.sort_aggregates),
            indirect=None,
        )

    def get_sesonal_adjusted_12_mth_change(
        self,
        period: str | None = None,
        max_nace_level: int | None = None,
        nace_filter: list[str] | None = None,
    ) -> ReturnData:
        """Beregner 12-måneders kalenderjusterte endringer og tilhørende vektede bidrag.

        Utfører aggregering og prosentvis sammenligning over et 12-måneders vindu
        basert på kalenderjustert data. Dataen sammenstilles til rapporteringsvennlige
        datasett som inkluderer både råverdier, endringer og vektet påvirkning.

        Args:
            period (str or None): Valgfri referanseperiode, som '2023M03'. Hvis None, brukes siste tilgjengelige periode.
            max_nace_level (int or None): Maksimalt antall tegn i NACE-koder som brukes til å filtrere detaljeringsnivå i resultatene.
            nace_filter: (list[str] or None): Filter for hvilke nacer du ønsker å ha med i tabellen.

        Returns:
            ReturnData: Objekt med tabelloverskrifter, datasett for visning og analyse,
            og vektede figurtall. Sparkline-data er ikke inkludert i denne varianten.
        """
        skip = self._prep_skip(period)
        header, weight = self.weight_source.n_month(1, skip=skip)
        _, comparison_weight = self.weight_source.n_mean_rolling(1, skip=12)
        header_i, index_season = self.calendar_source.n_month(1, skip=skip)
        header_i_pct, index_season_pct = self.calendar_source.n_percent_rolling(
            13, skip=skip
        )
        index_season_pct = index_season_pct.with_columns(
            calendar=pl.col("calendar").round(1)
        )
        weighted_pct = (
            index_season_pct.join(comparison_weight, on="nar")
            .with_columns(
                weighted=(pl.col("calendar") * (pl.col("weight") / 100)).round(2)
            )
            .drop("weight", "calendar")
        )
        table_data = multi_join(
            [weight, index_season, index_season_pct, weighted_pct], on="nar"
        )
        if max_nace_level is not None:
            numeric_mask = (
                pl.col("nar").str.replace(".", "", literal=True).str.contains(r"^\d+$")
            )
            table_data = table_data.filter(
                (
                    numeric_mask
                    & (
                        pl.col("nar").str.replace(".", "").str.len_chars()
                        <= max_nace_level
                    )
                )
                | (~numeric_mask)
            )

            weighted_pct = weighted_pct.filter(
                (
                    numeric_mask
                    & (
                        pl.col("nar").str.replace(".", "").str.len_chars()
                        <= max_nace_level
                    )
                )
                | (~numeric_mask)
            )
        if nace_filter:
            table_data = table_data.filter(pl.col("nar").is_in(nace_filter))
            table_data, weighted_pct = self._normalize_weight(table_data)
        return ReturnData(
            header_1=self.header_1,
            header_2=["", header, header_i, header_i_pct, header_i_pct],
            res_data=(
                self._prep_df(table_data, "nar")
                .round(1)
                .sort_values(
                    by="nar",
                    key=lambda col: col.map(
                        lambda x: (str(x).lstrip()[0].isdigit(), str(x).lstrip())
                    ),
                    ignore_index=True,
                )
            ),
            figure_data=self._prep_df(weighted_pct, "nar")
            .set_index("nar")["weighted"]
            .iloc[::-1],
            sparkline_data=None,
            indirect=None,
        )


@cache
def get_data_manager(path: str) -> DataManager:
    """Laster inn et Parquet-datasett og returnerer en DataManager-instans.

    Leser en Parquet-fil fra gitt filbane og oppretter en `DataManager` med de leste dataene.

    Args:
        path (str): Filbane til Parquet-filen som skal leses.

    Returns:
        DataManager: En instans som inneholder og håndterer de leste dataene.
    """
    data = pd.read_parquet(path)
    data_manager = DataManager(data)
    return data_manager
