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
        instans = Næringsanalyse(dataframe)
        tabell = instans.get_table_1_v2(period="2024M12")
        vis(tabell.res_data)

    """

    def __init__(
        self,
        data: pd.DataFrame,
        weight_data: pd.DataFrame,
        period_column: str = "periode",
        group_column: str = "nar",
        value_columns: list[str] | None = None,
        categorical_columns: list[str] | None = None,
    ) -> None:

        self.data = data
        self.period_col = period_column
        self.group_col = group_column
        
        self.weight_source = DataSource(
            pl.from_pandas(weight_data).with_columns(
                pl.col(self.period_col).str.strptime(
                    pl.Date,
                    "%Y-%m",
                    strict=False,
                )
            ),
            self.period_col,
            "vekt",
            self.group_col,
            internal_col="weight",
        )

        self.value_columns = value_columns
        self.categorical_columns = categorical_columns

        self.id_columns = [
            self.period_col,
            self.group_col,
            *self.categorical_columns,
        ]
        
        self.available_values = {
            col: sorted(
                data[col].dropna().unique().tolist()
            )
            for col in self.categorical_columns
        }

        
        self.class_codes = self._load_classification()
        self.root_code = None
        self._sort_order = self._build_sort_order()
        
        self.periods = AllPeriods(
            data[self.period_col].unique().tolist()
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
        
    def _load_classification(self) -> pd.DataFrame:
        nus = KlassClassification(
            classification_id="6",
            language="nb",
            include_future=False,
        )
    
        return nus.get_codes(
            from_date="2023-01-01"
        ).data[
            ["code", "parentCode", "name"]
        ]

    def calc_indirect(self, df: pd.DataFrame, col: str) -> float:
        """Beregner summen av en kolonne på høyeste hierarkinivå.

        Finner den høyeste nivåverdien i kolonnen 'nar' og summerer verdiene i
        spesifisert kolonne for disse radene.

        Args:
            df (pd.DataFrame): DataFrame som inneholder kolonnen 'nar' og den spesifiserte kolonnen.
            col (str): Navnet på kolonnen som skal summeres.

        Returns:
            float: Summen av verdiene for høyeste hierarkinivå.
        """
        max_splitted = (
            df[self.group_col].str.split(" - ").apply(lambda x: len(x[0])).max()
        )
        return df[col][
            df[self.group_col].str.split(" - ").apply(lambda x: len(x[0]))
            == max_splitted
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


    def get_group_col(self) -> list[str]:
        """Returnerer en liste over unike næringskoder fra datasettet.

        Henter alle unike verdier fra kolonnen 'nar' i selve datasettet.

        Returns:
            list[str]: Liste med unike næringskoder.
        """
        return self.data[self.group_col].unique().tolist()  # pyright: ignore

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
            self.class_codes[["code", "name"]],
            left_on=on,
            right_on="code",
            how="inner",
        )
        data_copy["complete"] = data_copy[on] + " - " + data_copy["name"]
        data_copy = data_copy.drop("name", axis="columns")
        data = data_copy.merge(data, right_on=on, left_on="code")
        data = data.drop([on + "_x", on + "_y", "code"], axis="columns")
        data = data.rename(columns={"complete": on})
        return data

    def _build_sort_order(self) -> dict[str, int]:
        """Bygger en hierarkisk sorteringsrekkefølge fra klassifikasjonskodene.

        Hierarkiet er definert av "parentCode" og starter ved "self.root_code" som kan vere definert i LocalDataManager, hvis ikke er den None.
        Du kan og legge til egene custom coder i en LocalDataManager.

        Returns:
            dict[str, int]: En dict som har et tall til hver code som brukes til sortering,
        """
        class_codes = self.class_codes
        children: dict[str | None, list[str]] = {}

        for row in class_codes.itertuples(index=False):
            parent = row.parentCode
            code = row.code
            if pd.isna(parent):
                parent = None
            if not isinstance(parent, (str, type(None))):
                raise TypeError(f"Ugyldig parentCode: {parent!r}")
            if not isinstance(code, str):
                raise TypeError(f"Ugyldig code: {code!r}")
            if parent not in children:
                children[parent] = []
            children[parent].append(code)
        for child_codes in children.values():
            child_codes.sort()

        sort_order = {}

        def add_children(parent_code: str | None, position: int) -> int:
            for code in children.get(parent_code, []):
                sort_order[code] = position
                position += 1
                position = add_children(code, position)

            return position

        if self.root_code is None:
            add_children(None, 0)
        else:
            sort_order[self.root_code] = 0
            add_children(self.root_code, 1)
        return sort_order

    def sort_aggregates(self, codes: pd.Series) -> pd.Series:
        """Sorterer aggregeringskoder i henhold til en forhåndsdefinert sorteringsrekkefølge.

        Args:
        codes: En pandas Series med aggregeringskoder som skal sorteres.

        Returns:
        En pandas Series der hver aggregeringskode er mappet til sin
        tilsvarende sorteringsverdi.
        """
        return codes.map(self._sort_order)

    def _normalize_weight(
        self,
        table_data: pl.DataFrame,
        includes_parent_aggregate: bool,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Normaliserer vektene til et datasett.

        Sørger for at vektene som blir brukt summerer opp til 100, og at vektet endring stemmer.

        Args:
            table_data (pl.DataFrame): Et datasett med vekt data og prosentendringsdata.
            includes_parent_aggregate (bool): En variabel som sier om du har hovudaggregatet og underaggregat i normaliseringen.

        Returns:
            table_data (pl.DataFrame): Et datasett med normalisert vekt data og ny prosentendringsdata.
            weighted_pct (pl.DataFrame): Samme datasett, men bare med vektet prosentendringsdata.
        """
        if includes_parent_aggregate:
            table_data = table_data.with_columns(
                (pl.col("weight") / pl.col("weight").max() * 100)
                .round(2)
                .alias("weight")
            )
        else:
            table_data = table_data.with_columns(
                ((pl.col("weight") / table_data["weight"].sum() * 100).round(2)).alias(
                    "weight"
                )
            )
        col = next(
            (c for c in ["value", "season1", "calendar1"] if c in table_data.columns), None
        )
        if col is None:
            raise ValueError("Wrong column name in table_data")

        table_data = table_data.with_columns(
            (0.01 * pl.col(col) * pl.col("weight")).round(2).alias("weighted")
        )

        weighted_pct = table_data.select([self.group_col, "weighted"])
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
        
    def _build_return_data(
        self,
        table_data: pl.DataFrame,
        figure_data: tuple[pl.DataFrame, str] | None,
        sparkline_data: list[pl.DataFrame] | None,
        header_1: list[str],
        header_2: list[str],
    ) -> ReturnData:
    
        result_df = (
            self._prep_df(table_data, self.group_col)
            .round(1)
            .sort_values(
                by=self.group_col,
                key=self.sort_aggregates,
            )
            .reset_index(drop=True)
        )
        
        figure_data_df = None
        
        if figure_data:
            figure_data_df = (
                self._prep_df(figure_data[0], self.group_col)
                .set_index(self.group_col)[figure_data[1]]
                .iloc[::-1]
            )
    
        sparkline_df = None
    
        if sparkline_data:
            sparkline_df = (
                multi_join(sparkline_data, on=self.group_col)
                .to_pandas()
                .sort_values(
                    by=self.group_col,
                    key=self.sort_aggregates,
                )
            )
    
        return ReturnData(
            header_1=header_1,
            header_2=header_2,
            res_data=result_df,
            figure_data=figure_data_df,
            sparkline_data=sparkline_df,
            indirect=None,
            groupby_col=self.group_col,
        )

    def _filter_max_nace_level(
        self,
        data: pl.DataFrame,
        max_nace_level: int,
    ) -> pl.DataFrame:
        numeric_mask = (
            pl.col(self.group_col)
            .str.replace(".", "", literal=True)
            .str.contains(r"^\d+$")
        )
    
        return data.filter(
            (
                numeric_mask
                & (
                    pl.col(self.group_col)
                    .str.replace(".", "", literal=True)
                    .str.len_chars()
                    <= max_nace_level
                )
            )
            | (~numeric_mask)
        )

    def _apply_group_filter(
        self,
        table_data: pl.DataFrame,
        weighted_data: pl.DataFrame,
        group_filter: list[str],
        includes_parent_aggregate: bool = True,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
    
        table_data = table_data.filter(
            pl.col(self.group_col).is_in(group_filter)
        )
    
        table_data, weighted_data = self._normalize_weight(
            table_data,
            includes_parent_aggregate=includes_parent_aggregate,
        )
    
        return table_data, weighted_data
    
    def get_source(
        self,
        value_column: str,
        filters: dict[str, str] | None = None,
    ) -> DataSource:
    
        if value_column not in self.value_columns:
            raise ValueError(
                f"Unknown value column: {value_column}"
            )
    
        subset = self.data.copy()
    
        if filters:
            for column, value in filters.items():
    
                if column not in self.categorical_columns:
                    raise ValueError(
                        f"Unknown categorical column: {column}"
                    )
    
                subset = subset[subset[column] == value]
    
        polars_data = pl.from_pandas(subset)
    
        polars_data = polars_data.with_columns(
            pl.col(value_column).cast(pl.Float64),
            pl.col(self.period_col).str.strptime(
                pl.Date,
                "%Y-%m",
                strict=False,
            ),
        )

        return DataSource(
            polars_data,
            self.period_col,
            value_column,
            self.group_col,
            internal_col="value",
        )

    def get_mth_change(
        self,
        source: DataSource,
        period: str | None = None,
        max_nace_level: int | None = None,
        group_filter: list[str] | None = None,
        includes_parent_aggregate: bool = True,
    ) -> ReturnData:
        skip = self._prep_skip(period)
    
        header, weight = self.weight_source.n_month(1, skip=skip)
        header_i, index = source.n_month(1, skip=skip)
        header_i_pct, index_pct = source.n_month_percent(
            1,
            skip=skip,
        )
        
        weighted_pct = (
            index_pct.join(weight, on=self.group_col)
            .with_columns(
                weighted_value=(
                    pl.col("value") * (pl.col("weight") / 100)
                ).round(2)
            )
            .drop("weight", "value")
        )
        
        sparkline_data = [source.n_month(1, i)[1] for i in range(6)]
        sparkline_data.reverse()
    
        table_data = multi_join(
            [weight, index, index_pct, weighted_pct],
            on=self.group_col,
        )
    
        if max_nace_level:
            table_data = self._filter_max_nace_level(
                table_data,
                max_nace_level,
            )
            weighted_pct = self._filter_max_nace_level(
                weighted_pct,
                max_nace_level,
            )
    
        if group_filter:
            table_data, weighted_pct = self._apply_group_filter(
                table_data,
                weighted_pct,
                group_filter,
                includes_parent_aggregate,
            )
    
        return self._build_return_data(
            table_data=table_data,
            figure_data=(weighted_pct, 'weighted_value'),
            sparkline_data=sparkline_data,
            header_1 = [
                "",
                "Vekt %",
                "Indeks",
                "% Endring",
                "% Endring vektet"
            ],
            header_2=[
                "",
                header,
                header_i,
                header_i_pct,
                header_i_pct,
            ],
        )
    def get_3_mth_change(
        self,
        source: DataSource,
        period: str | None = None,
        max_nace_level: int | None = None,
        group_filter: list[str] | None = None,
        includes_parent_aggregate: bool = True,
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
            includes_parent_aggregate (bool): En variabel som sier om du har hovudaggregatet og underaggregat i normaliseringen.

        Returns:
            ReturnData: Et objekt som inneholder overskrifter, formaterte resultatdata,
            figursøyledata og sparkline-serier for de valgte periodene.
        """
        skip = self._prep_skip(period)
        header, weight = self.weight_source.n_mean_rolling(3, skip=skip)

        #This is used to get the weigth right when the weight the previous 3 months and the 3 months before that is not the same 
        _, comparison_weight = self.weight_source.n_mean_rolling(3, skip=3)
        
        header_i, index = source.n_mean_rolling(3, skip=skip)

        header_i_pct, index_pct = (
            source.n_month_rolling_percent_compare(3, skip, skip + 3)
        )
        index_pct = index_pct.with_columns(
            value=pl.col("value").round(1)
        )
        weighted_pct = (
            index_pct.join(comparison_weight, on=self.group_col)
            .with_columns(
                weighted_value=(pl.col("value") * (pl.col("weight") / 100)).round(2)
            )
            .drop("weight", "value")
        )
        sparkline_data = [
            source.n_mean_rolling(3, i * 3)[1] for i in range(4)
        ]
        sparkline_data.reverse()

        table_data = multi_join(
            [weight, index, index_pct, weighted_pct], on=self.group_col
        )
        
        if max_nace_level:
            table_data = self._filter_max_nace_level(
                table_data,
                max_nace_level,
            )
            weighted_pct = self._filter_max_nace_level(
                weighted_pct,
                max_nace_level,
            )
        if group_filter:
            table_data, weighted_pct = self._apply_group_filter(
                table_data,
                weighted_pct,
                group_filter,
                includes_parent_aggregate,
            )
        return self._build_return_data(
            table_data=table_data,
            figure_data=(weighted_pct, 'weighted_value'),
            sparkline_data=sparkline_data,
            header_1 = [
                "",
                "Vekt %",
                "Indeks",
                "% Endring",
                "% Endring vektet"
            ],
            header_2=[
                "",
                header,
                header_i,
                header_i_pct,
                header_i_pct,
            ],
        )
        
    def get_12_mth_change(
        self,
        source: DataSource,
        period: str | None = None,
        max_nace_level: int | None = None,
        group_filter: list[str] | None = None,
        includes_parent_aggregate: bool = True,
    ) -> ReturnData:
        """Beregner 12-måneders kalenderjusterte endringer og tilhørende vektede bidrag.

        Utfører aggregering og prosentvis sammenligning over et 12-måneders vindu
        basert på kalenderjustert data. Dataen sammenstilles til rapporteringsvennlige
        datasett som inkluderer både råverdier, endringer og vektet påvirkning.

        Args:
            period (str or None): Valgfri referanseperiode, som '2023M03'. Hvis None, brukes siste tilgjengelige periode.
            max_nace_level (int or None): Maksimalt antall tegn i NACE-koder som brukes til å filtrere detaljeringsnivå i resultatene.
            nace_filter: (list[str] or None): Filter for hvilke nacer du ønsker å ha med i tabellen.
            includes_parent_aggregate (bool): En variabel som sier om du har hovudaggregatet og underaggregat i normaliseringen.

        Returns:
            ReturnData: Objekt med tabelloverskrifter, datasett for visning og analyse,
            og vektede figurtall. Sparkline-data er ikke inkludert i denne varianten.
        """
        skip = self._prep_skip(period)
        header, weight = self.weight_source.n_month(1, skip=skip)
        _, comparison_weight = self.weight_source.n_mean_rolling(1, skip=12)
        header_i, index = source.n_month(1, skip=skip)
        header_i_pct, index_pct = source.n_percent_rolling(
            13, skip=skip
        )
        index_pct = index_pct.with_columns(
            value=pl.col("value").round(1)
        )
        weighted_pct = (
            index_pct.join(comparison_weight, on=self.group_col)
            .with_columns(
                weighted_value=(pl.col("value") * (pl.col("weight") / 100)).round(2)
            )
            .drop("weight", "value")
        )
        table_data = multi_join(
            [weight, index, index_pct, weighted_pct], on=self.group_col
        )
        if max_nace_level:
            table_data = self._filter_max_nace_level(
                table_data,
                max_nace_level,
            )
            weighted_pct = self._filter_max_nace_level(
                weighted_pct,
                max_nace_level,
            )
        if group_filter:
            table_data, weighted_pct = self._apply_group_filter(
                table_data,
                weighted_pct,
                group_filter,
                includes_parent_aggregate,
            )
        return self._build_return_data(
            table_data=table_data,
            figure_data=(weighted_pct, 'weighted_value'),
            sparkline_data=None,
            header_1 = [
                "",
                "Vekt %",
                "Indeks",
                "% Endring",
                "% Endring vektet"
            ],
            header_2=[
                "",
                header,
                header_i,
                header_i_pct,
                header_i_pct,
            ],
        )
        
    def get_table_1(self, source: DataSource, period: str | None = None) -> ReturnData:
        """Genererer samleskjema med månedlige nivåer, endringer og vektet bidrag.

        Samler sesongjusterte nivådata for de tre siste månedene og prosentvise endringer
        for de siste fem. Deretter beregnes vektet bidrag for nåværende periode og alt
        settes sammen til et tabellvennlig datasett med riktige overskrifter.

        Args:
            period (str or None): Valgfri referanseperiode (f.eks. '2023M03'). Hvis None, brukes siste tilgjengelige periode.

        Returns:
            ReturnData: Et objekt som inneholder overskrifter, samlet tabell for visning,
            figurtall for vektet bidrag, og tom sparkline-data.
        """
        skip = self._prep_skip(period)
        df_data = []
        headers = []

        for i in range(2, -1, -1):
            header, data = source.n_month(1, i + skip)
            df_data.append(data)
            headers.append(header)

        for i in range(4, -1, -1):
            header, data = source.n_month_percent(1, i + skip)
            df_data.append(data)
            headers.append(header)

        header, weight = self.weight_source.n_month(1, skip=skip)
        _, index_pct = source.n_month_percent(1, skip=skip)
        index_pct = index_pct.with_columns(
            value=pl.col("value").round(1)
        )
        weighted_pct = (
            index_pct.join(weight, on=self.group_col)
            .with_columns(
                weighted_value=(pl.col("value") * (pl.col("weight") / 100)).round(2)
            )
            .drop("weight", "value")
        )

        return self._build_return_data(
                    table_data=multi_join(df_data, on=self.group_col),
                    figure_data=(weighted_pct, 'weighted_value'),
                    sparkline_data=None,
                    header_1=[
                        "",
                        "",
                        "",
                        "",
                        "% Endring",
                        "% Endring",
                        "% Endring",
                        "% Endring",
                        "% Endring",
                    ],
                    header_2=["", *headers],
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
