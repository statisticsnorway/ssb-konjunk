from __future__ import annotations

from functools import total_ordering
from typing import Self

import pendulum


@total_ordering
class Period:
    """A class to help transform the period to the rigth formats."""

    def __init__(self, period: str) -> None:
        """A class to help transform the period to the rigth formats."""
        self._dt = self._period_to_datetime(period)

    @property
    def year(self) -> int:
        """Returnerer året til perioden som et heltall."""
        return self._dt.year

    @property
    def month(self) -> int:
        """Returnerer måneden til perioden som et heltall (1-12)."""
        return self._dt.month

    @classmethod
    def from_dt(cls, dt: pendulum.DateTime) -> Self:
        """Oppretter en instans basert på et `pendulum.DateTime`-objekt.

        Args:
            dt (pendulum.DateTime): Dato og tid som brukes til å lage perioden.

        Returns:
            Self: En ny instans av klassen med perioden satt til `dt`.
        """
        period = period_parser(dt.year, dt.month)
        return cls(period)

    def _period_to_datetime(self, item: str) -> pendulum.DateTime:
        """Konverterer en periode-streng (YYYY-MM) til et `pendulum.DateTime`-objekt.

        Args:
            item (str): Periodestreng med format 'YYYY-MM'.

        Returns:
            pendulum.DateTime: Dato med første dag i måneden.
        """
        year = int(item[0:4])
        month = int(item[-2:])
        return pendulum.datetime(year, month, 1)

    def as_period(self) -> str:
        """Returnerer perioden som en streng i formatet 'YYYY-MM'.

        Returns:
            str: Periodestreng, f.eks. '2026-03'.
        """
        month = self._dt.format("MM")
        year = self._dt.format("YYYY")
        return f"{year}-{month}"

    def as_string(self) -> str:
        """Returnerer perioden som en lesbar streng med månedsnavn og år.

        Returns:
            str: Streng, f.eks. 'Mar 2026'.
        """
        month = self._dt.format("MMM")
        year = self._dt.format("YYYY")
        return f"{month} {year}"

    def set_period(
        self: Self,
        year: int | None = None,
        month: int | None = None,
        day: int | None = None,
        hour: int | None = None,
        minute: int | None = None,
        second: int | None = None,
    ) -> None:
        """Oppdaterer verdier i perioden. Uspesifiserte verdier forblir uendret.

        Args:
            year (int | None): År.
            month (int | None): Måned (1-12).
            day (int | None): Dag (1-31).
            hour (int | None): Time (0-23).
            minute (int | None): Minutt (0-59).
            second (int | None): Sekund (0-59).
        """
        self._dt.set(
            year=year, month=month, day=day, hour=hour, minute=minute, second=second
        )

    def as_datetime(self) -> pendulum.DateTime:
        """Returnerer perioden som et `pendulum.DateTime`-objekt.

        Returns:
            pendulum.DateTime: Dato og tid som representerer perioden.
        """
        return self._dt

    def subtract(
        self: Self,
        years: int = 0,
        months: int = 0,
        weeks: int = 0,
        days: int = 0,
        hours: int = 0,
        minutes: int = 0,
        seconds: float = 0,
        microseconds: int = 0,
    ) -> Period:
        """Trekker fra angitte tidskomponenter fra denne perioden og returnerer en ny `Period`.

        Parametere:
            years (int): Antall år å trekke fra. Standard 0.
            months (int): Antall måneder å trekke fra. Standard 0.
            weeks (int): Antall uker å trekke fra. Standard 0.
            days (int): Antall dager å trekke fra. Standard 0.
            hours (int): Antall timer å trekke fra. Standard 0.
            minutes (int): Antall minutter å trekke fra. Standard 0.
            seconds (float): Antall sekunder å trekke fra (kan være flyttall). Standard 0.
            microseconds (int): Antall mikrosekunder å trekke fra. Standard 0.

        Returnerer:
            Period: En ny periode der de oppgitte komponentene er trukket fra.

        Merknader:
            - Alle parametere er valgfrie; 0 betyr ingen endring for komponenten.
            - Negativt tall vil i praksis legge til tilsvarende tid.
        """
        return Period.from_dt(
            self._dt.subtract(
                years, months, weeks, days, hours, minutes, seconds, microseconds
            )
        )

    def __hash__(self) -> int:
        """Returnerer has verdien til objectet."""
        return hash(str(self))

    def __eq__(self, other: object) -> bool:
        """Sjekker om dette objektet er likt et annet basert på perioden."""
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._dt == other._dt

    def __lt__(self, other: object) -> bool:
        """Sjekker om dette objektets periode er mindre enn et annet objekt sin periode."""
        if not isinstance(other, type(self)):
            return NotImplemented

        return self._dt < other._dt

    def _as_string(self) -> str:
        return self._dt.format("YYYY-MM")

    def __as_string_repr(self) -> str:
        return f"Period<{self.as_period()}, {self._as_string()}>"

    def __str__(self) -> str:
        """Returnerer en lesbar strengrepresentasjon av objektet."""
        return self.__as_string_repr()

    def __repr__(self) -> str:
        """Returnerer en lesbar strengrepresentasjon av objektet."""
        return self.__as_string_repr()


class AllPeriods:
    """Håndterer og beregner perioder."""

    def __init__(self, periods: list[str]) -> None:
        """Oppretter et `AllPeriods`-objekt fra en liste med periodestrenger.

        Args:
            periods (list[str]): Liste med periodestrenger i format 'YYYY-MM'.
        """
        all_periods = [Period(item) for item in periods]
        self.periods = sorted(all_periods)

    def get_latest(self) -> Period:
        """Returnerer den nyeste perioden.

        Returns:
            Period: Den siste perioden i listen.
        """
        return self.periods[-1]

    def get_period_by_index(self, index: int) -> Period:
        """Returnerer perioden på gitt indeks.

        Args:
            index (int): Indeksen i periodenlisten.

        Returns:
            Period: Perioden på posisjonen `index`.
        """
        return self.periods[index]

    def get_last_n_periods(self, periods: int) -> list[Period]:
        """Returnerer de siste N periodene.

        Args:
            periods (int): Antall perioder som skal hentes.

        Returns:
            list[Period]: Liste med de siste `periods` periodene.
        """
        periods = -1 * periods
        return self.periods[periods:]

    @staticmethod
    def create_period_range_static(n_months: int = 12) -> list[Period]:
        """Lager en liste med perioder bakover fra dagens dato.

        Args:
            n_months (int, optional): Antall måneder som skal inkluderes. Defaults to 12.

        Returns:
            list[Period]: Liste med `Period`-objekter fra n måneder tilbake til nå.
        """
        dt = pendulum.now()
        result = []
        for i in range(n_months - 1, -1, -1):
            selected_period = dt.subtract(months=i)
            period = Period.from_dt(selected_period)
            result.append(period)

        return result

    def create_period_range(
        self,
        n_months: int = 12,
        year: int | None = None,
        month: int | None = None,
    ) -> list[Period]:
        """Lager en liste med perioder bakover fra en gitt måned eller nyeste periode.

        Args:
            n_months (int, optional): Antall måneder som skal inkluderes. Defaults to 12.
            year (int | None, optional): Startår. Defaults to None.
            month (int | None, optional): Startmåned (1-12). Defaults to None.

        Returns:
            list[Period]: Liste med `Period`-objekter.
        """
        if year and month:
            dt = pendulum.local(year, month, 1)
        else:
            dt = self.get_latest().as_datetime()

        result = []
        for i in range(n_months - 1, -1, -1):
            selected_period = dt.subtract(months=i)
            period = Period.from_dt(selected_period)
            result.append(period)

        return result

    def __str__(self) -> str:
        """Returnerer en lesbar strengrepresentasjon av alle perioder i objektet."""
        return f"AllPeriods<{[str(item) for item in self.periods]}>"


def period_parser(year: int, month: int) -> str:
    """Lager en periodestreng i formatet 'YYYYMmm' fra år og måned.

    Args:
        year (int): Året.
        month (int): Måned (1-12).

    Returns:
        str: Periodestreng, f.eks. '2026M03'.
    """
    return f"{year}M{month:02d}"


def create_period_range_list(year: int, month: int, n_months: int) -> list[str]:
    """Oppretter en liste med periodestrenger bakover fra en gitt måned.

    Args:
        year (int): Startår.
        month (int): Startmåned (1-12).
        n_months (int): Antall måneder som skal inkluderes bakover.

    Returns:
        list[str]: Liste med periodestrenger i format 'YYYYMmm'.
    """
    result = []
    dt = pendulum.local(year, month, 1)
    for i in range(n_months, -1, -1):
        selected_period = dt.subtract(months=i)
        period = period_parser(selected_period.year, selected_period.month)
        result.append(period)
    return result
