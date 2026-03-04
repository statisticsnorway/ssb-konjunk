from functools import total_ordering

import pendulum


@total_ordering
class Period:
    def __init__(self, period: str):
        self.period = self._period_to_datetime(period)

    @property
    def year(self):
        return self.period.year

    @property
    def month(self):
        return self.period.month

    @classmethod
    def from_dt(cls, dt: pendulum.DateTime):
        period = period_parser(dt.year, dt.month)
        return cls(period)

    def _period_to_datetime(self, item: str) -> pendulum.DateTime:
        year = int(item[0:4])
        month = int(item[-2:])
        return pendulum.datetime(year, month, 1)

    def as_period(self) -> str:
        month = self.period.format("MM")
        year = self.period.format("YYYY")
        return f"{year}-{month}"

    def as_string(self) -> str:
        month = self.period.format("MMM")
        year = self.period.format("YYYY")
        return f"{month} {year}"

    def set_period(
        self,
        year: int | None = None,
        month: int | None = None,
        day: int | None = None,
        hour: int | None = None,
        minute: int | None = None,
        second: int | None = None,
    ):
        self.period.set(
            year=year, month=month, day=day, hour=hour, minute=minute, second=second
        )

    def as_datetime(self) -> pendulum.DateTime:
        return self.period

    def _is_valid_operand(self, other):
        return hasattr(other, "period")

    def subtract(
        self,
        years: int = 0,
        months: int = 0,
        weeks: int = 0,
        days: int = 0,
        hours: int = 0,
        minutes: int = 0,
        seconds: float = 0,
        microseconds: int = 0,
    ):
        return Period.from_dt(
            self.period.subtract(
                years, months, weeks, days, hours, minutes, seconds, microseconds
            )
        )

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        if not self._is_valid_operand(other):
            return NotImplemented

        return self.period == other.period

    def __lt__(self, other):
        if not self._is_valid_operand(other):
            return NotImplemented

        return self.period < other.period

    def _as_string(self) -> str:
        return self.period.format("YYYY-MM")

    def __as_string_repr(self) -> str:
        return f"Period<{self.as_period()}, {self._as_string()}>"

    def __str__(self):
        return self.__as_string_repr()

    def __repr__(self):
        return self.__as_string_repr()


class AllPeriods:
    def __init__(self, periods: list[str]):
        all_periods = [Period(item) for item in periods]
        self.periods = sorted(all_periods)

    def get_latest(self) -> Period:
        return self.periods[-1]

    def get_period_by_index(self, index) -> Period:
        return self.periods[index]

    def get_last_n_periods(self, periods: int) -> list[Period]:
        periods = -1 * periods
        return self.periods[periods:]

    @staticmethod
    def create_period_range_static(n_months: int = 12) -> list[Period]:

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

    def __str__(self):
        return f"AllPeriods<{[str(item) for item in self.periods]}>"


def _pad_month(month: int) -> str:
    """Funksjonen gjør om ett- og tosifrete ints til str med en null foran hvis input bare er ett siffer.

    Args:
        month: int, ett eller to siffer.

    Returns:
        str

    """
    return "%02d" % (month,)


def period_parser(year, month):
    month = _pad_month(month)
    return f"{year}M{month}"


def create_period_range_list(year: int, month: int, n_months: int) -> list[str]:
    result = []
    dt = pendulum.local(year, month, 1)
    for i in range(n_months, -1, -1):
        selected_period = dt.subtract(months=i)
        period = period_parser(selected_period.year, selected_period.month)
        result.append(period)
    return result
