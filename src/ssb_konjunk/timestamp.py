"""Functions to create timestamp according to SSB standard."""


def _check_even(elements: list[int]) -> bool:
    """Function to check if number is even."""
    return len(elements) % 2 == 0


def _check_valid_day(day: int) -> None:
    """Function to check that day arg is valid."""
    if day > 31:
        raise ValueError(
            f"The arg for day is bigger than possible max is 31 you have: {day}."
        )
    if day < 1:
        raise ValueError(
            f"The arg for day is smaller than possible min is 1 you have: {day}."
        )


def _check_valid_week(week: int) -> None:
    """Function to check that day arg is valid."""
    if week > 52:
        raise ValueError(
            f"The arg for week is bigger than possible max is 52 you have: {week}."
        )
    if week < 1:
        raise ValueError(
            f"The arg for week is smaller than possible min is 1 you have: {week}."
        )


def _check_valid_month(month: int) -> None:
    """Function to check that month arg is valid."""
    if month > 12:
        raise ValueError(
            f"The arg for month is bigger than possible max is 12 you have: {month}."
        )
    if month < 1:
        raise ValueError(
            f"The arg for month is smaller than possible min is 1 you have: {month}."
        )


def _check_valid_term(term: int) -> None:
    """Function to check that day arg is valid."""
    if term > 6:
        raise ValueError(
            f"The arg for term is bigger than possible max is 6 you have: {term}."
        )
    if term < 1:
        raise ValueError(
            f"The arg for term is smaller than possible min is 1 you have: {term}."
        )


def _check_valid_quarter(quarter: int) -> None:
    """Function to check that day arg is valid."""
    if quarter > 4:
        raise ValueError(
            f"The arg for quarter is bigger than possible max is 4 you have: {quarter}."
        )
    if quarter < 1:
        raise ValueError(
            f"The arg for quarter is smaller than possible min is 1 you have: {quarter}."
        )


def _check_valid_trimester(trimester: int) -> None:
    """Function to check that day arg is valid."""
    if trimester > 3:
        raise ValueError(
            f"The arg for trimester is bigger than possible max is 3 you have: {trimester}."
        )
    if trimester < 1:
        raise ValueError(
            f"The arg for trimester is smaller than possible min is 1 you have: {trimester}."
        )


def _check_valid_half_year(half_year: int) -> None:
    """Function to check that day arg is valid."""
    if half_year > 2:
        raise ValueError(
            f"The arg for half_year is bigger than possible max is 2 you have: {half_year}."
        )
    if half_year < 1:
        raise ValueError(
            f"The arg for half_year is smaller than possible min is 1 you have: {half_year}."
        )


def _check_valid_year(year1: int, year2: int | None = None) -> None:
    """Function to check that year is valid."""
    if len(str(year1)) != 4:
        raise ValueError(
            f"Any valid year should be length 4, you have: {len(str(year1))}, maybe you should check if year: {year1} is correct."
        )
    if year2:
        if len(str(year2)) != 4:
            raise ValueError(
                f"Any valid year should be length 4, you have: {len(str(year2))}, maybe you should check if year: {year2} is correct."
            )

        if year1 > year2:
            raise ValueError(
                f"The order of args is start date and then end date. Therefore first year arg can not be bigger than the last. Your args are start year:{year1}  end year:{year2}."
            )


def _check_valid_args(*args: int, frequency: str) -> None:
    """Function to check if valid args."""
    if len(args) == 2:
        _check_valid_year(args[0])
        if frequency == "W":
            _check_valid_week(args[1])
        if frequency == "M":
            _check_valid_month(args[1])
        if frequency == "B":
            _check_valid_term(args[1])
        if frequency == "Q":
            _check_valid_quarter(args[1])
        if frequency == "T":
            _check_valid_trimester(args[1])
        if frequency == "H":
            _check_valid_half_year(args[1])
    elif len(args) == 4:
        _check_valid_year(args[0], args[2])
        if frequency == "W":
            _check_valid_week(args[1])
            _check_valid_week(args[3])
        if frequency == "M":
            _check_valid_month(args[1])
            _check_valid_month(args[3])
        if frequency == "B":
            _check_valid_term(args[1])
            _check_valid_term(args[3])
        if frequency == "Q":
            _check_valid_quarter(args[1])
            _check_valid_quarter(args[3])
        if frequency == "T":
            _check_valid_trimester(args[1])
            _check_valid_trimester(args[3])
        if frequency == "H":
            _check_valid_half_year(args[1])
            _check_valid_half_year(args[3])
    else:
        raise ValueError(
            f"You have the wrong number of args, youre args are {args}. You can have 2 or 4 for frequency: {frequency}"
        )


def _check_frequency_suport(frequency: str) -> None:
    """Function to check if frequency requested is supported."""
    if frequency not in ["Y", "Q", "B", "M", "D", "W", "T", "H"]:
        raise ValueError(
            f"The function does not support frequency: {frequency} yet. Please use one the supported ones: Y,Q,B,M,D,W,T,H"
        )


def _get_timestamp_daily(*args: int) -> str | None:
    """Function to create timestamp if frequency is daily."""
    if len(args) == 3:
        _check_valid_year(args[0])
        _check_valid_month(args[1])
        _check_valid_day(args[2])
        return f"p{args[0]}-{args[1]:02}-{args[2]:02}"
    elif len(args) == 6:
        _check_valid_year(args[0], args[3])
        _check_valid_month(args[1])
        _check_valid_day(args[2])
        _check_valid_month(args[4])
        _check_valid_day(args[5])
        return (
            f"p{args[0]}-{args[1]:02}-{args[2]:02}_p{args[3]}-{args[4]:02}-{args[5]:02}"
        )
    else:
        raise ValueError(f"Ikke gyldig mende args, du har antall:{len(args)}")


def _get_timestamp_yearly(*args: int) -> str | None:
    """Function to create timstamp if frequency is yearly."""
    if len(args) == 2:
        _check_valid_year(args[0], args[1])
        return f"p{args[0]}_p{args[1]}"
    else:
        raise ValueError(
            f"You have the wrong amount of args, you can have two you have args: {args}"
        )


def _get_timestamp_special(*args: int, frequency: str) -> str | None:
    """Function to create timestamp if frequency is not Y or D."""
    _check_valid_args(*args, frequency=frequency)

    if frequency in ["M", "W"]:
        if frequency == "M":
            frequency = "-"
        if frequency == "W":
            frequency = "-W"
        if len(args) == 2:
            return f"p{args[0]}{frequency}{args[1]:02}"
        elif len(args) == 4:
            return (
                f"p{args[0]}{frequency}{args[1]:02}_p{args[2]}{frequency}{args[3]:02}"
            )
        else:
            return None

    else:
        if frequency in ["Q", "B", "T", "H"]:
            frequency = f"-{frequency}"
        if len(args) == 2:
            return f"p{args[0]}{frequency}{args[1]}"
        elif len(args) == 4:
            return f"p{args[0]}{frequency}{args[1]}_p{args[2]}{frequency}{args[3]}"
        else:
            return None


def get_ssb_timestamp(*args: int, frequency: str = "M") -> str | None:
    r"""Function to create a string in ssb timestamp format.

    Args:
        args: Up to six arguments with int, to create timestamp for.
        frequency: Letter for which frequency the data is, Y for year etc.

    Returns:
        string|None: Returns time stamp in ssb format.

    Raises:
        ValueError: Raises error for wrong values in args.

    Example:
        >>> get_ssb_timestamp(2024,8,1, frequency='D')
        'p2024-08-01'
    """
    _check_frequency_suport(frequency)

    if len(args) > 6:
        raise ValueError(
            f"Du kan ikke ha flere enn seks argumenter for å lage en ssb timestamp. Du har: {len(args)}"
        )
    elif all(arg is None for arg in args):
        return None
    elif not args[0]:
        raise ValueError(
            "Mangler start år, timestamp blir da None. Vurder å fylle inn start år."
        )
    elif len(args) == 1 and frequency == "Y":
        _check_valid_year(args[0])
        return f"p{args[0]}"
    else:

        valid_args = [arg for arg in args if arg]

        if frequency == "D":
            return _get_timestamp_daily(*valid_args)

        if not _check_even(valid_args) or len(valid_args) > 4:
            print(
                f"For frekvens '{frequency}', må du ha enten to eller fire argumenter. Du har:",
                len(valid_args),
            )
            return None
        else:
            if frequency == "Y":
                return _get_timestamp_yearly(*valid_args)
            else:
                return _get_timestamp_special(*valid_args, frequency=frequency)


def check_periodic_year(year: int, cycle_year: int, period: int) -> bool:
    """Check if a year is a part of a periodic cycle.

    An example of use: a functionality should be performed every third
    year, starting in year 2021. I.e. not in 2022 and 2023, but in
    2024. Then this function should return True when passing
    2024 as the year argument, 2021 (or 2015, 2018, 2024 and so) is
    passed as the cycle year and period is passed as 3 (triennal period).

    Args:
        year: the year to check.
        cycle_year: a year in the cycle.
        period: the number of years in a period.

    Returns:
        bool: whether or not the year is part of the triennal cycle.
    """
    if abs(year - cycle_year) % period == 0:
        return True
    else:
        return False
