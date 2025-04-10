"""Function for date prompts in python.

The functions should be used to prompt which period to read files from og which period to run a statistic for.
"""

import re
from calendar import monthrange
from datetime import datetime
from typing import Any

import pendulum


def _input_valid_int() -> int:
    """Input function for valid int.

    Returns:
        int: Return only valid int.
    """
    # Get the desired year from the user
    while True:
        user_input = input("Input: ")

        try:
            valid_int = int(user_input)
            break  # Break the loop if a valid integer is entered
        except ValueError:
            print("Vennligst skriv inn et gyldig tall som f.eks.", 42)

    return valid_int


def input_year() -> int:
    """Input function for year.

    Returns:
        int: Year as int
    """
    # Get the desired year from the user
    now = pendulum.now("Europe/Oslo").year
    print("Skriv inn år i format YYYY som", now)
    while True:
        year = _input_valid_int()
        if year == now:
            return year
        else:
            print("Er du sikker på at du skal kjøre statistikk for dette året?", year)
            if input("y/n") == "y":
                return year


def input_month() -> int:
    """Input function for month.

    Returns:
        int: month
    """
    # Get the desired month from the user
    print("Skriv inn måned i format m, som:", 8)
    while True:
        month = _input_valid_int()
        if 1 <= month <= 12:
            return month
        else:
            print("Ikke en gyldig måned, vennligst skriv inn et tall fra 1 til 12.")


def input_term() -> int:
    """Input function for term.

    Returns:
        int: term
    """
    print("Skriv inn termin i format b, som:", 3)
    while True:
        term = _input_valid_int()
        if 1 <= term <= 6:
            return term
        else:
            print("Ikke en gyldig termin, vennligst skriv inn et tall fra 1 til 6.")


def input_quarter() -> int:
    """Input function for quarter.

    Returns:
        int: quarter
    """
    print("Skriv inn kvartal i format q, som:", 2)
    while True:
        quarter = _input_valid_int()
        if 1 <= quarter <= 4:
            return quarter
        else:
            print("Ikke et gyldig kvartal, vennligst skriv inn et tall fra 1 til 4.")


def input_trimester() -> int:
    """Input function for trimester.

    Returns:
        int: trimester
    """
    print("Skriv inn trimester i format t, som:", 2)
    while True:
        trimester = _input_valid_int()
        if 1 <= trimester <= 3:
            return trimester
        else:
            print("Ikke et gyldig trimester, vennligst skriv inn et tall fra 1 til 3.")


def input_week() -> int:
    """Input function for week.

    Returns:
        int: week
    """
    print("Skriv inn week i format w, som:", 52)
    while True:
        week = _input_valid_int()
        if 1 <= week <= 52:
            return week
        else:
            print("Ikke en gyldig uke, vennligst skriv inn et tall fra 1 til 52.")


def days_in_month(year: int, month: int) -> list[str]:
    """Function to get number of days in month.

    Args:
        year: Year.
        month: Month.

    Returns:
        list: List with days in month.
    """
    num_days = monthrange(year, month)[
        1
    ]  # Get the number of days in the specified month
    days_list = [f"{year}-{month:02d}-{day:02d}" for day in range(1, num_days + 1)]
    return days_list


def extract_start_end_dates(file_name: str) -> tuple[datetime, datetime]:
    """Function to extract start and end dates from file name.

    Args:
        file_name: String value with name of file.

    Returns:
        tuple: Tuple with two datetime objects
    """
    match = re.search(r"_p(\d{4})-(\d{2})-(\d{2})_p(\d{4})-(\d{2})-(\d{2})_", file_name)
    if match:
        start_date = datetime.strptime(
            f"{match.group(1)}-{match.group(2)}-{match.group(3)}", "%Y-%m-%d"
        )
        end_date = datetime.strptime(
            f"{match.group(4)}-{match.group(5)}-{match.group(6)}", "%Y-%m-%d"
        )
    return start_date, end_date


def months_in_term(term: int) -> tuple[int, int]:
    """Gives out months as ints from term as int.

    Args:
        term: term

    Returns:
        tuple: months
    """
    month_term_dict = {
        1: (1, 2),
        2: (3, 4),
        3: (5, 6),
        4: (7, 8),
        5: (9, 10),
        6: (11, 12),
    }

    return month_term_dict[term]


def iterate_years_months(
    start_year: int, end_year: int, start_month: int, end_month: int
) -> Any:
    """Function to iterate over years and month.

    Allows you to select start year, start month, end year and end month

    Args:
        start_year: Int for start year.
        start_month: Int for start month.
        end_year: Int for end year.
        end_month: Int for end month.

    Yields:
        Any: A tuple containing the year and month for each combination.

    Raises:
        ValueError: If start year is bigger than end year.
        ValueError: If month is invalid number.
        ValueError: If end month is bigger than start and only iterating on one year.
    """
    if start_year > end_year:
        raise ValueError("Start year must be less than or equal to end year")
    if start_month < 1 or start_month > 12 or end_month < 1 or end_month > 12:
        raise ValueError("Month must be between 1 and 12")
    if start_year == end_year and start_month > end_month:
        raise ValueError(
            "If iterating in same year start month must be less than end month."
        )

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            if (year == start_year and month < start_month) or (
                year == end_year and month > end_month
            ):
                continue
            yield year, month


def bump_quarter(year: int, quarter: int) -> tuple[int, int]:
    """Bump period with a quarter further.

    E.g. 2023 and quarter 4 as input, will be returned as 2024 and quarter 1.

    Args:
        year: The year.
        quarter: The quarter.

    Returns:
        tuple: The year and quarter with an "added" quarter.
    """
    if quarter == 4:
        bumped_quarter = 1
        bumped_year = year + 1
    else:
        bumped_quarter = quarter + 1
        bumped_year = year

    return bumped_year, bumped_quarter


def validate_month(month: int | str) -> str:
    """Ensure month to have leading zero if before october.

    Args:
        month: the number of the month

    Returns:
        str: the number of the month with leading zero if relevant
    """
    if int(month) < 10:
        month = "0" + str(int(month))
    return str(month)


def validate_day(day: int | str) -> str:
    """Ensure day to have leading zero if it less than 10.

    Args:
        day: the number of the month

    Returns:
        str: the number of the day with leading zero if relevant
    """
    if int(day) < 10:
        day = "0" + str(int(day))
    return str(day)


def quarter_for_month(month: str | int) -> int:
    """Find corresponding quarter for a month.

    Args:
        month: Month to find corresponding quarter for.

    Returns:
        int: The corresponding quarter.

    Raises:
        ValueError: If invalid month
    """
    month = int(month)

    if month < 1 or month > 12:
        raise ValueError(f"Invalid month: {month}")

    if month < 4:
        return 1
    elif month < 7:
        return 2
    elif month < 10:
        return 3
    else:
        return 4


def months_in_quarter(quarter: int | str) -> list[int]:
    """Return the three months in the quarter.

    Args:
        quarter: the relevant quarter.

    Returns:
        list: a list with the months in the quarter.

    Raises:
        ValueError: If invalid quarter.
    """
    quarter = int(quarter)

    if quarter < 1 or quarter > 4:
        raise ValueError(f"Invalid quarter: {quarter}")

    if quarter == 1:
        return [1, 2, 3]
    elif quarter == 2:
        return [4, 5, 6]
    elif quarter == 3:
        return [7, 8, 9]
    else:
        return [10, 11, 12]


def set_publishing_date() -> str:
    """Set the date for publication of tables.

    Used for loading to Statbank.

    Returns:
        str: a date.
    """
    year = input("Year (YYYY): ")
    month = input("Month (MM): ")
    day = input("Day (DD): ")

    assert int(month) < 13, f"{month} > 12. There are only 12 months in a year."
    assert int(day) < 32, f"{day} > 31. It can only be maximum 31 days in a month."
    assert int(year) > 1000, "Year have to have four digits."

    date = f"{year}-{month}-{day}"

    return date


def check_publishing_date(date: str) -> str:
    """Validate the publishing date.

    Args:
        date: the date to check is on valid format.

    Returns:
        str: the returned and corrected date.
    """
    today = str(datetime.today().date())
    date_ok = False

    while date_ok is False:
        if today == date:
            print("Publishing date is set to today.")
            publish_today = input("If correct enter, 'yes': ")

            if publish_today.lower() != "yes":
                date = set_publishing_date()
            else:
                date_ok = True

        elif today > date:
            print("Publishing date has passed.")
            date = set_publishing_date()

        else:
            print(f"Publishing date is set to: {date}")
            date_ok = True

    return date


def publishing_date() -> str:
    """Set publishing dat at format YYYY-MM-DD.

    Returns:
        str: the date.
    """
    date = set_publishing_date()
    ok_date = check_publishing_date(date)
    return ok_date


def get_previous_month(year: str | int, month: str | int) -> list[int]:
    """Turn e.g. month 01 year 2023 into month 12 and year 2022.

    Args:
        year: the current year YYYY.
        month: the current month MM.

    Returns:
        list[int]: the previous month with year.
    """
    prev_month = int(month) - 1

    if prev_month != 0:
        prev_year = int(year)
    else:
        prev_month = 12
        prev_year = int(year) - 1

    return [prev_year, prev_month]
