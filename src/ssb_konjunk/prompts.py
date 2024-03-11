"""Function for date prompts in python.

The functions should be used to prompt which period to read files from og which period to run a statistic for.
"""

import re
from calendar import monthrange
from datetime import datetime
from typing import Any


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
            print("Vennligst skriv inn ett gyldig tall som,", 42)

    return valid_int


def input_year() -> int:
    """Input function for year.

    Returns:
        int: Year as int
    """
    # Get the desired year from the user
    print("Skriv inn år i format YYYY som", 2024)
    while True:
        year = _input_valid_int()
        if 2000 <= year <= 2030:
            return year
        else:
            print("Er du sikker på at du skal kjøre statistikk for dette året,", year)
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
    print("Skriv inn termin i format t, som:", 3)
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
            print("Ikke en gyldig kvartal, vennligst skriv inn et tall fra 1 til 4.")


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


def find_file_for_month_daily(
    files: list[str], desired_year: int, desired_month: int
) -> str:
    """Function to retrieve spesific file str for period form list of file strings.

    Args:
        files: List of file strings.
        desired_year: Int to represent year(yyyy).
        desired_month: Int to represent a month(m).

    Returns:
        str: Filename on linux, Filepath on dapla
    """
    for file in files:
        if (
            extract_start_end_dates(file)[0].year == desired_year
            and extract_start_end_dates(file)[1].year == desired_year
            and extract_start_end_dates(file)[0].month == desired_month
            and extract_start_end_dates(file)[1].month == desired_month
        ):
            break
    return file


def delta_month(month: int, periods: int) -> int:
    """Function to shift month backwards or forward.

    Args:
        month: Current month you are using.
        periods: Periods you want to move, can be positive or negative int.

    Returns:
        int: Month we have shifted to.

    Raises:
        ValueError: If periods are are more or less than a year.
        ValueError: If period is 0.
    """
    if periods < -11 or periods > 11:
        raise ValueError(
            "Input periods must be between -11 and 11. If youre doing a going a year back in time, please change year."
        )
    elif periods == 0:
        raise ValueError("Input periods is 0, you should remove the function!")
    else:
        # Taking period adding periods.
        new_month = month + (periods)
        # If new_month is above twelve, i take negative twelve to get month from new year.
        if new_month > 12:
            new_month = new_month - 12
        # If new_month is below 1, I take 12 negative new_month. This should give the right month from last year.
        elif new_month < 1:
            new_month = new_month + 12
        return new_month


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
