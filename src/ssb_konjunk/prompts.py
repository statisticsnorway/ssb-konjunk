"""Function for date prompts in python.

The functions should be used to prompt which period to read files from og which period to run a statistic for.
"""

import re
from calendar import monthrange
from datetime import datetime
from datetime import timedelta


def days_in_month(year: int, month: int) -> list[str]:
    """Function to get number of days in month.

    Parameters
    ----------
        year (int): Year.
        month (int): Month.

    Returns:
    -------
        days_list (list): List with days in month.
    """
    num_days = monthrange(year, month)[
        1
    ]  # Get the number of days in the specified month
    days_list = [f"{year}-{month:02d}-{day:02d}" for day in range(1, num_days + 1)]
    return days_list


def extract_start_end_dates(file_name: str) -> tuple[datetime, datetime]:
    """Function to extract start and end dates from file name.

    Parameters
    ----------
        file_name (str): String value with name of file.

    Returns:
    -------
        start_date (datetime): Start date of file.
        end_date (datetime): End date of file.
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


def next_month(desired_year: int, desired_month: int) -> str:
    """Get the next month of a given year and month.

    Parameters
    ----------
        desired_year (int): year
        desired_month (int): month

    Returns:
    -------
        next_month_str (str): next month in the format 'YYYY-MM'
    """
    # Create a datetime object for the desired year and month
    current_date = datetime(desired_year, desired_month, 1)

    # Add one month to the current date
    next_date = current_date + timedelta(days=32)

    # Get the year and month of the next date
    next_year = next_date.year
    next_month = next_date.month

    # Format the year and month as 'YYYY-MM'
    next_month_str = f"{next_year}-{next_month:02d}"

    return next_month_str


def input_desired_year() -> int:
    """Get the desired year from the user.

    Returns:
    -------
        desired_year (int): year
    """
    # Get the desired year from the user
    while True:
        user_input = input("Skriv inn år(åååå): ")

        try:
            desired_year = int(user_input)
            break  # Break the loop if a valid integer is entered
        except ValueError:
            print("Vennligst sett inn gyldig år, som:", 2023)

    return desired_year


def input_desired_month() -> int:
    """Get the desired month from the user.

    Returns:
    -------
        desired_month (int): month
    """
    # Get the desired month from the user
    while True:
        user_input = input("Skriv inn måned(m): ")

        try:
            desired_month = int(user_input)
            print("Leter i måned:", desired_month)
            break  # Break the loop if a valid integer is entered
        except ValueError:
            print("Vennligst sett inn gyldig måned, som:", 1)

    return desired_month


def input_desired_term() -> int:
    """Get the desired term from the user.

    Returns:
    -------
        desired_term (int): term
    """
    # Get the desired month from the user
    while True:
        user_input = input("Skriv inn termin(t): ")

        try:
            desired_term = int(user_input)
            if desired_term < 7:
                print("Leter i termin:", desired_term)
                break  # Break the loop if a valid integer is entered
        except ValueError:
            print("Vennligst sett inn gyldig termin, som:", 1)

    return desired_term


def months_in_term(term: int) -> tuple[int, int]:
    """Gives out months as ints from term as int.

    Parameters
    ----------
        term (int): term

    Returns:
    -------
        month_term_dict[term] (tuple[int]): months
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

    Parameters
    ----------
        files (list[str]): List of file strings.
        desired_year (int): Int to represent year(yyyy).
        desired_month (int): Int to represent a month(m).

    Returns:
    -------
        file (str): Filename on linux, Filepath on dapla
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
