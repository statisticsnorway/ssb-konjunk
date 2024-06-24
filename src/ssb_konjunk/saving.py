"""Functions used for get in touch with you files and save them.

Follows the :crossed_fingers: the standardization for versioning and names.
"""

import os
import re
from re import Pattern

import dapla
import pandas as pd
import prompts


def get_saved_file(
    name: str,
    datatilstand: str,
    bucket_path: str,
    year: int | str = "",
    month: int | str = "",
    day: int | str = "",
    filetype: str = "parquet",
    fs: dapla.gcs.GCSFileSystem = None,
    seperator: str = ";",
    end_year: int | str = "",
    end_month: int | str = "",
    end_day: int | str = "",
) -> pd.DataFrame:
    """Function to get a saved file.

    Get the last version saved in the datatilstand specified (klargjorte-data, statistikk, utdata)
    at the correct bucket path and with the speficed name.
    If it is a year table, the filename is automatically adjusted.

    Args:
        name         : name of the file. E.g. overnatting16landet1, alleover, alleover-utvida
        datatilstand : the datatilstand for the file to get
        bucket_path  : the whole path, without file name and datatilstand. Ex.: '/ssb/stamme04/reiseliv/NV/wk48/'
        year         : the year for the data
        month        : the relevant month. Default: ''
        day          : the relvant date. Default: ''
        filetype     : the filetype to save as. Default: 'parquet'
        fs           : the filesystem, pass with gsc Filesystem if Dapla. Default: None
        seperator    : the seperator to use it filetype is csv. Default: ';'
        end_year     : if the data covers a period, time series, the end year here. Default: ''
        end_month    : if the data covers a period, time series, the end month here. Default: ''
        end_day      : if the data covers a period, time series, the end day here. Default: ''
    Return:
        df           : file as a data frame
    """
    # Find filename with correct time period
    filename = get_time_period_standard(
        base_name=name,
        start_year=year,
        start_month=month,
        end_year=end_year,
        end_month=end_month,
    )

    # Get newest version number
    versions = get_versions(
        bucket_path + datatilstand + "/",
        filename,
        re.compile(rf"{filename}(\d+)\.{filetype}"),
    )

    if len(versions) == 0:
        print(f"There are no such files. {filename}")
        return pd.DataFrame()
    else:
        path = (
            bucket_path + datatilstand + "/" + filename + f"{versions[-1]}.{filetype}"
        )

        # Different functions used for reading depending on the filetype
        if filetype == "csv":
            df = pd.read_csv(path, sep=seperator)
        elif filetype == "parquet":
            df = pd.read_parquet(path, filesystem=fs)

        return df


def get_time_period_standard(
    base_name: str,
    start_year: int | str = "",
    start_month: int | str = "",
    start_day: int | str = "",
    end_year: int | str = "",
    end_month: int | str = "",
    end_day: int | str = "",
) -> str:
    """Return a filename with correct timeperiod accroding to the navnestandard.

    Args:
        base_name   : the name of the file. E.g. alleover-utvida
        start_year  : the first year if time period, else the only year. YYYY
        start_month : the first month if time period, else a specific month, or ''. Default: ''
        start_day   : the first day if time period, else a specific date. Default: ''
        end_year    : the last year if timeperiod. YYYY. Defualt: ''
        end_month   : the last month if time period. Default: ''
        end_day     : the last day if time period. Default: ''
    Return
        filename    : the filename with correct date.
    """
    try:
        # Specific month, no time period
        if start_month != "" and end_year == "":
            filename = (
                f"{base_name}_p{start_year}-{prompts.validate_month(start_month)}_v"
            )

        # Only whole year, no time period
        elif start_month == "" and end_year == "":
            filename = f"{base_name}_p{start_year}_v"

        # Whole year, time period
        elif start_month == "" and end_year != "":
            filename = f"{base_name}_p{start_year}-p{end_year}_v"

        # Specific month, time period
        elif start_month != "" and end_year != "" and end_month != "":
            filename = f"{base_name}_p{start_year}-{prompts.validate_month(start_month)}-p{end_year}-{prompts.validate_month(end_month)}_v"

        return filename

    except Exception as e:
        print(e)
        print(
            f"Something is not valid.\nStart year:{start_year}\nStart month: {start_month}\nEnd month: {end_month}\nEnd year: {end_year}"
        )
        return ""


def get_versions(
    folder_path: str, filename: str, filename_pattern: Pattern[str]
) -> list[str]:
    """Get all the versions that exists of a file.

    Args:
        folder_path      : the whole path, without file name. Ex.: '/ssb/stamme04/reiseliv/NV/wk48/klargjorte-data/'
        filename         : the name of the file, without version and file type. Ex.: 'alleover-utvida_p2023-02_v'
        filename_pattern : the whole filename, including pattern of version and filetype. Ex.: re.compile(rf'{filename}(d+).parquet')

    Return:
        versions         : list with the version numbers existing for the filename.
    """
    versions = []

    # Search for existing files in folder
    for filename in os.listdir(folder_path):
        match = filename_pattern.match(filename)

        if match:
            # File matches the pattern
            version = match.group(1)
            versions.append(version)

    return versions
