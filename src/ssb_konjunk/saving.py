"""Functions used for get in touch with you files and save them.

Follows the :crossed_fingers: the standardization for versioning and names.
"""

import os
import re
from re import Pattern

import dapla
import pandas as pd
from ssb_konjunk.prompts import validate_month
from ssb_konjunk.prompts import validate_day

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
        name: name of the file. E.g. overnatting16landet1, alleover, alleover-utvida.
        datatilstand: the datatilstand for the file to get.
        bucket_path: the whole path, without file name and datatilstand. Ex.: '/ssb/stamme04/reiseliv/NV/wk48/'.
        year: the year for the data.
        month: the relevant month. Default: ''.
        day: the relvant date. Default: ''.
        filetype: the filetype to save as. Default: 'parquet'.
        fs: the filesystem, pass with gsc Filesystem if Dapla. Default: None.
        seperator: the seperator to use it filetype is csv. Default: ';'.
        end_year: if the data covers a period, time series, the end year here. Default: ''.
        end_month: if the data covers a period, time series, the end month here. Default: ''.
        end_day: if the data covers a period, time series, the end day here. Default: ''.

    Returns:
        pd.DataFrame: file as a data frame.
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
    start_quarter: int | str = "",
    start_week: int | str = "",
    start_bimester: int | str = "",
    start_tertial: int | str = "",
    start_halfyear: int | str = "",
    end_year: int | str = "",
    end_month: int | str = "",
    end_day: int | str = "",
    end_quarter: int | str = "",
    end_week: int | str = "",
    end_bimester: int | str = "",
    end_tertial: int | str = "",
    end_halfyear: int | str = "",
) -> str:
    """Return a filename with correct timeperiod accroding to the navnestandard.

    Args:
        base_name: the name of the file. E.g. alleover-utvida.
        start_year: the first year if time period, else the only year. YYYY.
        start_month: the first month if time period, else a specific month, or ''. Default: ''.
        start_day: the first day if time period, else a specific date. Default: ''.
        start_quarter: the first quarter in time period, else specific quarter. Defualt: ''.
        start_week: the first week in the time period. Default: ''.
        start_bimester: the first bimester in the time period. Default: ''.
        start_tertial: the first tertial in the time period. Default: ''.
        start_halfyear: the first halfyear in the time period. Default: ''.
        end_year: the last year if timeperiod. YYYY. Defualt: ''.
        end_month: the last month if time period. Default: ''.
        end_day: the last day if time period. Default: ''.
        end_quarter: the last quarter if time period. Default: ''.
        end_week: the last week if time period. Default: ''.
        end_bimester: the last bimester if time period. Default: ''.
        end_tertial: the last tertial if time period. Default: ''.
        end_halfyear: the last halfyear if time period. Default: ''.

    Returns:
        str: the filename with correct date.
    """  
    # Not possible to use both quarter and month in same filename
    if (start_quarter != "" and start_month != "") | (start_quarter != "" and start_day != "") | (end_quarter != "" and end_month != "") |(end_quarter != "" and end_day != ""):
        print("Not valid time period passed, return filename without any time period specification.")
        return base_name
    
    # Handle quarter periods
    if start_year != "" and start_quarter != "" and end_year != "" and end_quarter != "":
        return f"{base_name}_p{start_year}Q{start_quarter}_p{end_year}Q{end_quarter}_v"
    
    elif start_year != "" and start_quarter != "" and end_year == "" and end_quarter == "":
        return f"{base_name}_p{start_year}Q{start_quarter}_v"
    
    # Handle weekly periods
    if start_year != "" and start_week != "" and end_year != "" and end_week != "":
        return f"{base_name}_p{start_year}W{start_week}_p{end_year}W{end_week}_v"
    
    elif start_year != "" and start_week != "" and end_year == "" and end_week == "":
        return f"{base_name}_p{start_year}W{start_week}_v"
    
    # Handle bimester periods
    if start_year != "" and start_bimester != "" and end_year != "" and end_bimester != "":
        return f"{base_name}_p{start_year}B{start_bimester}_p{end_year}B{end_bimester}_v"
    
    elif start_year != "" and start_bimester != "" and end_year == "" and end_bimester == "":
        return f"{base_name}_p{start_year}B{start_bimester}_v"
    
    # Handle tertial periods
    if start_year != "" and start_tertial != "" and end_year != "" and end_tertial != "":
        return f"{base_name}_p{start_year}T{start_tertial}_p{end_year}T{end_tertial}_v"
    
    elif start_year != "" and start_tertial != "" and end_year == "" and end_tertial == "":
        return f"{base_name}_p{start_year}T{start_tertial}_v"
    
    # Handle halfyear periods
    if start_year != "" and start_halfyear != "" and end_year != "" and end_halfyear != "":
        return f"{base_name}_p{start_year}H{start_halfyear}_p{end_year}H{end_halfyear}_v"
    
    elif start_year != "" and start_halfyear != "" and end_year == "" and end_halfyear == "":
        return f"{base_name}_p{start_year}H{start_halfyear}_v"
    
    # Handle monthly and daily periods
    if start_year != "" and start_month != "" and start_day != "" and end_year != "" and end_month != "" and end_day != "":
        start_month = validate_month(start_month)
        end_month = validate_month(end_month)
        start_day = validate_day(start_day)
        end_day = validate_day(end_day)
        return f"{base_name}_p{start_year}-{start_month}-{start_day}_p{end_year}-{end_month}-{end_day}_v"
        
    elif start_year != "" and start_month != "" and end_year != "" and end_month != "":
        start_month = validate_month(start_month)
        end_month = validate_month(end_month)
        return f"{base_name}_p{start_year}-{start_month}_p{end_year}-{end_month}_v"
        
    elif start_year != "" and end_year != "" and start_month == "" and end_month == "" and start_day == "" and end_day == "":
        return f"{base_name}_p{start_year}_p{end_year}_v"
        
    elif start_year != "" and start_month != "" and start_day != "":
        start_month = validate_month(start_month)
        start_day = validate_day(start_day)
        return f"{base_name}_p{start_year}-{start_month}-{start_day}_v"
        
    elif start_year != "" and start_month != "" and end_year == "" and end_month == "":
        start_month = validate_month(start_month)
        return f"{base_name}_p{start_year}-{start_month}_v"
        
    elif start_year != "" and end_year == "":
        return f"{base_name}_p{start_year}_v"
    
    else:
        print("Not valid time period passed, return filename without any time period specification")
        return base_name



def get_versions(
    folder_path: str, filename: str, filename_pattern: Pattern[str]
) -> list[str]:
    """Get all the versions that exists of a file.

    Args:
        folder_path: the whole path, without file name. Ex.: '/ssb/stamme04/reiseliv/NV/wk48/klargjorte-data/'.
        filename: the name of the file, without version and file type. Ex.: 'alleover-utvida_p2023-02_v'.
        filename_pattern: the whole filename, including pattern of version and filetype. Ex.: re.compile(rf'{filename}(d+).parquet').

    Returns:
        list[str]: versions, list with the version numbers existing for the filename.
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
