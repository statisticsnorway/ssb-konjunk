"""Functions used for get in touch with you files and save them.

Follows the :crossed_fingers: the standardization for versioning and names.
"""
import re
import glob
import pandas as pd
from ssb_konjunk import timestamp


def get_files(folder_path: str, base_name: str, time_stamp:str, fs: dapla.gcs.GCSFileSystem = None) -> list[str]:
    """Function to list files in a folder based on base name and timestamp.
    
    Args:
        folder_path: String to folder.
        base_name: String for base name of file.
        time_stamp: String of time stamp.
        fs: FileSystem, if not using linux storage.
        
    Returns:
        list[str]: List with filenames.
    """
    filenames = []
    match_string = f"{folder_path}/{base_name}_{time_stamp}*"
    if fs:
        filenames = fs.glob(match_string)
    else:
        filenames = glob.glob(match_string)
    return filenames
    

def get_version(filename:str) -> int|None:
    """Function to do regex to find version number.
    
    Args:
        filename: String to find version number in.
        
    Returns:
        int|None: Return int if found version number else returns None.
    """
    pattern = r'_v(\d{1,3})\.(parquet|csv)$'
    
    match = re.search(pattern, filename)
    if match:
        return = match.group(1)
    else:
        return None
    
    
def get_versions_for_filenames(filenames:list[str]) -> dict[int,str]|dict[]:
    """Function to find version number in files and return filenames versioned.
    
    Args:
        filenames: List with filenames.
        
    Returns:
        dict[int,str]: Dictionairy with int for keys representing versions and str for value representing filename.
    """
    version_dict = {}
    
    for filename in filenames:
        version = get_version(filename)
        if version:
            version_dict[version] = filename
        else:
            continue

    return version_dict


def get_highest_version(version_dict:dict[int,str]) -> int:
    """Function to get highest version number from version_dict.
    
    Args:
        version_dict: Dictionairy with keys being version, values being filenames.
        
    Returns:
        int: Int of highest version.
    """
    return max(version_dict.keys())


def read_file_logic(filename:str,filetype:str="parquet",fs: dapla.gcs.GCSFileSystem = None,) -> pd.DataFrame|None:
    """Placeholder function should extend with multiple filetypes and check extension of filename."""
    
    if "parquet" not in filename:
        print("Fungerer bare med parquet for nå!")
        return None
    else:
        
        df = pd.read_parquet(filename, filesystem = fs)
        
        return df


def read_ssb_file(
    folder_path: str,
    name: str,
    year: int = None,
    mid_date: int = None,
    day: int = None,
    end_year: int = None,
    end_mid_date: int = None,
    end_day: int = None,
    frequency: str = "m",
    version: int = None,
    filetype: str = "parquet", #Strengt tatt ikke nødvendig.
    seperator: str = ";",
    encoding: str = "latin1", 
    fs: dapla.gcs.GCSFileSystem = None,
) -> pd.DataFrame|None:
    """Function to read ssb files.
    
    Args:
        folder_path: String to folder on linux or GCP.
        name: File name.
        year: Year file starts in.
        mid_date: Int for date after year.
        day: Day file stars in, if empty assumes file contains a whole month or year.
        end_year: End year for file, if empty assumes file contains only one year.
        end_mid_date: Int for date after year.
        end_day: End day for file, if empty assumes file contains only one day or whole years or months.
        version: Version of file, if specified the function will read this version, else it will read newest version.
        filetype: File type.
        seperator: Seperator for text based storage formats.
        encoding: Encoding for file, base is latin1.
        fs: Filesystem if not working on linux.
    
    Returns:
        pd.DataFram|None: Returns a pandas dataframe if file found.
    """
    # Create timestamp if given dates.
    time_stamp = timestamp.get_ssb_timestamp(year,end_mid_date,day,end_year,end_mid_date,end_day,frequency=frequency)
    # Find files that match name and date.
    files = get_files(
        folder_path=folder_path,
        base_name=name,
        timestamp=time_stamp
        fs=fs,
    )
    # Get file version
    if len(files) != 1: #if more than one version get versions.
        files_versioned = get_versions_for_filenames(files)
        
        if version: #If you know which version you want, get that one.
            filename = files_versioned[version]
        else: #Else get newest version.
            filename = get_highest_version(files_versioned)
    else: #If only one version, just get that one.
        filename = files[0]
    # Returning read file.    
    return read_file_logic(filename=filename,filetype=filetype,fs=fs)
        