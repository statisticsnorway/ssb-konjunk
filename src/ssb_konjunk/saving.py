"""Functions used for get in touch with you files and save them.

Follows the :crossed_fingers: the standardization for versioning and names.
"""

import os
import re
from re import Pattern

import dapla
import pandas as pd
from prompts import validate_month, validate_day
from timestamp import get_ssb_timestamp

def get_saved_file(
    dates: tuple[int|None],
    frequency: str, 
    name: str,
    datatilstand: str,
    bucket_statistikk: str,  
    folder_in_datatilstand: str = '',
    version_number: int = None,
    filetype: str = "parquet",
    fs: dapla.gcs.GCSFileSystem = None,
    seperator: str = ";",
    encoding: str = "latin1",
) -> pd.DataFrame:
    """Function to get a saved file.

    Get the last version saved in the datatilstand specified (klargjorte-data, statistikk, utdata)
    at the correct bucket path and with the speficed name.
    If it is a year table, the filename is automatically adjusted.

    Args:
        dates: Up to six arguments with int, to create timestamp for. E.g. (2022,2,2023,4) is p_2022-02_p2023-04 when frequency = 'M'.
        frequency: monthly (M), daily(D), quarter (Q), terital (T), weekly (W).
        name: name of the file. E.g. overnatting16landet1, alleover, alleover-utvida.
        datatilstand: the datatilstand for the file to get.
        bucket_statistikk: the bucket. Ex.: '/ssb/stamme04/reiseliv/NV/wk48/' or 'gs://ssb-<teamnavn>-data-produkt-prod/overnatting/'.
        folder_in_datatilstand: if there are folders under the datatilstand-level. Default: ''.
        version_number: possibility to get another version, than the newest (i.e. highest version number). Default: None.
        filetype: the filetype to save as. Default: 'parquet'.
        fs: the filesystem, pass with gsc Filesystem if Dapla. Default: None.
        seperator: the seperator to use it filetype is csv. Default: ';'.
        encoding: Encoding for file, base is latin1.
    Returns:
        pd.DataFrame: file as a data frame.
    """
    
    # Get the timestamp at corrext format
    timestamp = get_ssb_timestamp(*dates, frequency=frequency)

    # Combine timestamp and base name to filename
    filename = f"{name}_{timestamp}_v"
    
    # Validate all paths according to slashes and so
    bucket_statistikk = remove_edge_slashes(bucket_statistikk)
    datatilstand = remove_edge_slashes(datatilstand)
    
    # Make full file_path
    if folder_in_datatilstand != "":
        folder_in_datatilstand = remove_edge_slashes(folder_in_datatilstand)
        file_path = f"{bucket_statistikk}/{datatilstand}/{folder_in_datatilstand}/{filename}"
    else:
        file_path = f"{bucket_statistikk}/{datatilstand}/{filename}"
    
    # Get list with the filenames, if several, ordered by the highest version number at last. 
    files = get_files(file_path, fs=fs)

    # Case with no files. 
    if len(files) == 0:
        print(f"There are no such files. {filename}")
        return None
    
    # Get the newest version if not version_number is specified
    if version_number is None:
        path_file = files[-1]
    else:
        path_file = next((s for s in files if f'_v{version_number}' in s), None)

    # Different functions used for reading depending on the filetype
    if filetype == "csv":
        df = pd.read_csv(path_file, sep=seperator, encoding=encoding)
    elif filetype == "parquet":
        df = pd.read_parquet(path_file, filesystem=fs)

    return df


def get_files(folder_path: str, datatilstand: str=None, folder_in_datatilstand:str=None, filename: str=None, fs: dapla.gcs.GCSFileSystem = None) -> list[str]:
    """Function to list files in a folder based on base name and timestamp.
    
    Args:
        folder_path: String to folder.
        datatilstand: Name of the datatilstand.
        folder_in_datatilstand: if there are folders under the datatilstand-level. If none, ''.
        filename: String for name of file including timestamp.
        fs: FileSystem, if not using linux storage.
        
    Returns:
        list[str]: List with filenames.
    """
    filenames = []
    #match_string = f"{folder_path}/{datatilstand}/{folder_in_datatilstand}/{filename}*"
    match_string = f"{folder_path}*"
    if fs:
        filenames = fs.glob(match_string)
    else:
        filenames = glob.glob(match_string)
        
    # Sort it in stigende order with highest version number at the end
    filenames = sorted(filenames, key=lambda x: int(x.split('_v')[1].split('.')[0]))
    
    return filenames

def remove_edge_slashes(input_string:str) -> str:
    """
    """
    if input_string.startswith('/'):
        input_string = input_string[1:]
    if input_string.endswith('/'):
        input_string = input_string[:-1]
    return input_string

