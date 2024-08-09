"""Functions used for get in touch with you files and save them.

Follows the :crossed_fingers: the standardization for versioning and names.
"""

import glob
import re

import dapla as dp
import pandas as pd
import numpy as np
from timestamp import get_ssb_timestamp


def get_ssb_file(
    dates: tuple[int | None],
    frequency: str,
    name: str,
    datatilstand: str,
    bucket_statistikk: str,
    folder_in_datatilstand: str = "",
    version_number: int = np.nan,
    filetype: str = "parquet",
    fs: dp.gcs.GCSFileSystem = None,
    seperator: str = ";",
    encoding: str = "latin1",
) -> pd.DataFrame:
    """Function to get a saved file, stored at SSB-format.

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
        version_number: possibility to get another version, than the newest (i.e. highest version number). Default: np.nan.
        filetype: the filetype to save as. Default: 'parquet'.
        fs: the filesystem, pass with gsc Filesystem if Dapla. Default: None.
        seperator: the seperator to use it filetype is csv. Default: ';'.
        encoding: Encoding for file, base is latin1.

    Returns:
        pd.DataFrame: file as a data frame.
    """
    # Get the filepath, only without version number and filetype
    file_path = structure_ssb_filepath(
        dates=dates,
        frequency=frequency,
        name=name,
        datatilstand=datatilstand,
        bucket_statistikk=bucket_statistikk,
        folder_in_datatilstand=folder_in_datatilstand,
    )
    # Get list with the filenames, if several, ordered by the highest version number at last.
    files = get_files(file_path, fs=fs)

    # Case with no files.
    if len(files) == 0:
        print(f"There are no such files. {file_path}")
        return None
    # Get the newest version if not version_number is specified
    if version_number is None:
        path_file = files[-1]
    else:
        path_file = next((s for s in files if f"_v{version_number}" in s), None)

    # Different functions used for reading depending on the filetype
    if filetype == "csv":
        df = pd.read_csv(path_file, sep=seperator, encoding=encoding)
    elif filetype == "parquet":
        df = pd.read_parquet(path_file, filesystem=fs)

    return df


def save_ssb_file(
    df: pd.DataFrame,
    dates: tuple[int | None],
    frequency: str,
    stable_version: bool,
    name: str,
    datatilstand: str,
    bucket_statistikk: str,
    folder_in_datatilstand: str = "",
    filetype: str = "parquet",
    fs: dp.gcs.GCSFileSystem = None,
    seperator: str = ";",
    encoding: str = "latin1",
):
    """Function to save a file at SSB-format.

    Args:
        df: The dataframe to save.
        dates: Up to six arguments with int, to create timestamp for. E.g. (2022,2,2023,4) is p_2022-02_p2023-04 when frequency = 'M'.
        frequency: monthly (M), daily(D), quarter (Q), terital (T), weekly (W).
        stable_version: whether the version is stable or not. If True, versionize, if False version = 0.
        name: name of the file. E.g. overnatting16landet1, alleover, alleover-utvida.
        datatilstand: the datatilstand for the file to get.
        bucket_statistikk: the bucket. Ex.: '/ssb/stamme04/reiseliv/NV/wk48/' or 'gs://ssb-<teamnavn>-data-produkt-prod/overnatting/'.
        folder_in_datatilstand: if there are folders under the datatilstand-level. Default: ''.
        version_number: possibility to get another version, than the newest (i.e. highest version number). Default: None.
        filetype: the filetype to save as. Default: 'parquet'.
        fs: the filesystem, pass with gsc Filesystem if Dapla. Default: None.
        seperator: the seperator to use it filetype is csv. Default: ';'.
        encoding: Encoding for file, base is latin1.

    Raises:
        AssertionError: if df has no rows.
    """
    # Check content in df
    assert (
        len(df) > 0
    ), "There are no rows in the dataset. Fix this and try to save again."

    # Veirfy name of datatilstand and base filename
    name = verify_base_filename(name)
    datatilstand = verify_datatilstand(datatilstand)

    # Get the filepath, only without version number and filetype
    file_path = structure_ssb_filepath(
        dates=dates,
        frequency=frequency,
        name=name,
        datatilstand=datatilstand,
        bucket_statistikk=bucket_statistikk,
        folder_in_datatilstand=folder_in_datatilstand,
    )

    # Get list with the filenames, if several, ordered by the highest version number at last.
    files = get_files(file_path, fs=fs)

    # Find version number/decide whether to overwrite or make new version.
    version_number = find_version_number(files, stable_version)
    save_df(df, file_path, version_number, fs, filetype, seperator, encoding)


def find_version_number(files: list[str], stable_version: bool) -> str:
    """Find the correct version number to use for saving.

    Options about overwriting or making new version as well.

    Args:
        files: list with the files with same filename in the relevant folder.
        stable_version: If False version 0 is used.

    Returns:
        str: The number to use for the version.
    """
    existing_versions = [re.search(r"_v([^_]+)$", f).group(1) for f in files]
    print(existing_versions)
    if not stable_version and len(files) == 0:
        return "0"
    elif not stable_version and existing_versions[-1] == "0":
        return "0"
    elif not stable_version and existing_versions[-1] != "0":
        print(
            f"FYI: A stable version {files[-1]} is saved previously. Saves as version 0."
        )
        return "0"
    elif stable_version and len(files) == 0:
        return "1"
    elif stable_version and existing_versions[-1] == "0":
        return "1"
    elif stable_version and existing_versions[-1] != "0":
        overwrite = input(
            f"Do you want to overwrite the existing version (_v{existing_versions[-1]})? Confirm by 'y'"
        )
        if overwrite.lower() == "y":
            version = existing_versions[-1]
            version = version.split(".")[0]
            return str(version)
        else:
            make_new_version = input(
                "Do you want to make a new version (i.e. increase the version number by one)? Confirm by 'y'"
            )
            if make_new_version.lower() == "y":
                version = existing_versions[-1]
                version = int(version.split(".")[0]) + 1
                return str(version)
            else:
                return None
    else:
        raise ValueError("Somethin went wrong when finding the version number.")


def save_df(
    df: pd.DataFrame,
    file_path: str,
    version_number: str,
    fs: dp.gcs.GCSFileSystem | None,
    filetype: str,
    seperator: str,
    encoding: str,
):
    """Do the actual saving, either as csv or parquet.

    Args:
        df: The pandas dataframe to save.
        file_path: The full path with filename, and without filetype and version number. E.g. 'gs://ssb-prod-data/utdata/minfil_p2022_v'.
        version_number: The version number of the file.
        fs: the file system, None if prodsone.
        filetype: The type of the file. Either csv or parquet.
        seperator: To use if csv.
        encondig: To use if csv.
    """
    if version_number is None:
        print("No saving performed.")
    else:
        # Save as parquet
        if filetype == "parquet":
            file_path = file_path + version_number + ".parquet"
            try:
                df.to_parquet(file_path, index=False)
            except PermissionError:
                dp.write_pandas(
                    df=df,
                    gcs_path=file_path,
                    file_format="parquet",
                )

        # Save as csv
        elif filetype == "csv":
            file_path = file_path + version_number + ".csv"
            try:
                df.to_csv(whole_path, sep=seperator, index=False, encoding=encoding)
            except PermissionError:
                dp.write_pandas(
                    df=df,
                    gcs_path=file_path,
                    file_format="csv",
                    sep=seperator,
                    index=False,
                    encoding=encoding,
                )
        # Uknown filetype sent as argument
        else:
            print(
                f"The filetype {filetype} is not supported for saving. Nothing saved for {file_path}."
            )


def structure_ssb_filepath(
    dates: tuple[int | None],
    frequency: str,
    name: str,
    datatilstand: str,
    bucket_statistikk: str,
    folder_in_datatilstand: str = "",
) -> str:
    """Structure the name of the file to SSB-format and the path.

    Args:
        dates: Up to six arguments with int, to create timestamp for. E.g. (2022,2,2023,4) is p_2022-02_p2023-04 when frequency = 'M'.
        frequency: monthly (M), daily(D), quarter (Q), terital (T), weekly (W).
        name: name of the file. E.g. overnatting16landet1, alleover, alleover-utvida.
        datatilstand: the datatilstand for the file to get.
        bucket_statistikk: the bucket. Ex.: '/ssb/stamme04/reiseliv/NV/wk48/' or 'gs://ssb-<teamnavn>-data-produkt-prod/overnatting/'.
        folder_in_datatilstand: if there are folders under the datatilstand-level. Default: ''.

    Returns:
        str: the full path to the file.
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
        file_path = (
            f"{bucket_statistikk}/{datatilstand}/{folder_in_datatilstand}/{filename}"
        )
    else:
        file_path = f"{bucket_statistikk}/{datatilstand}/{filename}"
    return file_path


def get_files(folder_path: str, fs: dp.gcs.GCSFileSystem = None) -> list[str]:
    """Function to list files in a folder based on base name and timestamp.

    Args:
        folder_path: String to folder.
        fs: FileSystem, if not using linux storage.

    Returns:
        list[str]: List with filenames.
    """
    filenames = []

    match_string = f"{folder_path}*"
    if fs:
        filenames = fs.glob(match_string)
    else:
        filenames = glob.glob(match_string)
    # Sort it in stigende order with highest version number at the end
    filenames = sorted(
        filenames,
        key=lambda x: int(re.search(r"_v(\d+)(?=\.(parquet|csv)$)", x).group(1)),
    )
    return filenames


def remove_edge_slashes(input_string: str) -> str:
    """Function to remove edge slashes in strings.

    Args:
        input_string: The string to remove / for.

    Returns:
        str: String without slashes.
    """
    if input_string.startswith("/"):
        input_string = input_string[1:]
    if input_string.endswith("/"):
        input_string = input_string[:-1]
    return input_string


def verify_base_filename(name: str) -> str:
    """Verifies the base of the file name.
    Corrects small errors
    as upper case letters in the base of the file name.

    Args:
        name: the base of the filename.

    Returns:
        name: a corrected base of the filename if necessary.

    Raises:
        ValueError
    """
    # Ensure lower case
    if any(letter.isupper() for letter in name):
        old_name = name
        name = name.lower()
        print(
            f"Base filename changed from {old_name} to {name}. No upper case letters allowed."
        )
    # Raises error if starts with numbers
    if name[0].isdigit():
        raise ValueError(
            f"Base filename cannot start with a digit. Change it. \nCurrent base filename: {name}."
        )
    if "parquet" in name:
        raise ValueError(
            "The file format 'parquet' should not be specified in the base file name. Remove it."
        )

    if "csv" in name:
        raise ValueError(
            "The file format 'csv' should not be specified in the base file name. Remove it."
        )
    # Raises error if not valid signs are present in base file name
    not_valid_signs = [
        ".",
        " ",
        "!",
        ",",
        "æ",
        "ø",
        "å",
        "*",
        "^",
        "?",
        "#",
        "@",
        "%",
        "&",
        "(",
        ")",
    ]
    for letter in name:
        if letter in not_valid_signs:
            raise ValueError(
                f"Not valid signs present in base filename. Not valid signs are: {not_valid_signs}"
            )
    return name


def verify_datatilstand(datatilstand: str) -> str:
    """Veirfy the name of the datatilstand.
    The level 'temp' is here included as a valid datatilstand.

    Args:
        datatilstand: the name of the datatilstand.

    Returns:
        str: the (corrected) name of the datatilstand.
    """
    datatilstand = datatilstand.lower()
    if datatilstand not in [
        "inndata",
        "klargjorte-data",
        "statistikk",
        "utdata",
        "temp",
    ]:
        datatilstand = input(
            "Datatilstand må være enten inndata, klargjorte-data, statistikk eller utdata:"
        )
        datatilstand = verify_datatilstand(datatilstand)
    else:
        return datatilstand
