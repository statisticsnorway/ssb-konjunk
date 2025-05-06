"""Functions used for get in touch with you files and save them.

Follows the the standardization for versioning and names.
"""

import glob
import re

import dapla
import pandas as pd

from ssb_konjunk import timestamp


def _remove_edge_slashes(input_string: str, only_last: bool = False) -> str:
    """Function to remove edge slashes in strings.

    Args:
        input_string: The string to remove / for.
        only_last: True if only move the potential last edge slash. Default: False.

    Returns:
        str: String without slashes.
    """
    if input_string.startswith("/") and not only_last:
        input_string = input_string[1:]
    if input_string.endswith("/"):
        input_string = input_string[:-1]
    return input_string


def _structure_ssb_filepath(
    periode: tuple[int, ...],
    frequency: str,
    bucket: str,
    kortnavn: str,
    datatilstand: str,
    file_name: str,
    undermappe: str | None = None,
    version_number: int | None = None,
    filetype: str = "parquet",
    fs: dapla.gcs.GCSFileSystem | None = None,
) -> str:
    """Structure the name of the file to SSB-format and the path.

    Args:
        periode: Dates like ints in tuple for timestamp.
        frequency: Frequency like str for timestamp.
        bucket: String for gcp bucket.
        kortnavn: String for statistic or data-product.
        datatilstand: String for 'datatilstand'.
        file_name: String for Filename.
        undermappe: Optional string for if you want folders betwen 'datatilstand' and file.
        version_number: Optional int for reading specific file.
        filetype: String with default 'parquet', specifies file type.
        fs: the filesystem, pass with gsc Filesystem if Dapla. Default: None.

    Returns:
        str: the full path to the file.

    Raises:
        ValueError: Raise if version number is not None or int.
    """
    # Handle that path starts with / in prodsonen.
    if fs is None:
        bucket = _remove_edge_slashes(bucket, only_last=True)
    else:
        bucket = _remove_edge_slashes(bucket)
    kortnavn = _remove_edge_slashes(kortnavn)
    datatilstand = _remove_edge_slashes(datatilstand)
    file_name = _remove_edge_slashes(file_name)

    # Get the timestamp at correct format
    time_stamp = timestamp.get_ssb_timestamp(*periode, frequency=frequency)

    # Handle case with undermappe in datatilstand and temp and oppdrag folders.
    if undermappe and undermappe != "" and datatilstand != "":
        undermappe = _remove_edge_slashes(undermappe)
        file_path = f"{bucket}/{kortnavn}/{datatilstand}/{undermappe}"
    elif undermappe and undermappe != "" and datatilstand == "":
        undermappe = _remove_edge_slashes(undermappe)
        file_path = f"{bucket}/{kortnavn}/{undermappe}"
    elif (not undermappe or undermappe == "") and datatilstand != "":
        file_path = f"{bucket}/{kortnavn}/{datatilstand}"
    else:
        file_path = f"{bucket}/{kortnavn}"

    # Handle versionizing or not.
    if version_number is None:
        file_path = f"{file_path}/{file_name}_{time_stamp}_"
    elif isinstance(version_number, int):
        file_path = f"{file_path}/{file_name}_{time_stamp}_v{version_number}.{filetype}"
    else:
        raise ValueError("version_number has to be int or None.")
    return file_path


def _get_files(
    folder_path: str, filetype: str, fs: dapla.gcs.GCSFileSystem | None
) -> list[str]:
    """Function to list files in a folder based on base name and timestamp."""
    filenames = []

    match_string = f"{folder_path}*"
    if fs:
        filenames = fs.glob(match_string)
    else:
        filenames = glob.glob(match_string)

    # Only include files with the relevant file extension
    filenames = [i for i in filenames if i.endswith(filetype)]

    # Sorts the filenames according to version numbers.
    filenames.sort()

    return filenames


def _find_version_number(files: list[str], stable_version: bool) -> str | None:
    """Find the correct version number to use for saving."""
    existing_versions = []
    for f in files:
        match = re.search(r"_v([^_]+)$", f)
        if match:
            existing_versions.append(match.group(1))

    existing_versions = [
        match.group(1) for f in files if (match := re.search(r"_v([^_]+)\.", f))
    ]

    if not stable_version and len(files) == 0:
        return "0"
    elif not stable_version and existing_versions[-1] == "0":
        return "0"
    elif not stable_version and existing_versions[-1] != "0":
        print(
            f"En stabil versjon (versjon {files[-1]}) finnes allerede. Versjonsnummer 0 overskrives fordi stable_version er satt til False av bruker."
        )
        return "0"
    elif stable_version and len(files) == 0:
        return "1"
    elif stable_version and existing_versions[-1] == "0":
        return "1"
    elif stable_version and existing_versions[-1] != "0":
        overwrite = input(
            f"Vil du overskrive eksisterende versjon (_v{existing_versions[-1]})? Bekreft med 'y'."
        )
        if overwrite.lower() == "y":
            version = existing_versions[-1]
            version = version.split(".")[0]
            return str(version)
        else:
            make_new_version = input(
                "Vil du lage en ny versjon (altså øke versjonsnummeret med en)? Bekreft med 'y'."
            )
            if make_new_version.lower() == "y":
                version = existing_versions[-1]
                version = int(version.split(".")[0]) + 1
                return str(version)
            else:
                return None
    else:
        raise ValueError("Noe gikk galt da rett versjonsnummer skulle settes.")


def _verify_base_filename(name: str) -> str:
    """Verifies the base of the file name."""
    # Ensure lower case
    if any(letter.isupper() for letter in name):
        old_name = name
        name = name.lower()
        print(
            f"Basefilnavnet ble endret fra {old_name} til {name}. Store bokstaver er ikke tillatt."
        )
    # Raises error if starts with numbers
    if name[0].isdigit():
        raise ValueError(
            f"Basefilnavnet kan ikke starte med et siffer. Dette må endres. \nNåværende basefilnavn: {name}."
        )
    if "parquet" in name:
        raise ValueError(
            "Filformatet 'parquet' skal ikke være en del av basefilnavnet, fjern det."
        )

    if "csv" in name:
        raise ValueError(
            "Filformatet 'csv' skal ikke være en del av basefilnavnet, fjern det.."
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
                f"Ugyldige tegn finnes i basefilnavnet, fjern det. Ugyldige tegn er: {not_valid_signs}"
            )
    return name


def _verify_datatilstand(datatilstand: str) -> str:
    """Veirfy the name of the datatilstand."""
    datatilstand = datatilstand.lower()
    if datatilstand not in [
        "inndata",
        "klargjorte-data",
        "statistikk",
        "utdata",
        "logg",
        "konfigurasjon",
    ]:
        datatilstand = input(
            "Datatilstanden må være enten inndata, klargjorte-data, statistikk eller utdata (eller logg, konfigurasjon)."
        )
        datatilstand = _verify_datatilstand(datatilstand)
        return datatilstand
    else:
        return datatilstand


def _save_df(
    df: pd.DataFrame,
    file_path: str,
    filetype: str,
    fs: dapla.gcs.GCSFileSystem | None,
    seperator: str,
    encoding: str,
) -> None:
    """Do the actual saving, either as csv or parquet."""
    # Save as parquet
    if filetype == "parquet":

        if fs:
            with fs.open(file_path, "wb") as f:
                df.to_parquet(f, index=False)
                f.close()
        else:
            df.to_parquet(file_path, index=False)
    # Save as csv
    elif filetype == "csv":
        if fs:
            with fs.open(file_path, "wb") as f:
                df.to_csv(f, sep=seperator, index=False, encoding=encoding)
                f.close()
        else:
            df.to_csv(file_path, sep=seperator, index=False, encoding=encoding)
    # Save as jsonl
    elif filetype == "jsonl":
        df.to_json(orient="records", lines=True)

    # Save as json
    elif filetype == "json":
        df.to_json(orient="records", lines=False)

    # Uknown filetype sent as argument
    else:
        raise ValueError(
            f"Filtypen {filetype} er ikke støttet. Ingenting er blitt lagret for denne filstien: {file_path}."
        )


def write_ssb_file(
    df: pd.DataFrame,
    periode: tuple[int, ...],
    frequency: str,
    bucket: str,
    kortnavn: str,
    file_name: str,
    datatilstand: str = "",
    undermappe: str | None = None,
    stable_version: bool = True,
    filetype: str = "parquet",
    fs: dapla.gcs.GCSFileSystem | None = None,
    seperator: str = ";",
    encoding: str = "latin1",
) -> None:
    """Function to write and save a dataframe at SSB-format.

    Args:
        df: The dataframe to save.
        periode: Up to six arguments with int, to create timestamp for. E.g. (2022,2,2023,4) is p_2022-02_p2023-04 when frequency = 'M'.
        frequency: monthly (M), daily(D), quarter (Q), terital (T), weekly (W).
        bucket: GCP bucket passed with a FileClient object or path in prodsonen.
        kortnavn: Name of statistic or data product, temp or oppdrag is also valid.
        file_name: Name for file.
        datatilstand: Datatilstand following SSB standards, except when temp and oppdrag is the kortnavn.
        undermappe: Optional folder under 'datatilstand'.
        stable_version: Bool for whether you should have checks in place in case of overwrite.
        filetype: the filetype to save as. Default: 'parquet'.
        fs: the filesystem, pass with gsc Filesystem if Dapla. Default: None.
        seperator: the seperator to use it filetype is csv. Default: ';'.
        encoding: Encoding for file, base is latin1.

    Raises:
        ValueError: if df has no rows.
    """
    # Check content in df
    if not len(df) > 0:
        raise ValueError("Dataframen har ingen rader. Fiks dette og prøv igjen.")
    # Veirfy name of datatilstand and base filename
    file_name = _verify_base_filename(file_name)
    # Verify 'datatilstand' if not the kortnavn is temp or oppdrag
    if kortnavn.lower() not in ["temp", "oppdrag"]:
        datatilstand = _verify_datatilstand(datatilstand)
    # Get the filepath, only without version number and filetype
    file_path = _structure_ssb_filepath(
        periode=periode,
        frequency=frequency,
        bucket=bucket,
        kortnavn=kortnavn,
        datatilstand=datatilstand,
        file_name=file_name,
        undermappe=undermappe,
        fs=fs,
    )
    # Get list with the filenames, if several, ordered by the highest version number at last.
    files = _get_files(file_path, filetype, fs=fs)
    # Find version number/decide whether to overwrite or make new version.
    version_number = _find_version_number(files, stable_version)

    if version_number:
        if file_path.endswith("_"):
            file_path = file_path[:-1]
        file_path = f"{file_path}_v{version_number}.{filetype}"

        _save_df(df, file_path, filetype, fs, seperator, encoding)


def read_ssb_file(
    periode: tuple[int, ...],
    frequency: str,
    bucket: str,
    kortnavn: str,
    file_name: str,
    datatilstand: str = "",
    undermappe: str | None = None,
    filetype: str = "parquet",
    version_number: int | None = None,
    fs: dapla.gcs.GCSFileSystem | None = None,
    seperator: str = ";",
    encoding: str = "latin1",
) -> pd.DataFrame | None:
    """Function to read a saved file, stored at SSB-format.

    Get the last version saved in the datatilstand specified (klargjorte-data, statistikk, utdata).
    at the correct bucket path and with the speficed name.
    If it is a year table, the filename is automatically adjusted.

    Args:
        periode: Up to six arguments with int, to create timestamp for. E.g. (2022,2,2023,4) is p_2022-02_p2023-04 when frequency = 'M'.
        frequency: monthly (M), daily(D), quarter (Q), terital (T), weekly (W).
        bucket: GCP bucket passed with a FileClient object or path in prodsonen.
        kortnavn: Name of statistic or data product, temp and oppdrag is also valid.
        file_name: Name for file.
        datatilstand: Datatilstand following SSB standards, except when temp and oppdrag is the kortnavn.
        undermappe: Optional folder under 'datatilstand'.
        version_number: possibility to get another version, than the newest (i.e. highest version number). Default: np.nan.
        filetype: the filetype to save as. Default: 'parquet'.
        fs: the filesystem, pass with gsc Filesystem if Dapla. Default: None.
        seperator: the seperator to use it filetype is csv. Default: ';'.
        encoding: Encoding for file, base is latin1.

    Raises:
        FileNotFoundError: If no files matching the file path and filetype are found.

    Returns:
        pd.DataFrame: file as a data frame.
    """
    # Get the filepath, only without version number and filetype.
    file_path = _structure_ssb_filepath(
        periode=periode,
        frequency=frequency,
        bucket=bucket,
        kortnavn=kortnavn,
        datatilstand=datatilstand,
        file_name=file_name,
        undermappe=undermappe,
        version_number=version_number,
        filetype=filetype,
        fs=fs,
    )

    if not version_number:
        # If version number not specified then list out versions.
        files = _get_files(file_path, filetype, fs=fs)
        # If list is empty, no matching files of any version were found.
        if not files:
            raise FileNotFoundError(
                f"Fant ingen {filetype}-filer som matcher filstien '{file_path}'."
            )
        # Otherwise, use the newest version of file.
        file_path = files[-1]

    # Different functions used for reading depending on the filetype.
    if filetype == "csv":
        if fs:
            # Samme som tidligere kan brukes til å lese alle filformater.
            with fs.open(file_path, "r") as f:
                df = pd.read_csv(f, sep=seperator, encoding=encoding)
                f.close()
        else:
            df = pd.read_csv(file_path, sep=seperator, encoding=encoding)
    elif filetype == "parquet":
        df = pd.read_parquet(file_path, filesystem=fs)
    elif filetype == "jsonl":
        df = pd.read_json(file_path, lines=True)
    elif filetype == "json":
        df = pd.read_json(file_path, lines=False)
    # Returns pandas df.
    return df
