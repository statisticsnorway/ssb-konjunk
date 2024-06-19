"""A collection of functions to make fame files in the cloud.

The template and this example uses Google style docstrings as described at:
https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html

"""

# Importing external packages
import pandas as pd
from dapla import FileClient

# Getting filesystem
fs = FileClient.get_gcs_file_system()


def change_date_format_fame(series: pd.Series) -> pd.Series:
    """Function for turning ISO-8601 to fame time format.

    Args:
        series: A pandas series containing string for dates in format ISO-8601(YYYY-mm-dd).

    Returns:
        pd.Series: A pandas series with dates in fame format(YYYY:M:D)
    """
    series = pd.to_datetime(series)

    # Format the datetime column as "YYYY:M:D"
    series = series.dt.strftime("%Y:%-m:%-d")

    return series


def write_out_fame_format_txt(
    names: pd.Series,
    dates: pd.Series,
    values: pd.Series,
    gcp_path: str,
) -> None:
    """Function to write out txt file in fame format.

    Args:
        names: Pandas series containing name or type for value.
        dates: Pandas series containing date for values.
        values: Pandas series containing values.
        gcp_path: String to google cloud.
    """
    with fs.open(gcp_path, "w") as f:
        # Write data rows
        for name, date, value in zip(names, dates, values, strict=False):
            # Apply format specification
            formatted_value = f"{value:20.2f}"
            # Write data row
            f.write(f"{name:<35}{date:<50}{formatted_value}\n")
