import pytest
import re
import pandas as pd
from dapla import FileClient

from ssb_konjunk.saving import get_time_period_standard
from ssb_konjunk.saving import get_versions
from ssb_konjunk.saving import get_saved_file



def test_get_versions():
    """
    ONLY FOR DAPLA, NOT PRODSONE. 
    Add underscore in front of function name for testing in prodsone.
    """
    fs = FileClient.get_gcs_file_system()
    filename = "test-data_p2022-03-01_p2023-12-31_v"
    folder_path = "gs://ssb-reiseliv-korttid-data-produkt-prod/overnatting/inndata/"
    
    assert get_versions(folder_path, filename, re.compile(rf'{re.escape(filename)}(\d+)\.parquet'), filesystem = fs) == ['0', '1', '2'], get_versions(folder_path, filename, re.compile(rf'{re.escape(filename)}(\d+)\.parquet'), filesystem = fs)

    
def _test_get_versions():
    """
    ONLY FOR PRODSONE, NOT DAPLA. 
    Remove underscore in front of function name for testing in prodsone.
    """
    filename = 'chv_p2024-01_v'
    folder_path = '/ssb/stamme04/reiseliv/NV/wk48/temp/utvikling/konfidensialitet/'
    
    assert get_versions(folder_path, filename, re.compile(rf'{re.escape(filename)}(\d+)\.csv')) == ['0', '1'], get_versions(folder_path, filename, re.compile(rf'{re.escape(filename)}(\d+)\.csv'))
    
    filename = 'chv_p2024-88_v'
    assert get_versions(folder_path, filename, re.compile(rf'{re.escape(filename)}(\d+)\.csv')) == [], get_versions(folder_path, filename, re.compile(rf'{re.escape(filename)}(\d+)\.csv'))
    
def _test_get_saved_file() -> None:
    """
    ONLY FOR PRODSONE, NOT DAPLA.
    Remove underscore in front of function name for testing in prodsone.
    """
    df = pd.read_csv('/ssb/stamme04/reiseliv/NV/wk48/temp/utvikling/konfidensialitet/chv_p2024-01_v1.csv', sep=';')

    assert get_saved_file('chv', 'temp/utvikling/konfidensialitet', '/ssb/stamme04/reiseliv/NV/wk48/', 2024, 1,
                         filetype='csv').equals(df) == True, "Not equal"
    