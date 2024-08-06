import pytest
import re
import pandas as pd

from ssb_konjunk.saving import get_time_period_standard
from ssb_konjunk.saving import get_versions
from ssb_konjunk.saving import get_saved_file


"""Test function get_time_period_standard"""


def test_get_time_period_standard() -> None:
    # Test year and month
    assert (
        get_time_period_standard("filnavn", 2023, 1) == "filnavn_p2023-01_v"
    ), get_time_period_standard('filnavn', 2023, 1)
    
    # Test year
    assert (
        get_time_period_standard("filnavn", 2023) == "filnavn_p2023_v"
    ), get_time_period_standard('filnavn', 2023)
    
    # Test start and end year
    assert (
        get_time_period_standard("filnavn", 2023, end_year=2024) == "filnavn_p2023_p2024_v"
    ), get_time_period_standard('filnavn', 2023, end_year=2024)
    
    # Test invalid period
    assert (
        get_time_period_standard("filnavn", 2023, 2, end_year=2024) == "filnavn"
    ), get_time_period_standard('filnavn', 2023, 2, end_year=2024)
    
    # Test year month period
    assert (
        get_time_period_standard("filnavn", 2023, 2, end_year=2024, end_month=5) == "filnavn_p2023-02_p2024-05_v"
    ), get_time_period_standard('filnavn', 2023, 2, end_year=2024, end_month=5)
    
    # Test year month period
    assert (
        get_time_period_standard("filnavn", 2023, 2, end_year=2024, end_month=5) == "filnavn_p2023-02_p2024-05_v"
    ), get_time_period_standard('filnavn', 2023, 2, end_year=2024, end_month=5)
    
    # Test year, month, day
    assert (
        get_time_period_standard("filnavn", 2023, 1, 22) == "filnavn_p2023-01-22_v"
    ), get_time_period_standard('filnavn', 2023, 1,22)
    
    # Test year, month, day, period
    assert (
        get_time_period_standard("filnavn", 2023, 1, 22, end_year = 2023, end_month = 3, end_day = 2) == "filnavn_p2023-01-22_p2023-03-02_v"
    ), get_time_period_standard('filnavn', 2023, 1,22, end_year = 2023, end_month = 3, end_day = 2)
    
    # Test invalid period
    assert (
        get_time_period_standard("filnavn", 2023, 1, start_quarter = 1) == "filnavn"
    ), get_time_period_standard('filnavn', 2023, 1, start_quarter = 1)

    # Test quarter
    assert (
        get_time_period_standard("filnavn", 2023, start_quarter = 1) == "filnavn_p2023Q1_v"
    ), get_time_period_standard('filnavn', 2023, start_quarter = 1)
    
    # Test quarter period
    assert (
        get_time_period_standard("filnavn", 2023, start_quarter = 1, end_year=2023, end_quarter=2) == "filnavn_p2023Q1_p2023Q2_v"
    ), get_time_period_standard('filnavn', 2023, start_quarter = 1, end_year=2023, end_quarter=2)
    
    # Test week
    assert (
        get_time_period_standard("filnavn", 2023, start_week = 1) == "filnavn_p2023W1_v"
    ), get_time_period_standard('filnavn', 2023, start_week = 1)
    
    # Test week period
    assert (
        get_time_period_standard("filnavn", 2023, start_week = 1, end_year=2023, end_week=32) == "filnavn_p2023W1_p2023W32_v"
    ), get_time_period_standard('filnavn', 2023, start_week = 1, end_year=2023, end_week=32)
    
    # Test bimester
    assert (
        get_time_period_standard("filnavn", 2023, start_bimester = 1) == "filnavn_p2023B1_v"
    ), get_time_period_standard('filnavn', 2023, start_bimester = 1)
    
    # Test bimester period
    assert (
        get_time_period_standard("filnavn", 2023, start_bimester = 1, end_year=2023, end_bimester=5) == "filnavn_p2023B1_p2023B5_v"
    ), get_time_period_standard('filnavn', 2023, start_bimester = 1, end_year=2023, end_bimester=5)
    
    # Test tertial
    assert (
        get_time_period_standard("filnavn", 2022, start_tertial = 1) == "filnavn_p2022T1_v"
    ), get_time_period_standard('filnavn', 2022, start_tertial = 1)
    
    # Test tertial period
    assert (
        get_time_period_standard("filnavn", 2022, start_tertial = 1, end_year=2023, end_tertial=2) == "filnavn_p2022T1_p2023T2_v"
    ), get_time_period_standard('filnavn', 2022, start_tertial = 1, end_year=2023, end_tertial=2)
    
    # Test halfyear
    assert (
        get_time_period_standard("filnavn", 2022, start_halfyear = 1) == "filnavn_p2022H1_v"
    ), get_time_period_standard('filnavn', 2022, start_halfyear = 1)
    
    # Test halfyear period
    assert (
        get_time_period_standard("filnavn", 2022, start_halfyear = 1, end_year=2023, end_halfyear=2) == "filnavn_p2022H1_p2023H2_v"
    ), get_time_period_standard('filnavn', 2022, start_halfyear = 1, end_year=2023, end_halfyear=2)
    
def test_get_versions():
    filename = 'chv_p2024-01_v'
    folder_path = '/ssb/stamme04/reiseliv/NV/wk48/temp/utvikling/konfidensialitet/'
    
    assert get_versions(folder_path, filename, re.compile(rf'{re.escape(filename)}(\d+)\.csv')) == ['0', '1'], get_versions(folder_path, filename, re.compile(rf'{re.escape(filename)}(\d+)\.csv'))
    
    filename = 'chv_p2024-88_v'
    assert get_versions(folder_path, filename, re.compile(rf'{re.escape(filename)}(\d+)\.csv')) == [], get_versions(folder_path, filename, re.compile(rf'{re.escape(filename)}(\d+)\.csv'))
    
def test_get_saved_file() -> None:
    df = pd.read_csv('/ssb/stamme04/reiseliv/NV/wk48/temp/utvikling/konfidensialitet/chv_p2024-01_v1.csv', sep=';')

    assert get_saved_file('chv', 'temp/utvikling/konfidensialitet', '/ssb/stamme04/reiseliv/NV/wk48/', 2024, 1,
                         filetype='csv').equals(df) == True, "Not equal"
    