import pytest

from ssb_konjunk.saving import get_time_period_standard

"""Test function get_time_period_standard"""

def test_get_time_period_standard() -> None:
    # TEst
    base_name = 'filnavn'
    start_year = 2023
    start_month = 1
    start_day = 1
    end_year = 2024
    end_month = 2
    end_day = 14
    
    assert get_time_period_standard('filnavn', 2023, 1,22) == 'filnavn_p2023-01-22_v', f"get_time_period_standard('filnavn', 2023, 1,22)"