from ssb_konjunk.saving import get_time_period_standard

"""Test function get_time_period_standard"""


def test_get_time_period_standard() -> None:
    # TEst
    assert (
        get_time_period_standard("filnavn", 2023, 1, 22) == "filnavn_p2023-01-22_v"
    ), "get_time_period_standard('filnavn', 2023, 1,22)"
