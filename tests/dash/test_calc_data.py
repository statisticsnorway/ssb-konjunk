import numpy as np
import pandas as pd
import polars as pl
import pytest

from ssb_konjunk.dash.calculations.calc_data import DataManager
from ssb_konjunk.dash.calculations.calc_data import get_data_manager
from ssb_konjunk.dash.calculations.period_utils import Period


def test_data_manager_init(test_df):

    manager = DataManager(test_df)

    assert manager.data is not None
    assert manager.season_adjusted_series is not None
    assert manager.raw_series is not None
    assert manager.calendar_source is not None


def test_pad_single():
    assert DataManager.pad_single("H") == "H"
    assert DataManager.pad_single("64") == "  64"
    assert DataManager.pad_single("49.1") == "      49.1"


def test_calc_indirect(test_df):
    assert DataManager.calc_indirect(test_df, "jus") == pytest.approx(
        2644.08719, rel=1e-4
    )


def test_to_percent(test_df):
    np.random.seed(0)
    data = DataManager(test_df)
    weight = data.weight_source.data.filter(pl.col("nar") == "K")["verdi"]
    chg_rate = pl.Series("chg_rate", np.random.uniform(-2, 2, 13))

    percent_change = data.to_percent(weight, chg_rate)
    assert percent_change[0] == pytest.approx(0.024927, rel=1e-4)


def test_header_1(test_df):
    data = DataManager(test_df)
    assert data.header_1 == ["", "Vekt %", "Indeks", "% Endring", "% Endring vektet"]


def test_get_nacer(test_df):
    data = DataManager(test_df)
    assert data.get_nacer() == ["H", "K", "49.1", "49.2", "64"]


def test_add_klass_codes(test_df):
    data = DataManager(test_df)
    nace_data = data.data["nar"].to_frame()
    nace_data_with_codes = data.add_klass_codes(nace_data, "nar")
    assert nace_data_with_codes["nar"].iloc[0] == "H - Transport og lagring"
    assert nace_data_with_codes["nar"].iloc[-1] == "64 - Finansieringsvirksomhet"


def test_sort_aggregates():
    test_series = pd.Series(["42.1", "40.2", "40", "F", "40.1"])
    sorted_series = DataManager.sort_aggregates(test_series).tolist()
    assert sorted_series == [5, 3, 1, 0, 2]


def test_normalize_weight():
    test_table_data = pl.DataFrame(
        {
            "nar": ["K", "H"],
            "weight": [24.236229, 18.370044],
            "season": [87.518523, 97.889373],
            "season1": [-16.4, -3.3],
            "weighted": [-4.46, -1.24],
        }
    )

    test_table_data, test_weighted = DataManager._normalize_weight(test_table_data)

    assert test_table_data["weight"].cast(pl.Float64).sum() == 100
    assert test_weighted["weighted"].to_list() == test_table_data["weighted"].to_list()

    test_table_data_fail = pl.DataFrame(
        {
            "nar": ["K", "H"],
            "weight": [24.236229, 18.370044],
            "season": [87.518523, 97.889373],
            "weighted": [-4.46, -1.24],
        }
    )

    with pytest.raises(ValueError):
        DataManager._normalize_weight(test_table_data_fail)


def test_get_all_periods(test_df):
    data = DataManager(test_df)
    period_data = DataManager.get_all_periods(data)
    expected = [f"{2024}-{month:02d}" for month in range(1, 13)]
    assert period_data == expected


def test_format_aggregates(test_df):
    data = DataManager(test_df)
    nace_data = data.get_nacer()
    nace_data_formated = data.format_aggregates(pd.Series(nace_data)).tolist()
    expected = ["H", "K", "      49.1", "      49.2", "  64"]
    assert nace_data_formated == expected


def test_prep_skip(test_df):
    data = DataManager(test_df)
    prep_skip = data._prep_skip("2023-06")
    assert prep_skip == 18


def test_create_periods_and_latest(test_df):
    data = DataManager(test_df)
    periods, latest = data.create_periods_and_latest(None, 2)
    expected_periods = [Period("2024-11"), Period("2024-12")]
    expected_latest = Period("2024-12")
    assert periods == expected_periods
    assert latest == expected_latest

    periods_2, latest_2 = data.create_periods_and_latest("2024-03", 2)
    expected_periods_2 = [Period("2024-02"), Period("2024-03")]
    expected_latest_2 = Period("2024-03")
    assert periods_2 == expected_periods_2
    assert latest_2 == expected_latest_2


def test_create_period_range(test_df):
    data = DataManager(test_df)
    period_range = data.create_period_range(None, 2)
    period_range_skip = data.create_period_range(None, 2, 3)
    assert period_range == [Period("2024-12"), Period("2024-11")]
    assert period_range_skip == [Period("2024-12"), Period("2024-09")]


def test_get_sesonal_adjusted_3_mth_change(test_df):
    data = DataManager(test_df)
    seasonal_3_mnt_change_1 = data.get_sesonal_adjusted_3_mth_change(
        nace_filter=["H", "K"]
    )
    seasonal_3_mnt_change_2 = data.get_sesonal_adjusted_3_mth_change(
        nace_filter=["H", "49.1", "49.2"]
    )
    seasonal_3_mnt_change_3 = data.get_sesonal_adjusted_3_mth_change(max_nace_level=1)

    expected_header_1 = ["", "Vekt %", "Indeks", "% Endring", "% Endring vektet"]
    expected_header_2 = [
        "",
        "Oct 2024 - Dec 2024",
        "Oct 2024 - Dec 2024",
        "Jul 2024 - Sep 2024 / Oct 2024 - Dec 2024",
        "Jul 2024 - Sep 2024 / Oct 2024 - Dec 2024",
    ]

    expected_res_data_1 = pd.DataFrame(
        {
            "nar": [
                "  H - Transport og lagring",
                "  K - Finansierings- og forsikringsvirks",
            ],
            "weight": [43.1, 56.9],
            "season": [97.9, 87.5],
            "season1": [-3.3, -16.4],
            "weighted": [-1.4, -9.3],
        }
    )

    expected_res_data_2 = pd.DataFrame(
        {
            "nar": [
                "  H - Transport og lagring",
                "        49.1 - Passasjertransport med je",
                "        49.2 - Godstransport med jernban",
            ],
            "weight": [20.7, 28.5, 50.8],
            "season": [97.9, 98.6, 102.4],
            "season1": [-3.3, -0.9, 1.0],
            "weighted": [-0.7, -0.3, 0.5],
        }
    )
    expected_res_data_3 = pd.DataFrame(
        {
            "nar": [
                "  H - Transport og lagring",
                "  K - Finansierings- og forsikringsvirks",
                "    64 - Finansieringsvirksomhet",
            ],
            "weight": [18.4, 24.2, 27.5],
            "season": [97.9, 87.5, 99.5],
            "season1": [-3.3, -16.4, -6.2],
            "weighted": [-1.2, -4.5, -1.4],
        }
    )

    assert seasonal_3_mnt_change_1.header_1 == expected_header_1
    assert seasonal_3_mnt_change_1.header_2 == expected_header_2
    pd.testing.assert_frame_equal(seasonal_3_mnt_change_1.res_data, expected_res_data_1)
    pd.testing.assert_frame_equal(seasonal_3_mnt_change_2.res_data, expected_res_data_2)
    pd.testing.assert_frame_equal(seasonal_3_mnt_change_3.res_data, expected_res_data_3)


def test_get_sesonal_adjusted_mth_change(test_df):
    data = DataManager(test_df)
    seasonal_mnt_change_1 = data.get_sesonal_adjusted_mth_change(nace_filter=["H", "K"])
    seasonal_mnt_change_2 = data.get_sesonal_adjusted_mth_change(
        nace_filter=["H", "49.1", "49.2"]
    )
    seasonal_mnt_change_3 = data.get_sesonal_adjusted_mth_change(max_nace_level=1)

    expected_header_1 = ["", "Vekt %", "Indeks", "% Endring", "% Endring vektet"]
    expected_header_2 = [
        "",
        "Nov 2024 - Dec 2024",
        "Nov 2024 - Dec 2024",
        "Nov 2024 - Dec 2024",
        "Nov 2024 - Dec 2024",
    ]

    expected_res_data_1 = pd.DataFrame(
        {
            "nar": [
                "  H - Transport og lagring",
                "  K - Finansierings- og forsikringsvirks",
            ],
            "weight": [49.5, 50.5],
            "season": [92.6, 81.0],
            "season1": [-13.2, -12.4],
            "weighted": [-6.5, -6.3],
        }
    )

    expected_res_data_2 = pd.DataFrame(
        {
            "nar": [
                "  H - Transport og lagring",
                "        49.1 - Passasjertransport med je",
                "        49.2 - Godstransport med jernban",
            ],
            "weight": [22.9, 40.4, 36.6],
            "season": [92.6, 107.1, 86.5],
            "season1": [-13.2, 3.5, -20.3],
            "weighted": [-3.0, 1.4, -7.4],
        }
    )
    expected_res_data_3 = pd.DataFrame(
        {
            "nar": [
                "  H - Transport og lagring",
                "  K - Finansierings- og forsikringsvirks",
                "    64 - Finansieringsvirksomhet",
            ],
            "weight": [27.5, 28.1, 21.7],
            "season": [92.6, 81.0, 102.1],
            "season1": [-13.2, -12.4, -7.7],
            "weighted": [-2.0, -2.1, -1.0],
        }
    )

    assert seasonal_mnt_change_1.header_1 == expected_header_1
    assert seasonal_mnt_change_1.header_2 == expected_header_2
    pd.testing.assert_frame_equal(seasonal_mnt_change_1.res_data, expected_res_data_1)
    pd.testing.assert_frame_equal(seasonal_mnt_change_2.res_data, expected_res_data_2)
    pd.testing.assert_frame_equal(seasonal_mnt_change_3.res_data, expected_res_data_3)


def test_get_sesonal_adjusted_12_mth_change(test_df):
    data = DataManager(test_df)
    seasonal_12_mnt_change_1 = data.get_sesonal_adjusted_12_mth_change(
        nace_filter=["H", "K"]
    )
    seasonal_12_mnt_change_2 = data.get_sesonal_adjusted_12_mth_change(
        nace_filter=["H", "49.1", "49.2"]
    )
    seasonal_12_mnt_change_3 = data.get_sesonal_adjusted_12_mth_change(max_nace_level=1)

    expected_header_1 = ["", "Vekt %", "Indeks", "% Endring", "% Endring vektet"]
    expected_header_2 = [
        "",
        "Nov 2024 - Dec 2024",
        "Nov 2024 - Dec 2024",
        "Dec 2023 - Dec 2024",
        "Dec 2023 - Dec 2024",
    ]

    expected_res_data_1 = pd.DataFrame(
        {
            "nar": [
                "  H - Transport og lagring",
                "  K - Finansierings- og forsikringsvirks",
            ],
            "weight": [49.5, 50.5],
            "calendar": [94.5, 82.7],
            "calendar1": [-12.9, -19.5],
            "weighted": [-6.4, -9.9],
        }
    )

    expected_res_data_2 = pd.DataFrame(
        {
            "nar": [
                "  H - Transport og lagring",
                "        49.1 - Passasjertransport med je",
                "        49.2 - Godstransport med jernban",
            ],
            "weight": [22.9, 40.4, 36.6],
            "calendar": [94.5, 90.8, 104.6],
            "calendar1": [-12.9, 8.0, 1.5],
            "weighted": [-3.0, 3.2, 0.6],
        }
    )

    expected_res_data_3 = pd.DataFrame(
        {
            "nar": [
                "  H - Transport og lagring",
                "  K - Finansierings- og forsikringsvirks",
                "    64 - Finansieringsvirksomhet",
            ],
            "weight": [27.5, 28.1, 21.7],
            "calendar": [94.5, 82.7, 103.4],
            "calendar1": [-12.9, -19.5, -10.8],
            "weighted": [-4.1, -2.5, -2.0],
        }
    )

    assert seasonal_12_mnt_change_1.header_1 == expected_header_1
    assert seasonal_12_mnt_change_1.header_2 == expected_header_2
    pd.testing.assert_frame_equal(
        seasonal_12_mnt_change_1.res_data, expected_res_data_1
    )
    pd.testing.assert_frame_equal(
        seasonal_12_mnt_change_2.res_data, expected_res_data_2
    )
    pd.testing.assert_frame_equal(
        seasonal_12_mnt_change_3.res_data, expected_res_data_3
    )


def test_data_manager(mocker, test_df):
    mocker.patch("pandas.read_parquet", return_value=test_df)
    class_object = get_data_manager("ignored.parquet")
    assert isinstance(class_object, DataManager)
