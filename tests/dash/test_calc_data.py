import numpy as np
import pandas as pd
import polars as pl
import pytest

from ssb_konjunk.dash.calculations.calc_data import DataManager
from ssb_konjunk.dash.calculations.period_utils import Period


def test_data_manager_init(data):

    assert data.data is not None
    assert data.period_col is not None
    assert data.group_col is not None
    assert data.weight_source is not None
    assert data.value_columns is not None
    assert data.categorical_columns is not None
    assert data.id_columns is not None
    assert data.available_values is not None
    


def test_pad_single():
    assert DataManager.pad_single("H") == "H"
    assert DataManager.pad_single("64") == "  64"
    assert DataManager.pad_single("49.1") == "      49.1"


def test_calc_indirect(data, test_df_data):
    assert data.calc_indirect(test_df_data, data.value_columns[0]) == pytest.approx(30221.80466, rel=1e-4)


def test_to_percent(data):
    np.random.seed(0)
    print(data.weight_source.data)
    weight = data.weight_source.data.filter(pl.col("nar") == "J")["vekt"]
    chg_rate = pl.Series("chg_rate", np.random.uniform(-2, 2, 38))

    percent_change = data.to_percent(weight, chg_rate)
    assert percent_change[0] == pytest.approx(0.083084, rel=1e-4)

def get_group_col(data):
    assert data.get_nacer() == [
        "H",
        "49.1",
        "49.2",
        "J",
        "59",
    ]


def test_add_klass_codes(data):
    nace_data = data.data["nar"].to_frame()
    nace_data_with_codes = data.add_klass_codes(nace_data, "nar")
    assert "H - Transport og lagring" in nace_data_with_codes["nar"].values
    assert "49.2 - Godstransport med jernbane" in nace_data_with_codes["nar"].values


def test_build_sort_order(data):
    sort_order = data._build_sort_order()
    assert set(sort_order) == set(data.class_codes["code"])


def test_sort_aggregates(data):
    test_series = pd.Series(["42.1", "41.2", "41", "F", "41.1"])

    sorted_codes = test_series.iloc[
        data.sort_aggregates(test_series).argsort()
    ].tolist()

    assert sorted_codes == ["F", "41", "41.1", "41.2", "42.1"]


def test_normalize_weight(data):
    test_table_data = pl.DataFrame(
        {
            "nar": ["K", "H", "HTNXK"],
            "weight": [24.23, 18.37, 42.60],
            "season": [87.518523, 97.889373, 103.823473],
            "season1": [-16.4, -3.3, 14.5],
            "weighted": [-4.46, -1.24, 23.5],
        }
    )

    test_table_data, test_weighted = data._normalize_weight(
        test_table_data, includes_parent_aggregate=True
    )

    assert test_table_data["weight"].cast(pl.Float64).sum() == 200
    assert test_weighted["weighted"].to_list() == test_table_data["weighted"].to_list()

    test_table_data_2 = pl.DataFrame(
        {
            "nar": ["K", "H"],
            "weight": [24.236229, 18.370044],
            "season": [87.518523, 97.889373],
            "season1": [-16.4, -3.3],
            "weighted": [-4.46, -1.24],
        }
    )

    test_table_data_2, _ = data._normalize_weight(
        test_table_data_2, includes_parent_aggregate=False
    )

    assert test_table_data_2["weight"].cast(pl.Float64).sum() == 100

    test_table_data_fail = pl.DataFrame(
        {
            "nar": ["K", "H"],
            "weight": [24.236229, 18.370044],
            "season": [87.518523, 97.889373],
            "weighted": [-4.46, -1.24],
        }
    )

    with pytest.raises(ValueError):
        data._normalize_weight(test_table_data_fail, includes_parent_aggregate=True)


def test_get_all_periods(data):
    period_data = DataManager.get_all_periods(data)
    expected = [f"{2024}-{month:02d}" for month in range(1, 13)]
    assert period_data == expected


def test_format_aggregates(data):
    nace_data = data.get_group_col()
    nace_data_formated = data.format_aggregates(pd.Series(nace_data)).tolist()
    expected = ["H", "      49.1", "      49.2", "J", "  59"]
    assert nace_data_formated == expected


def test_prep_skip(data):
    prep_skip = data._prep_skip("2023-06")
    assert prep_skip == 18


def test_create_periods_and_latest(data):
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


def test_create_period_range(data):
    period_range = data.create_period_range(None, 2)
    period_range_skip = data.create_period_range(None, 2, 3)
    assert period_range == [Period("2024-12"), Period("2024-11")]
    assert period_range_skip == [Period("2024-12"), Period("2024-09")]


def test_get_sesonal_adjusted_3_mth_change(data, test_df_datasource):
    seasonal_3_mnt_change_1 = data.get_3_mth_change(
        test_df_datasource,
        group_filter=["H", "J"],
        includes_parent_aggregate=False,
    )
    seasonal_3_mnt_change_2 = data.get_3_mth_change(
        test_df_datasource,
        group_filter=["H", "49.1", "49.2"],
        includes_parent_aggregate=False,
    )
    seasonal_3_mnt_change_3 = data.get_3_mth_change(
        test_df_datasource,
        max_nace_level=1,
    )

    expected_header_1 = ["", "Vekt %", "Indeks", "% Endring", "% Endring vektet"]
    expected_header_2 = [
        "",
        "Oct 2024 - Dec 2024",
        "Oct 2024 - Dec 2024",
        "Jul 2024 - Sep 2024 / Oct 2024 - Dec 2024",
        "Jul 2024 - Sep 2024 / Oct 2024 - Dec 2024",
    ]
    result = seasonal_3_mnt_change_3.res_data.to_dict(orient="list")
    print(result)
    expected_res_data_1 = pd.DataFrame(
        {
            "nar": [
                "  H - Transport og lagring",
                "  J - Informasjon og kommunikasjon",
            ],
           'weight': [34.6, 36.9],
            'value': [115.7, 97.1],
            'value1': [16.5, -8.5],
            'weighted_value': [4.0, -2.8],
        }
    )

    expected_res_data_2 = pd.DataFrame(
        {
            "nar": [
                "  H - Transport og lagring",
                "        49.1 - Passasjertransport med je",
                "        49.2 - Godstransport med jernban",
            ],
            'weight': [34.6, 28.4, 34.6],
            'value': [115.7, 96.9, 109.5],
            'value1': [16.5, 9.6, 19.3],
            'weighted_value': [4.0, 4.1, 6.1],
        }
    )
    expected_res_data_3 = pd.DataFrame(
        {
            "nar": [
                "  H - Transport og lagring",
                "  J - Informasjon og kommunikasjon",
            ],
            'weight': [34.6, 36.9],
            'value': [115.7, 97.1],
            'value1': [16.5, -8.5],
            'weighted_value': [4.0, -2.8],
        }
    )

    assert seasonal_3_mnt_change_1.header_1 == expected_header_1
    assert seasonal_3_mnt_change_1.header_2 == expected_header_2
    pd.testing.assert_frame_equal(seasonal_3_mnt_change_1.res_data, expected_res_data_1)
    pd.testing.assert_frame_equal(seasonal_3_mnt_change_2.res_data, expected_res_data_2)
    pd.testing.assert_frame_equal(seasonal_3_mnt_change_3.res_data, expected_res_data_3)


def test_get_sesonal_adjusted_mth_change(data, test_df_datasource):
    seasonal_mnt_change_1 = data.get_mth_change(
        test_df_datasource,
        group_filter=["H", "J"],
        includes_parent_aggregate=False,
    )
    seasonal_mnt_change_2 = data.get_mth_change(
        test_df_datasource,
        group_filter=["H", "49.1", "49.2"],
        includes_parent_aggregate=False,
    )
    seasonal_mnt_change_3 = data.get_mth_change(
        test_df_datasource,
        max_nace_level=1,
    )

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
                "  J - Informasjon og kommunikasjon",
            ],
            'weight': [34.7, 44.7],
            'value': [114.7, 89.5],
            'value1': [-1.8, 8.5],
            'weighted_value': [-0.6, 3.8],
        }
    )

    expected_res_data_2 = pd.DataFrame(
        {
            "nar": [
                "  H - Transport og lagring",
                "        49.1 - Passasjertransport med je",
                "        49.2 - Godstransport med jernban",
            ],
            'weight': [34.7, 11.6, 21.9],
            'value': [114.7, 83.2, 89.3],
            'value1': [-1.8, -29.7, -25.4],
            'weighted_value': [-0.6, -3.4, -5.6],
        }
    )
    expected_res_data_3 = pd.DataFrame(
        {
            "nar": [
                "  H - Transport og lagring",
                "  J - Informasjon og kommunikasjon",
            ],
            'weight': [34.7, 44.7],
            'value': [114.7, 89.5],
            'value1': [-1.8, 8.5],
            'weighted_value': [-0.6, 3.8],
        }
    )

    assert seasonal_mnt_change_1.header_1 == expected_header_1
    assert seasonal_mnt_change_1.header_2 == expected_header_2
    pd.testing.assert_frame_equal(seasonal_mnt_change_1.res_data, expected_res_data_1)
    pd.testing.assert_frame_equal(seasonal_mnt_change_2.res_data, expected_res_data_2)
    pd.testing.assert_frame_equal(seasonal_mnt_change_3.res_data, expected_res_data_3)


def test_get_sesonal_adjusted_12_mth_change(data, test_df_datasource):
    seasonal_12_mnt_change_1 = data.get_12_mth_change(
        test_df_datasource,
        group_filter=["H", "J"],
        includes_parent_aggregate=False,
    )
    seasonal_12_mnt_change_2 = data.get_12_mth_change(
        test_df_datasource,
        group_filter=["H", "49.1", "49.2"],
        includes_parent_aggregate=False,
    )
    seasonal_12_mnt_change_3 = data.get_12_mth_change(
        test_df_datasource,
        max_nace_level=1,
    )

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
                "  J - Informasjon og kommunikasjon",
            ],
            'weight': [34.7, 44.7],
            'value': [114.7, 89.5],
            'value1': [-3.2, -22.5],
            'weighted_value': [-1.1, -6.1],
        }
    )

    expected_res_data_2 = pd.DataFrame(
        {
            "nar": [
                "  H - Transport og lagring",
                "        49.1 - Passasjertransport med je",
                "        49.2 - Godstransport med jernban",
            ],
            'weight': [34.7, 11.6, 21.9],
            'value': [114.7, 83.2, 89.3],
            'value1': [-3.2, -0.1, 0.0],
            'weighted_value': [-1.1, -0.0, 0.0]
        }
    )

    expected_res_data_3 = pd.DataFrame(
        {
            "nar": [
                "  H - Transport og lagring",
                "  J - Informasjon og kommunikasjon",
            ],
            'weight': [34.7, 44.7],
            'value': [114.7, 89.5],
            'value1': [-3.2, -22.5],
            'weighted_value': [-1.1, -6.1],
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


def test_get_table_1(data, test_df_datasource):
    get_table_1 = data.get_table_1(test_df_datasource)
    expected_header_1 = [
        "",
        "",
        "",
        "",
        "% Endring",
        "% Endring",
        "% Endring",
        "% Endring",
        "% Endring",
    ]
    expected_header_2 = [
        "",
        "Sep 2024 - Oct 2024",
        "Oct 2024 - Nov 2024",
        "Nov 2024 - Dec 2024",
        "Jul 2024 - Aug 2024",
        "Aug 2024 - Sep 2024",
        "Sep 2024 - Oct 2024",
        "Oct 2024 - Nov 2024",
        "Nov 2024 - Dec 2024",
    ]
    expected_res_data = pd.DataFrame(
        {
            "nar": [
                "  H - Transport og lagring",
                "        49.1 - Passasjertransport med je",
                "        49.2 - Godstransport med jernban",
                "  J - Informasjon og kommunikasjon",
                "    59 - Film-, video- og fjernsynsprogr",
            ],
            "value": [115.7, 89.0, 119.4, 119.2, 102.0],
            "value0": [116.8, 118.4, 119.7, 82.5, 115.1],
            "value1": [114.7, 83.2, 89.3, 89.5, 92.7],
            "value2": [6.6, 23.1, -0.4, 40.7, 0.8],
            "value3": [-6.1, -13.7, -15.3, 3.5, 24.9],
            "value4": [18.9, 4.1, 46.0, -0.6, -6.2],
            "value5": [1.0, 33.0, 0.3, -30.8, 12.8],
            "value6": [-1.8, -29.7, -25.4, 8.5, -19.5],
        }
    )

    assert get_table_1.header_1 == expected_header_1
    assert get_table_1.header_2 == expected_header_2
    pd.testing.assert_frame_equal(get_table_1.res_data, expected_res_data)

def test_data_manager(data):
    assert isinstance(data, DataManager)
