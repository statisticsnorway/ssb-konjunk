import numpy as np
import pandas as pd
import polars as pl
import pytest

from ssb_konjunk.dash.calculations.calc_data import DataManager
from ssb_konjunk.dash.calculations.period_utils import Period


def test_data_manager_init(data):

    assert data.data is not None
    assert data.season_adjusted_series is not None
    assert data.raw_series is not None
    assert data.calendar_source is not None


def test_pad_single():
    assert DataManager.pad_single("H") == "H"
    assert DataManager.pad_single("64") == "  64"
    assert DataManager.pad_single("49.1") == "      49.1"


def test_calc_indirect(data, test_df):
    assert data.calc_indirect(test_df, "jus") == pytest.approx(7587.79153, rel=1e-4)


def test_to_percent(data):
    np.random.seed(0)
    weight = data.weight_source.data.filter(pl.col("nar") == "J")["verdi"]
    chg_rate = pl.Series("chg_rate", np.random.uniform(-2, 2, 38))

    percent_change = data.to_percent(weight, chg_rate)
    assert percent_change[0] == pytest.approx(0.088389, rel=1e-4)


def test_header_1(data):
    assert data.header_1 == ["", "Vekt %", "Indeks", "% Endring", "% Endring vektet"]


def test_get_nacer(data):
    assert data.get_nacer() == ["H", "49.1", "49.2", "J", "59",]


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

    sorted_codes = (
        test_series
        .iloc[data.sort_aggregates(test_series).argsort()]
        .tolist()
    )

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
    nace_data = data.get_nacer()
    nace_data_formated = data.format_aggregates(pd.Series(nace_data)).tolist()
    expected = ["H","      49.1", "      49.2", "J", "  59"]
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


def test_get_sesonal_adjusted_3_mth_change(data):
    seasonal_3_mnt_change_1 = data.get_sesonal_adjusted_3_mth_change(
        nace_filter=["H", "J"], includes_parent_aggregate=False
    )
    seasonal_3_mnt_change_2 = data.get_sesonal_adjusted_3_mth_change(
        nace_filter=["H", "49.1", "49.2"], includes_parent_aggregate=False
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
                "  J - Informasjon og kommunikasjon",
            ],
            "weight": [62.8, 37.2],
            "season": [112.2, 95.3],
            "season1": [14.2, -3.8],
            "weighted": [8.9, -1.4],
        }
    )

    expected_res_data_2 = pd.DataFrame(
        {
            "nar": [
                "  H - Transport og lagring",
                "        49.1 - Passasjertransport med je",
                "        49.2 - Godstransport med jernban",
            ],
            "weight": [40.8, 24.0, 35.2],
            "season": [112.2, 104.4, 99.3],
            "season1": [14.2, 9.7, 4.0],
            "weighted": [5.8, 2.3, 1.4],
        }
    )
    expected_res_data_3 = pd.DataFrame(
        {
            "nar": [
                "  H - Transport og lagring",
                "  J - Informasjon og kommunikasjon",
                "    59 - Film-, video- og fjernsynsprogr",
            ],
            "weight": [45.7, 27.1, 33.3],
            "season": [112.2, 95.3, 110.3],
            "season1": [14.2, -3.8, 19.2],
            "weighted": [4.2, -1.4, 4.6],
        }
    )

    assert seasonal_3_mnt_change_1.header_1 == expected_header_1
    assert seasonal_3_mnt_change_1.header_2 == expected_header_2
    pd.testing.assert_frame_equal(seasonal_3_mnt_change_1.res_data, expected_res_data_1)
    pd.testing.assert_frame_equal(seasonal_3_mnt_change_2.res_data, expected_res_data_2)
    pd.testing.assert_frame_equal(seasonal_3_mnt_change_3.res_data, expected_res_data_3)


def test_get_sesonal_adjusted_mth_change(data):
    seasonal_mnt_change_1 = data.get_sesonal_adjusted_mth_change(
        nace_filter=["H", "J"], includes_parent_aggregate=False
    )
    seasonal_mnt_change_2 = data.get_sesonal_adjusted_mth_change(
        nace_filter=["H", "49.1", "49.2"], includes_parent_aggregate=False
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
                "  J - Informasjon og kommunikasjon",
            ],
            "weight": [69.6, 30.4],
            "season": [108.6, 104.3],
            "season1": [-3.2, 19.9],
            "weighted": [-2.2, 6.0],
        }
    )

    expected_res_data_2 = pd.DataFrame(
        {
            "nar": [
                "  H - Transport og lagring",
                "        49.1 - Passasjertransport med je",
                "        49.2 - Godstransport med jernban",
            ],
            "weight": [57.9, 17.1, 25.0],
            "season": [108.6, 116.3, 90.0],
            "season1": [-3.2, 38.6, -24.2],
            "weighted": [-1.8, 6.6, -6.0],
        }
    )
    expected_res_data_3 = pd.DataFrame(
        {
            "nar": [
                "  H - Transport og lagring",
                "  J - Informasjon og kommunikasjon",
                "    59 - Film-, video- og fjernsynsprogr",
            ],
            "weight": [44.7, 19.5, 22.7],
            "season": [108.6, 104.3, 94.9],
            "season1": [-3.2, 19.9, -19.0],
            "weighted": [-1.5, 2.5, -8.6],
        }
    )

    assert seasonal_mnt_change_1.header_1 == expected_header_1
    assert seasonal_mnt_change_1.header_2 == expected_header_2
    pd.testing.assert_frame_equal(seasonal_mnt_change_1.res_data, expected_res_data_1)
    pd.testing.assert_frame_equal(seasonal_mnt_change_2.res_data, expected_res_data_2)
    pd.testing.assert_frame_equal(seasonal_mnt_change_3.res_data, expected_res_data_3)


def test_get_sesonal_adjusted_12_mth_change(data):
    seasonal_12_mnt_change_1 = data.get_sesonal_adjusted_12_mth_change(
        nace_filter=["H", "J"], includes_parent_aggregate=False
    )
    seasonal_12_mnt_change_2 = data.get_sesonal_adjusted_12_mth_change(
        nace_filter=["H", "49.1", "49.2"], includes_parent_aggregate=False
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
                "  J - Informasjon og kommunikasjon",
            ],
            "weight": [69.6, 30.4],
            "calendar": [120.0, 99.1],
            "calendar1": [32.1, 2.8],
            "weighted": [22.4, 0.8],
        }
    )

    expected_res_data_2 = pd.DataFrame(
        {
            "nar": [
                "  H - Transport og lagring",
                "        49.1 - Passasjertransport med je",
                "        49.2 - Godstransport med jernban",
            ],
            "weight": [57.9, 17.1, 25.0],
            "calendar": [120.0, 111.0, 84.2],
            "calendar1": [32.1, 0.9, -14.2],
            "weighted": [18.6, 0.2, -3.6],
        }
    )

    expected_res_data_3 = pd.DataFrame(
        {
            "nar": [
                "  H - Transport og lagring",
                "  J - Informasjon og kommunikasjon",
                "    59 - Film-, video- og fjernsynsprogr",
            ],
            "weight": [44.7, 19.5, 22.7],
            "calendar": [120.0, 99.1, 80.1],
            "calendar1": [32.1, 2.8, -29.9],
            "weighted": [15.6, 1.3, -9.7],
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


def test_get_table_1(data):
    get_table_1 = data.get_table_1()
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
        
            "season":  [115.9, 112.9, 89.3, 94.5, 118.7],
            "season0": [112.2, 83.9, 118.8, 87.0, 117.2],
            "season1": [108.6, 116.3, 90.0, 104.3, 94.9],
            "season2": [-19.7, 2.4, 13.8, -23.6, -10.1],
            "season3": [20.6, 6.4, -7.6, 29.7, 27.6],
            "season4": [12.5, 13.0, -5.4, -11.6, 13.6],
            "season5": [-3.2, -25.7, 33.0, -7.9, -1.3],
            "season6": [-3.2, 38.6, -24.2, 19.9, -19.0],
        }
    )

    assert get_table_1.header_1 == expected_header_1
    assert get_table_1.header_2 == expected_header_2
    pd.testing.assert_frame_equal(get_table_1.res_data, expected_res_data)


def test_get_table_2(data):
    get_table_2 = data.get_table_2()
    expected_header_1 = ["", "", "", "", "% Endring", "% Endring", "% Endring"]
    expected_header_2 = [
        "",
        "Apr 2024 - Jun 2024",
        "Jul 2024 - Sep 2024",
        "Oct 2024 - Dec 2024",
        "Jan 2024 - Mar 2024 / Apr 2024 - Jun 2024",
        "Apr 2024 - Jun 2024 / Jul 2024 - Sep 2024",
        "Jul 2024 - Sep 2024 / Oct 2024 - Dec 2024",
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
            
            "season":  [107.1, 97.9, 112.0, 86.6, 100.2],
            "season0": [98.3, 95.2, 95.5, 99.1, 92.5],
            "season1": [112.2, 104.4, 99.3, 95.3, 110.3],
            "season2": [12.0, -1.6, 23.2, -18.3, -5.5],
            "season3": [-8.2, -2.8, -14.7, 14.4, -7.7],
            "season4": [14.2, 9.7, 4.0, -3.8, 19.2],
        }
    )
    print(get_table_2.res_data)
    print(expected_res_data)
    assert get_table_2.header_1 == expected_header_1
    assert get_table_2.header_2 == expected_header_2
    pd.testing.assert_frame_equal(get_table_2.res_data, expected_res_data)


def test_get_table_3(data):
    get_table_3 = data.get_table_3()
    expected_header_1 = [
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "% Endring",
        "% Endring",
        "% Endring",
    ]
    expected_header_2 = [
        "",
        "Sep 2023 - Oct 2023",
        "Oct 2023 - Nov 2023",
        "Nov 2023 - Dec 2023",
        "Sep 2024 - Oct 2024",
        "Oct 2024 - Nov 2024",
        "Nov 2024 - Dec 2024",
        "Oct 2023 - Oct 2024",
        "Nov 2023 - Nov 2024",
        "Dec 2023 - Dec 2024",
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
            "calendar":  [108.7, 95.8, 94.3, 93.4, 88.9],
            "calendar0": [80.8, 99.5, 117.2, 100.8, 84.0],
            "calendar1": [90.8, 109.9, 98.2, 96.4, 114.3],
            "calendar2": [94.7, 87.6, 93.9, 98.8, 100.3],
            "calendar3": [108.2, 114.5, 116.2, 93.1, 100.8],
            "calendar4": [120.0, 111.0, 84.2, 99.1, 80.1],
            "calendar5": [-12.8, -8.6, -0.3, 5.9, 12.8],
            "calendar6": [33.8, 15.0, -0.8, -7.6, 20.0],
            "calendar7": [32.1, 0.9, -14.2, 2.8, -29.9],
        }
    )

    assert get_table_3.header_1 == expected_header_1
    assert get_table_3.header_2 == expected_header_2
    pd.testing.assert_frame_equal(get_table_3.res_data, expected_res_data)


def test_get_table_4(data):
    get_table_4 = data.get_table_4()
    expected_header_1 = [
        "",
        "3 måneders gjennomsnitt",
        "3 måneders gjennomsnitt",
        "3 måneders gjennomsnitt",
        "3 måneders gjennomsnitt",
        "3 måneders gjennomsnitt",
        "3 måneders gjennomsnitt",
        "% Endring",
        "% Endring",
        "% Endring",
    ]
    expected_header_2 = [
        "",
        "Apr 2023 - Jun 2023",
        "Jul 2023 - Sep 2023",
        "Oct 2023 - Dec 2023",
        "Apr 2024 - Jun 2024",
        "Jul 2024 - Sep 2024",
        "Oct 2024 - Dec 2024",
        "Apr 2023 - Jun 2023 / Apr 2024 - Jun 2024",
        "Jul 2023 - Sep 2023 / Jul 2024 - Sep 2024",
        "Oct 2023 - Dec 2023 / Oct 2024 - Dec 2024",
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
            "calendar":  [90.9, 114.1, 107.4, 94.1, 99.5],
            "calendar0": [101.5, 88.1, 108.8, 98.1, 87.0],
            "calendar1": [93.4, 101.8, 103.2, 96.9, 95.7],
            "calendar2": [102.5, 110.0, 103.6, 105.8, 100.1],
            "calendar3": [96.6, 101.8, 103.8, 102.5, 101.0],
            "calendar4": [107.6, 104.3, 98.1, 97.0, 93.7],
            "calendar5": [12.7, -3.6, -3.5, 12.5, 0.5],
            "calendar6": [-4.8, 15.6, -4.6, 4.4, 16.1],
            "calendar7": [15.2, 2.5, -4.9, 0.2, -2.1],
        }
    )

    assert get_table_4.header_1 == expected_header_1
    assert get_table_4.header_2 == expected_header_2
    pd.testing.assert_frame_equal(get_table_4.res_data, expected_res_data)


def test_get_table_5(data):
    get_table_5 = data.get_table_5()
    expected_header_1 = [
        "",
        "3 måneders gjennomsnitt",
        "3 måneders gjennomsnitt",
        "3 måneders gjennomsnitt",
        "3 måneders gjennomsnitt",
        "3 måneders gjennomsnitt",
        "3 måneders gjennomsnitt",
        "",
        "",
        "% Endring",
    ]
    expected_header_2 = [
        "",
        "Aug 2023 - Sep 2023",
        "Nov 2023 - Dec 2023",
        "Aug 2024 - Sep 2024",
        "Nov 2024 - Dec 2024",
        "Sep 2023 - Nov 2023 / Sep 2024 - Nov 2024",
        "Oct 2023 - Dec 2023 / Oct 2024 - Dec 2024",
        "Dec 2023 - Dec 2023",
        "Dec 2024 - Dec 2024",
        "Dec 2023 - Dec 2023 / Dec 2024 - Dec 2024",
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
            "raw":  [96.8, 95.9, 81.9, 105.9, 100.8],
            "raw0": [111.3, 105.2, 88.5, 106.3, 88.5],
            "raw1": [104.4, 107.0, 112.6, 100.9, 97.7],
            "raw2": [85.0, 106.1, 105.0, 99.3, 90.3],
            "raw3": [-3.3, 5.6, 30.7, -10.0, 8.5],
            "raw4": [-14.9, -0.1, 25.1, -9.4, 4.7],
            "raw5": [109.4, 116.1, 93.1, 104.9, 86.5],
            "raw6": [86.0, 93.3, 118.0, 91.4, 89.9],
            "raw7": [-21.4, -19.6, 26.8, -12.9, 4.0],
        }
    )

    assert get_table_5.header_1 == expected_header_1
    assert get_table_5.header_2 == expected_header_2
    pd.testing.assert_frame_equal(get_table_5.res_data, expected_res_data)


def test_get_table_6(data):
    get_table_6 = data.get_table_6()
    expected_header_1 = []
    expected_header_2 = [
        "",
        "Dec 2021 - Dec 2022",
        "Dec 2022 - Dec 2023",
        "Dec 2023 - Dec 2024",
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
            "weight":  [29.3, 21.6, 30.1, 32.3, 32.5],
            "weight0": [23.5, 28.2, 28.0, 34.2, 26.7],
            "weight1": [35.5, 32.2, 27.7, 29.9, 33.7],
        }
    )

    assert get_table_6.header_1 == expected_header_1
    assert get_table_6.header_2 == expected_header_2
    pd.testing.assert_frame_equal(get_table_6.res_data, expected_res_data)


def test_data_manager(data):
    assert isinstance(data, DataManager)
