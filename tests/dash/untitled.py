from ssb_konjunk.dash.calculations.calc_data import DataManager
import pandas as pd

test_series = pd.Series(["42.1", "40.2", "40", "F", "40.1"])
sorted_series = DataManager.sort_aggregates(test_series).tolist()
sorted_series


