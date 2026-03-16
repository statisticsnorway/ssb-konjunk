from datetime import date
from typing import Literal

import polars as pl

from .loading_test import AGG_TYPES


class DataSource:
    def __init__(self, filename: str, index_col: str, date_pattern: str) -> None:
        self.dt_colname = "dt"
        self.data = pl.read_parquet(filename).with_columns(
            dt=pl.col(index_col).cast(pl.String).str.to_date(date_pattern)
        )

    def filter_dt(self, lower: date, highest: date):
        return self.data.filter(pl.col(self.dt_colname).is_between(lower, highest))

    def get_unique_dates(self) -> list[date]:
        return self.data.get_column(self.dt_colname).unique().to_list()

    def get_unique_groupby(self, groupby_col: str):
        return self.data.get_column(groupby_col).unique().to_list()

    def subset_group(self, groupby_col: str, filter_val):
        return self.data.filter(pl.col(groupby_col) == filter_val)

    def _set_base_year(
        self,
        data: pl.DataFrame,
        year: str,
        col: str,
        method: Literal["discrete", "none"],
        agg_type: AGG_TYPES,
    ):
        # print(data)
        filtered = data.filter(
            (pl.col(self.dt_colname) < date(int(year), 12, 31))
            & (pl.col(self.dt_colname) >= date(int(year), 1, 1))
        )

        selected_col = filtered.get_column(col).cast(pl.Float64)
        if method == "discrete":
            if agg_type == "AVERAGE":
                index_factor = selected_col.mean()
            else:
                index_factor = selected_col.sum()
        else:
            index_factor = selected_col.mean()

        altered = data.with_columns(
            **{col: (pl.col(col).cast(pl.Float64) / index_factor) * 100}
        )
        # print(altered)
        return altered

    def set_base_year(
        self,
        data: pl.DataFrame,
        year: str,
        col: str,
        group_col: str | None,
        method: Literal["discrete", "none"],
        agg_mapping: AGG_TYPES | dict[str, AGG_TYPES],
    ) -> pl.DataFrame:

        if isinstance(agg_mapping, dict):
            agg_type = agg_mapping.get(col, "AVERAGE")
        elif isinstance(agg_mapping, str):
            agg_type = agg_mapping
        else:
            raise ValueError("")

        if group_col is not None:
            return data.group_by(group_col).map_groups(
                lambda x: self._set_base_year(x, year, col, method, agg_type)
            )
        else:
            return self._set_base_year(data, year, col, method, agg_type)
