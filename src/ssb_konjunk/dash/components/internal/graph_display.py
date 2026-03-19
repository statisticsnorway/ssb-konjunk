import uuid
from itertools import cycle
from typing import Literal

import plotly.graph_objects as go
import polars as pl

from dash import Input
from dash import Output
from dash import State
from dash import callback
from dash import dcc
from dash import html

from .data_source import GenVisData
from .loading_test import DatasetConfig
from .series_setting_display import SeriesSetting

GRAPH_COLORS = [
    "#1A9D49",
    "#075745",
    "#1D9DE2",
    "#0F2080",
    "#C78800",
    "#471F00",
    "#C775A7",
    "#A3136C",
    "#909090",
    "#000000",
]


class GraphDisplay(html.Div):
    """The class handles its own global state.

    This is the state that is passed to components downstream.
    """

    class ids:
        """Generates standardized IDs for the GraphDisplay component."""

        @staticmethod
        def series_store(aio_id: str) -> dict:
            """ID for the series store subcomponent."""
            return {
                "component": "GraphDisplay",
                "subcomponent": "series-store",
                "aio_id": aio_id,
            }

        @staticmethod
        def settings_store(aio_id: str) -> dict:
            """ID for the settings store subcomponent."""
            return {
                "component": "GraphDisplay",
                "subcomponent": "settings-store",
                "aio_id": aio_id,
            }

        @staticmethod
        def graph(aio_id: str) -> dict:
            """ID for the graph subcomponent."""
            return {
                "component": "GraphDisplay",
                "subcomponent": "graph",
                "aio_id": aio_id,
            }

    def __init__(
        self, datasets: dict[str, DatasetConfig], aio_id: None | str = None
    ) -> None:
        """Expects the datasets.

        Can provide an aio_id if necessary.
        """
        if aio_id is None:
            aio_id = str(uuid.uuid4())

        super().__init__(
            children=[
                dcc.Store(data=[], id=self.ids.series_store(aio_id)),
                dcc.Store(data=[], id=self.ids.settings_store(aio_id)),
                html.Div(
                    children=[
                        dcc.Graph(id=self.ids.graph(aio_id)),
                    ],
                    style={"width": "100%", "height": "100%"},
                ),
            ]
        )

        @callback(
            Output(self.ids.graph(aio_id), "figure"),
            Input(self.ids.series_store(aio_id), "data"),
            Input(self.ids.settings_store(aio_id), "data"),
            State(self.ids.graph(aio_id), "figure"),
        )
        def change_graph(
            series_data: list[SeriesSetting],
            settings: dict[str, str | Literal["none", "discrete"]],
            old_fig: dict,
        ):
            # Callback that updates the graph based on changed series settings or
            # graph settings
            base_year: str | None = settings.get("base_year")
            convert_method: Literal["none", "discrete"] = settings.get("convert", "none")  # type: ignore
            fig = go.Figure()
            fig.update_layout(
                template="simple_white",
                title="Graf",
                autosize=True,
            )

            # Cycle for colors to alternative between allowed colors
            GRAPH_CYCLE = cycle(GRAPH_COLORS)

            if series_data is None:
                return fig

            for item in series_data:
                col = item["col"]
                dataset = item["dataset"]
                filename = item["path"]
                # Aggregation settings are optional
                convert_function = item.get("aggregation", None)
                # Shortens filepaths for displaying
                short_filename = "/".join(filename.split("/")[-2:])
                dataset_config = datasets[dataset]
                dataset = GenVisData(
                    filename, dataset_config.index_col, dataset_config.index_pattern
                )

                data = dataset.data

                if (base_year is not None) and (convert_method != "none"):
                    agg_type = dataset_config.agg_type
                    if agg_type is None:
                        agg_type = dataset_config.agg_type_by_col
                    if agg_type is None:
                        agg_type = {}

                    data = dataset.set_base_year(
                        data,
                        base_year.split("-")[0],
                        col,
                        dataset_config.groupby_col,
                        convert_method,
                        agg_type,
                    )

                if (dataset_config.groupby_col) and ("groupby" in item):
                    for selected_val in item["groupby"]:
                        subset = data.filter(
                            pl.col(dataset_config.groupby_col) == selected_val
                        )
                        if convert_function is not None:
                            if convert_function == "pct":
                                subset = subset.with_columns(pl.col(col).pct_change())

                        # Add a line to the plot
                        fig.add_trace(
                            go.Scatter(
                                x=subset[dataset_config.index_col],
                                y=subset[col],
                                name=f"{short_filename} - {col} - {selected_val}",
                                line=dict(color=next(GRAPH_CYCLE)),
                                mode="lines",
                            )
                        )

                else:
                    if (convert_function is not None) and ("id" in convert_function):
                        if convert_function["id"] == "pct":
                            data = data.with_columns(pl.col(col).pct_change())

                    # Add the line to the plot
                    fig.add_trace(
                        go.Scatter(
                            x=data[dataset_config.index_col],
                            y=data[col],
                            name=col,
                            line=dict(color=next(GRAPH_CYCLE)),
                            mode="lines",
                        )
                    )

            # Tries to keep the selected timerange between callbacks
            selected_time_config = {}
            try:
                selected_time_config["range"] = old_fig["layout"]["xaxis"]["range"]
                selected_time_config["autorange"] = old_fig["layout"]["xaxis"][
                    "autorange"
                ]
            except Exception:
                pass

            # Creates shortcuts for selecting timeframes
            fig.update_layout(
                xaxis=dict(
                    rangeselector=dict(
                        buttons=list(
                            [
                                dict(
                                    count=1,
                                    label="1m",
                                    step="month",
                                    stepmode="backward",
                                ),
                                dict(
                                    count=6,
                                    label="6m",
                                    step="month",
                                    stepmode="backward",
                                ),
                                dict(
                                    count=1, label="YTD", step="year", stepmode="todate"
                                ),
                                dict(
                                    count=1,
                                    label="1y",
                                    step="year",
                                    stepmode="backward",
                                ),
                                dict(step="all"),
                            ]
                        )
                    ),
                    rangeslider=dict(visible=True),
                    type="date",
                    **selected_time_config,
                )
            )
            return fig
