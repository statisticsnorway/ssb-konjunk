"""Dash component for generic visualization at SSB.

This module provides a dash all-in-one component that are intended to replace
some functions of myfame for generic visualizations.
It will only work in SSBs-developer environments.

The module expects the path to JSON-configuration file.

"dataset_1": {
        "glob_pattern": "glob/to/files/*.parquet",
        "index_col": "period", // The columns that contains the periods.
        "index_pattern": "%Y-%m", // The pattern of the period column. See: https://strftime.org/
        "groupby_col": "nar" // OPTIONAL: Columns that are supposed to be subset
        "agg_type": "AVERAGE" // OPTIONAL: For now, only "AVERAGE" is allowed.
    },
"dataset_2": ...

Example:
    ```
    from ssb_konjunk.dash.gen_vis import GenVis
    layout = GenVis("./data_setup.json")
    # Or

    from dash import html

    layout = html.Div(
        children = [
            GenVis("./data_setup.json")
        ]
    )
    ```


"""

import uuid

from ssb_dash_components import Button

from dash import Input
from dash import Output
from dash import callback
from dash import html

from .internal.graph_display import GraphDisplay
from .internal.graph_settings_display import GraphSettingsDisplay
from .internal.loading_test import load_datasets
from .internal.series_selector import SeriesSelector
from .internal.series_settings_display import SeriesSettingsDisplay
from .internal.tab_selector import TabSelector


class GenVis(html.Div):

    class ids:
        dropdown = lambda aio_id: {
            "component": "MarkdownWithColorAIO",
            "subcomponent": "dropdown",
            "aio_id": aio_id,
        }
        markdown = lambda aio_id: {
            "component": "MarkdownWithColorAIO",
            "subcomponent": "markdown",
            "aio_id": aio_id,
        }

    # Make the ids class a public class
    ids = ids

    # Define the arguments of the All-in-One component
    def __init__(self, config_path: str, aio_id=None):
        """Returns a component that can be used as its own page or as a component in other layouts.

        Args:
            config_path (str): Path to the config file.
            aio_id (str | None): An optional id. Will be randomised if not provided.
        """
        if aio_id is None:
            aio_id = str(uuid.uuid4())

        datasets = load_datasets(config_path)
        select_data = {}
        for key, value in datasets.items():
            select_data[key] = value.get_entries()

        super().__init__(
            [
                html.Div(
                    [
                        html.Div(
                            children=[
                                html.Div(
                                    children=[
                                        Button(
                                            "Oppdater datasett",
                                            id="update-dataset-button",
                                        ),
                                    ],
                                ),
                                html.Div(
                                    TabSelector(
                                        datasets, aio_id=aio_id, height="200px"
                                    ),
                                ),
                            ],
                        ),
                        html.Div(
                            children=[
                                html.H3("Velg serier"),
                                html.Div(
                                    children=[SeriesSelector(datasets, aio_id=aio_id)],
                                ),
                            ],
                        ),
                        html.Div(
                            children=[GraphSettingsDisplay(aio_id)],
                        ),
                    ],
                    style={
                        "display": "grid",
                        "gridTemplateColumns": "40% 45% 15%",
                    },
                ),
                html.Div(
                    children=[
                        html.Div(
                            html.Div(
                                children=[
                                    SeriesSettingsDisplay(datasets, aio_id),
                                    GraphDisplay(datasets, aio_id),
                                ],
                                id="graph-display",
                                style={
                                    "height": "100px",
                                    "width": "100%",
                                    "display": "grid",
                                    "gridTemplateColumns": "30% 70%",
                                },
                            )
                        ),
                    ],
                    style={
                        "gap": "30px",
                    },
                ),
            ],
            style={"gap": "10px"},
        )

        @callback(
            Output(SeriesSelector.ids.store(aio_id), "data"),
            Input(TabSelector.ids.store(aio_id), "data"),
        )
        def update_selected(selected):
            """Move data from the file selector to the series selector"""
            return selected

        @callback(
            Output(SeriesSettingsDisplay.ids.store(aio_id), "data"),
            Input(SeriesSelector.ids.selected_store(aio_id), "data"),
        )
        def update_display(selected):
            """Move data from the series selcetor to series settings display"""
            return selected

        @callback(
            Output(GraphDisplay.ids.series_store(aio_id), "data"),
            Input(SeriesSettingsDisplay.ids.settings_store(aio_id), "data"),
        )
        def update_graph_series(selected):
            """Moves series settings to the graph display"""
            return selected

        @callback(
            Output(GraphDisplay.ids.settings_store(aio_id), "data"),
            Input(GraphSettingsDisplay.ids.settings_store(aio_id), "data"),
        )
        def update_graph_settings(settings):
            """Move general graph settings to the graph display"""
            return settings
