from ssb_dash_components.DropdownMultiple import DropdownMultiple


import uuid
from dataclasses import dataclass
from dataclasses import field
from typing import Any

from ssb_dash_components import Dropdown
from ssb_dash_components import DropdownMultiple

from dash import ALL, MATCH
from dash import Input
from dash import Output
from dash import State
from dash import callback
from dash import dcc
from dash import html


from .data_source import GenVisData
from .loading_test import DatasetConfig


@dataclass
class SeriesSetting:
    """A simple class for adding typing to the ids. Easier to work with."""

    path: str
    dataset: str
    col: str
    groupby: list[str] = field(default_factory=list)
    aggregation: str | None = field(default=None)

    def hash(self) -> int:
        """Returnerer hash av objektets attributter."""
        return hash(self.__dict__)

    def dict(self) -> dict:
        """Returnerer objektets attributter som en dict."""
        return self.__dict__


def create_dropdown_options(groupbys: list[str]) -> list[dict[str, str]]:
    options = []
    for i in groupbys:
        options.append({"title": i, "id": i})
    return options


class SeriesSettingsDisplay(html.Div):
    """The class handles its own global state.

    This is the state that is passed to components downstream.
    """

    class ids:
        """Generates standardized IDs for the SeriesSettingsDisplay component."""

        @staticmethod
        def store(aio_id: str) -> dict:
            """ID for the main store of the SeriesDisplay."""
            return {
                "component": "SeriesDisplay",
                "subcomponent": "store",
                "aio_id": aio_id,
            }

        @staticmethod
        def settings_store(aio_id: str) -> dict:
            """ID for the settings store of the SeriesDisplay."""
            return {
                "component": "SeriesDisplay",
                "subcomponent": "settings-store",
                "aio_id": aio_id,
            }

        @staticmethod
        def settings_display(aio_id: str) -> dict:
            """ID for the settings display component."""
            return {
                "component": "SeriesDisplay",
                "subcomponent": "settings-display",
                "aio_id": aio_id,
            }

        @staticmethod
        def aggregation_settings(
            aio_id: str, path: str | Any, dataset: str | Any, col: str | Any
        ) -> dict:
            """ID for an aggregation settings component."""
            return {
                "component": "SeriesDisplay",
                "subcomponent": "aggregation-settings",
                "aio_id": aio_id,
                "dataset": dataset,
                "path": path,
                "col": col,
            }

        @staticmethod
        def groupby_settings(
            aio_id: str,
            path: str | Any,
            dataset: str | Any,
            col: str | Any,
            groupby: str | Any,
        ) -> dict:
            """ID for a groupby settings component."""
            return {
                "component": "SeriesDisplay",
                "subcomponent": "groupby-settings",
                "aio_id": aio_id,
                "dataset": dataset,
                "path": path,
                "col": col,
                "groupby": groupby,
            }

        @staticmethod
        def groupby_settings_store(
            aio_id: str,
            path: str | Any,
            dataset: str | Any,
            col: str | Any,
        ) -> dict:
            """ID for a groupby settings component."""
            return {
                "component": "SeriesDisplay",
                "subcomponent": "groupby-settings-store",
                "aio_id": aio_id,
                "dataset": dataset,
                "path": path,
                "col": col,
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
                dcc.Store(data=[], id=self.ids.store(aio_id)),
                dcc.Store(data=[], id=self.ids.settings_store(aio_id)),
                html.Div(id=self.ids.settings_display(aio_id)),
            ],
            style={
                "width": "100%",
            },
        )

        @callback(
            Output(self.ids.settings_display(aio_id), "children"),
            Input(self.ids.store(aio_id), "data"),
            State(self.ids.settings_store(aio_id), "data"),
        )
        def update_display(
            data: list[dict[str, str]], current_state: list[dict[str, Any]]
        ):
            children = []
            for item in data:
                dataset = item.get("dataset")
                path = item.get("path")
                col = item.get("col")

                if (dataset is None) or (path is None) or (col is None):
                    continue

                current = {}
                for state in current_state:
                    if (
                        (state is not None)
                        and (state["dataset"] == dataset)
                        and (state["path"] == path)
                        and (state["col"] == col)
                    ):
                        current = state

                data_src = datasets.get(dataset)
                groupby: dict = current.get("groupby_settings", {})

                if data_src:
                    src = GenVisData(path, data_src.index_col, data_src.index_pattern)
                    groupby_dropdowns = []
                    if isinstance(data_src.groupby_col, list):
                        for groupby_col in data_src.groupby_col:
                            groupbys = src.get_unique_groupby(groupby_col)
                            options = create_dropdown_options(groupbys)
                            groupby_id = self.ids.groupby_settings(
                                aio_id, path, dataset, col, groupby_col
                            )
                            groupby_dropdown = DropdownMultiple(
                                header=groupby_col,
                                items=options,  # pyright: ignore
                                searchable=True,
                                value=groupby.get(groupby_col),
                                id=groupby_id,
                            )
                            groupby_dropdowns.append(groupby_dropdown)

                    elif data_src.groupby_col is not None:
                        groupbys = src.get_unique_groupby(data_src.groupby_col)
                        options = create_dropdown_options(groupbys)
                        groupby_id = self.ids.groupby_settings(
                            aio_id, path, dataset, col, data_src.groupby_col
                        )
                        groupby_dropdown = DropdownMultiple(
                            header=data_src.groupby_col,
                            items=options,  # pyright: ignore
                            searchable=True,
                            value=groupby.get(data_src.groupby_col),
                            id=groupby_id,
                        )
                        groupby_dropdowns.append(groupby_dropdown)
                    else:
                        continue

                    child = html.Div(
                        children=[
                            html.P(path),
                            html.P(col),
                            dcc.Store(
                                data=groupby,
                                id=self.ids.groupby_settings_store(
                                    aio_id, path, dataset, col
                                ),
                            ),
                            Dropdown(
                                items=[
                                    {"title": "None", "id": ""},
                                    {"title": "pct", "id": "pct"},
                                    {"title": "ypct", "id": "ypct"},
                                ],
                                value=current.get("aggregation", ""),
                                id=self.ids.aggregation_settings(
                                    aio_id, path, dataset, col
                                ),
                            ),
                            *groupby_dropdowns,
                        ],
                        style={
                            "border": "1px solid #1A9D49",
                            "marginBottom": "2px",
                            "padding": "2px",
                        },
                    )
                    children.append(child)
            return children

        @callback(
            Output(
                self.ids.groupby_settings_store(aio_id, MATCH, MATCH, MATCH), "data"
            ),
            Input(self.ids.groupby_settings(aio_id, MATCH, MATCH, MATCH, ALL), "value"),
            Input(self.ids.groupby_settings(aio_id, MATCH, MATCH, MATCH, ALL), "id"),
            Input(self.ids.aggregation_settings(aio_id, MATCH, MATCH, MATCH), "value"),
        )
        def handle_settings_selection(
            groupbys: list[list[str]] | None,
            ids: list[dict] | None,
            aggregation: str | None,
        ):
            state: dict[str, Any] = {}

            if (ids is None) or (groupbys is None):  # or (aggregation is None):
                return state

            state["dataset"] = ids[0].get("dataset")
            state["path"] = ids[0].get("path")
            state["col"] = ids[0].get("col")
            state["aggregation"] = aggregation
            state["groupby_settings"] = {}

            if (groupbys is not None) and (ids is not None):
                for values, _id in zip(groupbys, ids, strict=True):
                    groupby_col = _id.get("groupby")
                    state["groupby_settings"][groupby_col] = values

            return state

        @callback(
            Output(self.ids.settings_store(aio_id), "data"),
            Input(self.ids.groupby_settings_store(aio_id, ALL, ALL, ALL), "data"),
        )
        def update_global_store(data: list):
            return data
