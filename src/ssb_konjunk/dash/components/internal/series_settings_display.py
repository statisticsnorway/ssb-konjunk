import uuid
from dataclasses import dataclass
from dataclasses import field
from typing import Any

from ssb_dash_components import Dropdown
from ssb_dash_components import DropdownMultiple

from dash import ALL
from dash import Input
from dash import Output
from dash import State
from dash import callback
from dash import ctx
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
            aio_id: str, path: str | Any, dataset: str | Any, col: str | Any
        ) -> dict:
            """ID for a groupby settings component."""
            return {
                "component": "SeriesDisplay",
                "subcomponent": "groupby-settings",
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
            # Updates series settings based on which series are checked
            curr_state = [SeriesSetting(**item) for item in current_state]

            children = []
            # new_state: list[SeriesSetting] = []
            for item in data:
                dataset = item.get("dataset")
                path = item.get("path")
                col = item.get("col")

                if dataset is None:
                    continue

                if path is None:
                    continue

                if col is None:
                    continue

                states = None
                for state in curr_state:
                    if (
                        (state.col == col)
                        and (state.dataset == dataset)
                        and (state.path == path)
                    ):
                        states = state
                        break
                else:
                    states = SeriesSetting(path=path, col=col, dataset=dataset)
                # new_state.append(states)

                data_src = datasets.get(dataset)
                if data_src:
                    src = GenVisData(path, data_src.index_col, data_src.index_pattern)
                    groupby_dropdown = None
                    if data_src.groupby_col:
                        groupbys = src.get_unique_groupby(data_src.groupby_col)
                        options = []
                        for i in groupbys:
                            options.append({"title": i, "id": i})
                        groupby_dropdown = DropdownMultiple(
                            header="Groupby",
                            items=options,
                            searchable=True,
                            value=states.groupby if states.groupby else [],
                            id=self.ids.groupby_settings(aio_id, path, dataset, col),
                        )

                    child = html.Div(
                        children=[
                            html.P(path),
                            Dropdown(
                                items=[
                                    {"title": "None", "id": ""},
                                    {"title": "pct", "id": "pct"},
                                    {"title": "ypct", "id": "ypct"},
                                ],
                                value=(
                                    states.aggregation if states.aggregation else None
                                ),
                                id=self.ids.aggregation_settings(
                                    aio_id, path, dataset, col
                                ),
                            ),
                            groupby_dropdown,
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
            Output(self.ids.settings_store(aio_id), "data"),
            Input(self.ids.groupby_settings(aio_id, ALL, ALL, ALL), "value"),
            Input(self.ids.groupby_settings(aio_id, ALL, ALL, ALL), "id"),
            Input(self.ids.aggregation_settings(aio_id, ALL, ALL, ALL), "id"),
            Input(self.ids.aggregation_settings(aio_id, ALL, ALL, ALL), "value"),
            State(self.ids.settings_store(aio_id), "data"),
            State(self.ids.store(aio_id), "data"),
        )
        def update_groupby_settings(
            values: list[list[str]],
            ids: list[dict[str, str]],
            agg_ids: list[dict[str, str]],
            agg_settings: list[str | None],
            current_state: list[dict[str, Any]],
            input_data: list[dict[str, Any]],
        ):
            # Callback that handles the global state of the component
            curr_state: list[SeriesSetting] = []

            # This filters out data in the current state that are not present in the input and adds new
            # files to the state
            for item in input_data:
                for data in current_state:
                    if not isinstance(data, dict):
                        continue
                    if (
                        (data["path"] == item["path"])
                        and (data["col"] == item["col"])
                        and (data["dataset"] == item["dataset"])
                    ):
                        curr_state.append(SeriesSetting(**data))
                        break
                else:
                    curr_state.append(SeriesSetting(**item))

            # A callback is triggered when checkboxes are first rendered
            # This is not an interaction and the following code makes sure
            # we ignore that callback
            if any(".id" in key for key in ctx.triggered_prop_ids.keys()):
                return [item.dict() for item in curr_state]

            triggered: dict | None = ctx.triggered_id
            if (triggered is None) or not isinstance(triggered, dict):
                return [item.dict() for item in curr_state]

            path = triggered.get("path")
            dataset = triggered.get("dataset")
            col = triggered.get("col")

            # Handles groupby settings
            for item_id, item in zip(ids, values, strict=True):
                if item_id == ctx.triggered_id:
                    for idx, val in enumerate(curr_state):
                        if (
                            (val.col == col)
                            and (val.dataset == dataset)
                            and (val.path == path)
                        ):
                            curr_state[idx].groupby = item
                            break
                    break

            # Handles aggregation settings
            for item_id, item in zip(agg_ids, agg_settings, strict=True):
                if item_id == ctx.triggered_id:
                    for idx, val in enumerate(curr_state):
                        if (
                            (val.col == col)
                            and (val.dataset == dataset)
                            and (val.path == path)
                        ):
                            curr_state[idx].aggregation = item
                            break

            return [item.dict() for item in curr_state]
