import uuid
from typing import Any

import polars as pl
from ssb_dash_components import Accordion
from ssb_dash_components import Checkbox

from dash import ALL
from dash import MATCH
from dash import Input
from dash import Output
from dash import State
from dash import callback
from dash import ctx
from dash import dcc
from dash import html

from pydantic import BaseModel

from .loading_test import DatasetConfig


class CurrentState(BaseModel):
    pass


class SeriesSelector(html.Div):
    """The class handles its own global state.

    This is the state that is passed to components downstream.
    """

    class ids:
        """Generates standardized IDs for the SeriesSelector component."""

        @staticmethod
        def store(aio_id: str) -> dict:
            """ID for the main store subcomponent."""
            return {
                "component": "SeriesSelector",
                "subcomponent": "store",
                "aio_id": aio_id,
            }

        @staticmethod
        def selected_store(aio_id: str) -> dict:
            """ID for the selected_store subcomponent."""
            return {
                "component": "SeriesSelector",
                "subcomponent": "selected_store",
                "aio_id": aio_id,
            }

        @staticmethod
        def selector_container(aio_id: str) -> dict:
            """ID for the selector-container subcomponent."""
            return {
                "component": "SeriesSelector",
                "subcomponent": "selector-container",
                "aio_id": aio_id,
            }

        @staticmethod
        def checkbox(
            aio_id: str, path: str | Any, col: str | Any, dataset: str | Any
        ) -> dict:
            """ID for a checkbox subcomponent, with dataset and column context."""
            return {
                "component": "SeriesSelector",
                "subcomponent": "checkbox",
                "path": path,
                "aio_id": aio_id,
                "col": col,
                "dataset": dataset,
            }

        @staticmethod
        def checkbox_store(
            aio_id: str, path: str | Any, col: str | Any, dataset: str | Any
        ) -> dict:
            """ID for a checkbox subcomponent, with dataset and column context."""
            return {
                "component": "SeriesSelector",
                "subcomponent": "checkbox_store",
                "path": path,
                "aio_id": aio_id,
                "col": col,
                "dataset": dataset,
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
                dcc.Store(data=[], id=self.ids.selected_store(aio_id)),
                html.Div(id=self.ids.selector_container(aio_id)),
            ]
        )

        @callback(
            Output(self.ids.selector_container(aio_id), "children"),
            Input(self.ids.store(aio_id), "data"),
            State(self.ids.selected_store(aio_id), "data"),
        )
        def update_selector_view(selected: list[dict], current_state: list[dict]):
            # Renders the new series selector when selected files change
            # Uses stored state so users dont lose settings already entered
            children: list[Accordion] = []

            for item in selected:
                path: str | None = item.get("path")
                dataset: str | None = item.get("dataset")
                if path is None:
                    continue

                schema = pl.read_parquet_schema(path)
                cols: list[Checkbox] = []
                for col, col_type in schema.items():
                    if col_type.is_numeric():
                        checkbox_val = any(
                            (i.get("path") == path)
                            and (i.get("col") == col)
                            and (i.get("dataset") == dataset)
                            for i in current_state
                        )

                        # If only one series we automatically toogle this series
                        if (
                            len(
                                [val for (_, val) in schema.items() if val.is_numeric()]
                            )
                            == 1
                        ):
                            checkbox_val = True

                        check_box_id = self.ids.checkbox(aio_id, path, col, dataset)
                        if checkbox_val:
                            store_val = check_box_id
                        else:
                            store_val = None
                        box = Checkbox(
                            label=col,
                            name=col,
                            value=checkbox_val,
                            id=check_box_id,
                        )
                        cols.append(box)
                        cols.append(
                            dcc.Store(
                                id=self.ids.checkbox_store(aio_id, path, col, dataset),
                                data=store_val,  # pyright: ignore
                            )
                        )
                children.append(Accordion(header=path, children=cols))
            return children

        @callback(
            # Output(self.ids.selected_store(aio_id), "data"),
            Output(self.ids.checkbox_store(aio_id, MATCH, MATCH, MATCH), "data"),
            Input(self.ids.checkbox(aio_id, MATCH, MATCH, MATCH), "value"),
            Input(self.ids.checkbox(aio_id, MATCH, MATCH, MATCH), "id"),
            # State(self.ids.selected_store(aio_id), "data"),
            State(self.ids.checkbox_store(aio_id, MATCH, MATCH, MATCH), "data"),
            State(self.ids.store(aio_id), "data"),
        )
        def update_checked(
            checked: bool,
            ids: dict[str, str],
            current_state: dict[str, str] | None,
            files: list[dict[str, str]],
        ):
            # Reacts to inputs in settings and stores them in a store so the user
            # doesn't lose settings when they add another series.
            # print(checked, ids, current_state, files)
            triggered: None | dict = ctx.triggered_id
            
            if not isinstance(triggered, dict):
                return current_state

            if ids["dataset"] != triggered["dataset"]:
                return current_state

            if ids["path"] != triggered["path"]:
                return current_state

            if ids["col"] != triggered["col"]:
                return current_state

            if ids["aio_id"] != triggered["aio_id"]:
                return current_state

            if checked:
                return ids
            else:
                return None

        @callback(
            Output(self.ids.selected_store(aio_id), "data"),
            Input(self.ids.checkbox_store(aio_id, ALL, ALL, ALL), "data"),
        )
        def handle_update_selected_store(ids: list):
            return [item for item in ids if item is not None]
