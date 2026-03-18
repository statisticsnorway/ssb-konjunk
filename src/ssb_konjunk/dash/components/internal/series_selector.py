import uuid

import polars as pl
from ssb_dash_components import Accordion
from ssb_dash_components import Checkbox

from dash import ALL
from dash import Input
from dash import Output
from dash import State
from dash import callback
from dash import ctx
from dash import dcc
from dash import html

from .loading_test import DatasetConfig


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
        def checkbox(aio_id: str, path: str, col: str, dataset: str) -> dict:
            """ID for a checkbox subcomponent, with dataset and column context."""
            return {
                "component": "SeriesSelector",
                "subcomponent": "checkbox",
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
                        box = Checkbox(
                            label=col,
                            name=col,
                            value=any(
                                (i.get("path") == path)
                                and (i.get("col") == col)
                                and (i.get("dataset") == dataset)
                                for i in current_state
                            ),
                            id=self.ids.checkbox(aio_id, path, col, dataset),
                        )
                        cols.append(box)

                children.append(Accordion(header=path, children=cols))
            return children

        @callback(
            Output(self.ids.selected_store(aio_id), "data"),
            Input(self.ids.checkbox(aio_id, ALL, ALL, ALL), "value"),
            Input(self.ids.checkbox(aio_id, ALL, ALL, ALL), "id"),
            State(self.ids.selected_store(aio_id), "data"),
            State(self.ids.store(aio_id), "data"),
        )
        def update_checked(
            checked: list[bool],
            ids: list[dict[str, str]],
            current_state: list[dict[str, str]],
            files: list[dict[str, str]],
        ):
            # Reacts to inputs in settings and stores them in a store so the user
            # doesn't lose settings when they add another series.
            triggered = ctx.triggered_id

            # We need to drop files that have been removed up stream
            # This ensures that when a user unselects a file it is removed from the view and
            # all hidden states
            active_files = {file["path"] for file in files}
            active_state = {file["path"] for file in current_state}
            files_to_keep = active_state.intersection(active_files)
            current_state = [
                file for file in current_state if file["path"] in files_to_keep
            ]

            # A callback is triggered when checkboxes are first rendered
            # This is not an interaction and the following code makes sure
            # we ignore that callback
            if any(".id" in key for key in ctx.triggered_prop_ids.keys()):
                return current_state

            if isinstance(triggered, dict):
                col: str | None = triggered.get("col")
                path: str | None = triggered.get("path")
                dataset: str | None = triggered.get("dataset")
                if col is None:
                    return current_state

                if path is None:
                    return current_state

                if dataset is None:
                    return current_state

                for item_id, item in zip(ids, checked, strict=True):
                    if ctx.triggered_id == item_id:
                        if not item:
                            current_state = [
                                i
                                for i in current_state
                                if (i.get("path") != path)
                                or (i.get("col") != col)
                                or (i.get("dataset") != dataset)
                            ]
                            break
                        else:
                            current_state.append(
                                {"path": path, "col": col, "dataset": dataset}
                            )
                            break

            return current_state
