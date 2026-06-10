import uuid
from typing import Any

from ssb_dash_components import Checkbox
from ssb_dash_components import Input as SSBInput
from ssb_dash_components import Tabs

from dash import ALL, MATCH
from dash import Input
from dash import Output
from dash import State
from dash import callback
from dash import clientside_callback
from dash import dcc
from dash import html

from .loading_test import DatasetConfig


class TabSelector(html.Div):
    """The class handles its own global state.

    This is the state that is passed to components downstream.
    """

    class ids:
        """Generates standardized IDs for the TabSelector component."""

        @staticmethod
        def tabs(aio_id: str) -> dict:
            """ID for the tabs subcomponent."""
            return {
                "component": "TabSelector",
                "subcomponent": "tabs",
                "aio_id": aio_id,
            }

        @staticmethod
        def checklist_container(aio_id: str) -> dict:
            """ID for the checklist-container subcomponent."""
            return {
                "component": "TabSelector",
                "subcomponent": "checklist-container",
                "aio_id": aio_id,
            }

        @staticmethod
        def checklist_item(aio_id: str, path: str | Any, random: str | Any) -> dict:
            """ID for a checklist item, including path and random key for JS search."""
            return {
                "component": "TabSelector",
                "subcomponent": "checklist_item",
                "path": path,
                "aio_id": aio_id,
                "random": random,  # Required for JS search functionality
            }

        @staticmethod
        def checklist_item_store(
            aio_id: str, path: str | Any, random: str | Any
        ) -> dict:
            """ID for a checklist item, including path and random key for JS search."""
            return {
                "component": "TabSelector",
                "subcomponent": "checklist_item_store",
                "path": path,
                "aio_id": aio_id,
                "random": random,  # Required for JS search functionality
            }

        @staticmethod
        def store(aio_id: str) -> dict:
            """ID for the store subcomponent."""
            return {
                "component": "TabSelector",
                "subcomponent": "store",
                "aio_id": aio_id,
            }

        @staticmethod
        def search(aio_id: str) -> dict:
            """ID for the input/search subcomponent."""
            return {
                "component": "TabSelector",
                "subcomponent": "input",
                "aio_id": aio_id,
            }

    def __init__(
        self,
        datasets: dict[str, DatasetConfig],
        aio_id: None | str = None,
        height: str = "300px",
    ) -> None:
        """Expects the datasets.

        Can provide an aio_id if necessary.
        Height can be provided, but the default value works well for most screen sizes.
        """
        # TODO: Replace height with a more sensible setting.
        if aio_id is None:
            aio_id = str(uuid.uuid4())

        tabs = []
        # Creates the dataset tabs to be displayed.
        for key in datasets.keys():
            tabs.append({"title": key, "path": key})

        super().__init__(
            children=[
                html.H3("Velg filer"),
                dcc.Store(id=self.ids.store(aio_id), data=[]),
                SSBInput(placeholder="Søk", type="text", id=self.ids.search(aio_id)),
                Tabs(
                    id=self.ids.tabs(aio_id),
                    items=tabs,
                    activeOnInit=tabs[0]["title"],
                    active=tabs[0]["title"],
                ),
                html.Div(
                    id=self.ids.checklist_container(aio_id),
                    style={"overflowY": "scroll", "height": height},
                ),
            ],
        )

        # Callback for searching the list of files for each dataset
        # Each checkbox has a random id. The callback uses a regex to search for that partial id
        # since object ids are complicated in dash.
        # The callback just hides items that does not match the search
        # Errors are ignored but logged to the console.
        clientside_callback(
            r"""
            function(searchVal, currState, labels, ids) {
                if (searchVal) {
                    for (let i = 0; i < currState.length; i++) {
                        let unique = ids[i].random;
                        try {
                            const element = document.querySelector(`[id*="${unique}"]`).parentElement;
                            if (!labels[i].includes(searchVal)) {
                                element.style["display"] = "none"
                            } else {
                                element.style["display"] = "flex"
                            }
                        } catch (error) {
                            // Code to handle the error
                            console.error("An error occurred: ", error.message);
                        }

                    }
                }

                return window.dash_clientside.no_update;;
            }
            """,
            Output(self.ids.checklist_item(aio_id, ALL, ALL), "style"),
            Input(self.ids.search(aio_id), "value"),
            State(self.ids.checklist_item(aio_id, ALL, ALL), "style"),
            State(self.ids.checklist_item(aio_id, ALL, ALL), "label"),
            State(self.ids.checklist_item(aio_id, ALL, ALL), "id"),
        )

        @callback(
            Output(self.ids.checklist_container(aio_id), "children"),
            Input(self.ids.tabs(aio_id), "active"),
            State(self.ids.store(aio_id), "data"),
        )
        def update_tabs(selected: str, checked_files: list[dict[str, str]]):
            # Updates which tabs are displayed in the data selector.
            dataset_list = datasets.get(selected)
            children = []
            if dataset_list:
                files = dataset_list.get_entries()
                for file in files:
                    id_str = str(uuid.uuid4())
                    checkbox_id = self.ids.checklist_item(aio_id, file, id_str)
                    checkbox_val: None | dict[str, Any] = None
                    for item in checked_files:
                        if (item["dataset"] == selected) and (item["path"] == file):
                            checkbox_val = {
                                "path": file,
                                "dataset": selected,
                                "checked": True,
                            }

                    children.append(
                        html.Div(
                            children=[
                                Checkbox(
                                    className="TabChecklist",
                                    label=file,
                                    name=file,
                                    value=checkbox_val is not None,
                                    id=checkbox_id,
                                    style={"display": "flex"},
                                ),
                                dcc.Store(
                                    id=self.ids.checklist_item_store(
                                        aio_id, file, id_str
                                    ),
                                    data=checkbox_val,
                                ),
                            ],
                            id=id_str,
                        )
                    )
            return children

        @callback(
            Output(self.ids.checklist_item_store(aio_id, MATCH, MATCH), "data"),
            Input(self.ids.checklist_item(aio_id, MATCH, MATCH), "value"),
            Input(self.ids.checklist_item(aio_id, MATCH, MATCH), "id"),
            State(self.ids.tabs(aio_id), "active"),
            prevent_initial_call=True,
        )
        def update_checked_state(
            checked: bool | None,
            ids: dict[str, str],
            selected_tab: str | None,
        ):
            if checked is not None:
                return {
                    "path": ids["path"],
                    "dataset": selected_tab,
                    "checked": checked,
                }
            else:
                return None

        @callback(
            Output(self.ids.store(aio_id), "data"),
            Input(self.ids.checklist_item_store(aio_id, ALL, ALL), "data"),
            State(self.ids.store(aio_id), "data"),
        )
        def update_output_state(
            items: list[dict[str, Any]], current: list[dict[str, Any]]
        ):
            displayed_active = []
            inactive = []
            if items:
                for item in items:
                    if item is not None:
                        if item["checked"] is True:
                            displayed_active.append(item)
                        else:
                            inactive.append(item)

            if current:
                for state in current:
                    if any(
                        (i["dataset"] == state["dataset"])
                        and (i["path"] == state["path"])
                        for i in displayed_active
                    ):
                        continue

                    if any(
                        (i["dataset"] == state["dataset"])
                        and (i["path"] == state["path"])
                        for i in inactive
                    ):
                        continue

                    displayed_active.append(state)

            displayed_active.sort(key=lambda x: x["path"])
            return displayed_active