import uuid
from dataclasses import dataclass, field

from dash import ALL, html, Input, callback, Output, State, dcc, ctx
from ssb_dash_components import Dropdown, DropdownMultiple

from .loading_test import DatasetConfig
from .data_source import DataSource


@dataclass
class SeriesSetting:
    """A simple class for adding typing to the ids. Easier to work with"""
    path: str
    dataset: str
    col: str
    groupby: list[str] = field(default_factory=list)
    aggregation: str | None = field(default=None)

    def hash(self):
        return hash(self.__dict__)

    def dict(self):
        return self.__dict__


class SeriesSettingsDisplay(html.Div):
    """
    The class handles its own global state. 
    This is the state that is passed to components downstream
    """
    class ids:
        store = lambda aio_id: {
            "component": "SeriesDisplay",
            "subcomponent": "store",
            "aio_id": aio_id,
        }
        settings_store = lambda aio_id: {
            "component": "SeriesDisplay",
            "subcomponent": "settings-store",
            "aio_id": aio_id,
        }
        settings_display = lambda aio_id: {
            "component": "SeriesDisplay",
            "subcomponent": "settings-display",
            "aio_id": aio_id,
        }
        aggregation_settings = lambda aio_id, path, dataset, col: {
            "component": "SeriesDisplay",
            "subcomponent": "aggregation-settings",
            "aio_id": aio_id,
            "dataset": dataset,
            "path": path,
            "col": col,
        }
        groupby_settings = lambda aio_id, path, dataset, col: {
            "component": "SeriesDisplay",
            "subcomponent": "groupby-settings",
            "aio_id": aio_id,
            "dataset": dataset,
            "path": path,
            "col": col,
        }

    # Make the ids class a public class
    ids = ids

    def __init__(self, datasets: dict[str, DatasetConfig], aio_id: None | str = None):
        """
        Expects the datasets.
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
        def update_display(data: list[dict[str, str]], current_state: list[dict]):
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
                    src = DataSource(path, data_src.index_col, data_src.index_pattern)
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
            values, ids, agg_ids, agg_settings, current_state, input_data
        ):
            # Callback that handles the global state of the component
            curr_state: list[SeriesSetting] = []

            # This filters out data in the current state that are not present in the input and adds new
            # files to the state
            for item in input_data:
                for data in current_state:
                    if isinstance(data, dict) != True:
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
            if (triggered is None) or isinstance(triggered, dict) != True:
                return [item.dict() for item in curr_state]

            path = triggered.get("path")
            dataset = triggered.get("dataset")
            col = triggered.get("col")

            # Handles groupby settings
            for id, item in zip(ids, values):
                if id == ctx.triggered_id:
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
            for id, item in zip(agg_ids, agg_settings):
                if id == ctx.triggered_id:
                    for idx, val in enumerate(curr_state):
                        if (
                            (val.col == col)
                            and (val.dataset == dataset)
                            and (val.path == path)
                        ):
                            curr_state[idx].aggregation = item
                            break

            return [item.dict() for item in curr_state]
