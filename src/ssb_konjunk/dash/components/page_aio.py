import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import pandas as pd
from dash import Input
from dash import Output
from dash import State
from dash import callback
from dash import html
from ssb_dash_components.Dropdown import Dropdown

from .figure import generate_fig
from .file_switcher import FileSwitcher
from .header import header
from .table import generate_custom_table
from .table_card import generate_table_card


@dataclass
class ReturnData:
    """Dataclass to define what data the page component expects from getter functions.

    Choosen to improve readability for type hints. Also allows for validation implementation if needed.
    """

    header_1: list[str]
    header_2: list[str]
    res_data: pd.DataFrame
    figure_data: pd.Series | None
    sparkline_data: pd.DataFrame | None
    indirect: float | int | None


@dataclass
class Tables:
    """Dataclass to define how a table and graph should be constructed."""

    getter_function: Callable[[str | None, str | None], ReturnData]
    table_header: str
    figure_header: str


# All-in-One Components should be suffixed with 'AIO'
class AnalyticsPageAIO(html.Div):  # html.Div will be the "parent" component
    """All in on component to define how a page should look like.

    Since most pages have a similar layout, the layout is abstracted into this component to
    reduce the amount of boilerplate code needed for each new table.
    """

    class AIOids:
        """The ids of the components."""

        dropdown: Callable[..., dict[str, Any]] = lambda aio_id: {
            "component": "TableAIO",
            "subcomponent": "dropdown",
            "aio_id": aio_id,
        }

        dropdown_container: Callable[..., dict[str, Any]] = lambda aio_id: {
            "component": "TableAIO",
            "subcomponent": "dropdown_container",
            "aio_id": aio_id,
        }

        table_container: Callable[..., dict[str, Any]] = lambda aio_id: {
            "component": "TableAIO",
            "subcomponent": "table_container",
            "aio_id": aio_id,
        }

    # Make the ids class a public class
    ids = AIOids

    # Define the arguments of the All-in-One component
    def __init__(
        self,
        component_header: str,
        table_getters: list[Tables],
        get_dropdown_data_function: Callable[[str | None], list[dict[str, str]]],
        aio_id: None | str = None,
    ) -> None:
        """All in one component to define the layout of each table page.

        The All-in-One component dictionary IDs are available as
        - AnalyticsPageAIO.ids.dropdown(aio_id)
        - AnalyticsPageAIO.ids.table_container(aio_id)

        Params:
            component_header: str, The header of the component
            table_getters: list[Tables], dataclass with additional information for each table. See class Tables for additional information
            get_dropdown_data_function: Callable, function to the data for the dropdown list. Se callback signature. Should return a list of dicts like [{id: str, title: str}]
            aio_id: str, id of the component. If set it must be unique.

        Returns:
            html.Div - With the defined layout
        """
        if aio_id is None:
            aio_id = str(uuid.uuid4())

        super().__init__(
            [
                header(component_header),
                html.Div(
                    [
                        Dropdown(
                            items=get_dropdown_data_function(),  # type: ignore
                            value=None,
                            id=self.ids.dropdown(aio_id),
                        )
                    ],
                    id=self.ids.dropdown_container(aio_id),
                    className="dropdown-container",
                ),
                html.Div([], id=self.ids.table_container(aio_id)),
            ],
            className="main-layout",
        )

        @callback(
            Output(self.ids.dropdown_container(aio_id), "children"),
            Input(FileSwitcher.ids.store, "data"),
            Input(self.ids.dropdown(aio_id), "value"),
        )
        def update_dropdown(prev_store: str, value: str):  # type: ignore
            options = get_dropdown_data_function(prev_store)
            if (value is None) or (value == "None"):
                value = options[0]["id"]
            elif value not in [x["id"] for x in options]:
                value = options[0]["id"]
            else:
                value = value
            return Dropdown(items=options, id=self.ids.dropdown(aio_id), value=value)

        @callback(
            Output(self.ids.table_container(aio_id), "children"),
            Input(self.ids.dropdown(aio_id), "value"),
            State(FileSwitcher.ids.store, "data"),
        )
        def update_table(value: str, prev_store: str):  # type: ignore
            all_tables: list[html.Div] = []
            if (value is None) or (len(value) == 0):
                return all_tables
            for item in table_getters:
                table = item.getter_function(value, prev_store)
                if table.figure_data is not None:
                    fig = generate_fig(
                        item.figure_header,
                        table.figure_data,
                        table.figure_data.index,
                    )
                else:
                    fig = None
                generated = html.Div(
                    children=[
                        generate_table_card(
                            generate_custom_table(
                                item.table_header,
                                table.res_data,
                                table.header_1,
                                table.header_2,
                                table.sparkline_data,
                            ),
                            fig=fig,
                            indirect_num=table.indirect,
                        )
                    ]
                )
                all_tables.append(generated)
            return all_tables
