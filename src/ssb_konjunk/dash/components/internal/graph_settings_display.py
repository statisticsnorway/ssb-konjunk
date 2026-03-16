import uuid
from typing import Literal

from ssb_dash_components import Dropdown
from ssb_dash_components import Input as SSBInput

from dash import Input
from dash import Output
from dash import callback
from dash import dcc
from dash import html

# TODO: There is a literal defined here that should be made to a type so
# it can be used in other components that uses that same typing


class GraphSettingsDisplay(html.Div):
    """The class handles its own global state.
    
    This is the state that is passed to components downstream.
    """

    class ids:
        """Generates standardized IDs for the GraphSettingsDisplay component."""
        @staticmethod
        def settings_store(aio_id: str) -> dict:
            """ID for the settings store subcomponent."""
            return {
                "component": "GraphSettingsDisplay",
                "subcomponent": "settings-store",
                "aio_id": aio_id,
            }
    
        @staticmethod
        def frequency(aio_id: str) -> dict:
            """ID for the frequency subcomponent."""
            return {
                "component": "GraphSettingsDisplay",
                "subcomponent": "frequency",
                "aio_id": aio_id,
            }
    
        @staticmethod
        def convert_dropdown(aio_id: str) -> dict:
            """ID for the convert-dropdown subcomponent."""
            return {
                "component": "GraphSettingsDisplay",
                "subcomponent": "convert-dropdown",
                "aio_id": aio_id,
            }
    
        @staticmethod
        def base_year(aio_id: str) -> dict:
            """ID for the base-year subcomponent."""
            return {
                "component": "GraphSettingsDisplay",
                "subcomponent": "base-year",
                "aio_id": aio_id,
            }

    # Make the ids class a public class
    ids = ids

    def __init__(self, aio_id: None | str = None) -> None:
        """Can provide an aio_id if necessary."""
        if aio_id is None:
            aio_id = str(uuid.uuid4())

        super().__init__(
            children=[
                html.H3("Innstillinger"),
                dcc.Store(data={}, id=self.ids.settings_store(aio_id)),
                Dropdown(
                    id=self.ids.frequency(aio_id),
                    header="Velg frekvens",
                    items=[
                        {"title": "None", "id": "none"},
                        {"title": "Daily", "id": "daily"},
                        {"title": "Weekly", "id": "weekly"},
                        {"title": "Monthly", "id": "monthly"},
                    ],
                ),
                Dropdown(
                    id=self.ids.convert_dropdown(aio_id),
                    header="Velg konvertering",
                    items=[
                        {"title": "None", "id": "none"},
                        {
                            "title": "Discrete",
                            "id": "discrete",
                        },  # TODO: Add more options
                    ],
                ),
                SSBInput(
                    id=self.ids.base_year(aio_id),
                    label="Set base year",
                    type="month",
                ),
            ],
            style={
                "display": "flex",
                "flexDirection": "column",
                "gap": "10px",
            },
        )

        @callback(
            Output(self.ids.settings_store(aio_id), "data"),
            Input(self.ids.base_year(aio_id), "value"),
            Input(self.ids.convert_dropdown(aio_id), "value"),
            Input(self.ids.frequency(aio_id), "value"),
        )
        def update_store(
            base_year: str | None,
            convert: Literal["discrete", "none"],
            frequency: str | None,
        ):
            # Callback for handling input changes to the graph settings
            return {"base_year": base_year, "convert": convert, "frequency": frequency}
