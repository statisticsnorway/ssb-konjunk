from typing import Literal
import uuid

from dash import html, dcc, callback, Input, Output, State

from ssb_dash_components import Dropdown, Input as SSBInput

#TODO: There is a literal defined here that should be made to a type so
# it can be used in other components that uses that same typing

class GraphSettingsDisplay(html.Div):
    """
    The class handles its own global state.
    This is the state that is passed to components downstream
    """

    class ids:
        settings_store = lambda aio_id: {
            "component": "GraphSettingsDisplay",
            "subcomponent": "settings-store",
            "aio_id": aio_id,
        }
        frequency = lambda aio_id: {
            "component": "GraphSettingsDisplay",
            "subcomponent": "frequency",
            "aio_id": aio_id,
        }
        convert_dropdown = lambda aio_id: {
            "component": "GraphSettingsDisplay",
            "subcomponent": "convert-dropdown",
            "aio_id": aio_id,
        }
        base_year = lambda aio_id: {
            "component": "GraphSettingsDisplay",
            "subcomponent": "base-year",
            "aio_id": aio_id,
        }

    # Make the ids class a public class
    ids = ids

    def __init__(self, aio_id: None | str = None):
        """
        Can provide an aio_id if necessary.
        """
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
