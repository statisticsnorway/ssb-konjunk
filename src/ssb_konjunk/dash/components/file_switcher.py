from dash import Input
from dash import Output
from dash import callback
from dash import dcc
from dash import html
from ssb_dash_components import Dropdown


class FileSwitcher(html.Div):
    """Switches the files from the previous version and the newest version."""

    class AIOids:
        """The ids of the components."""

        store: str = "file-switcher-store"
        toogle: str = "file-switcher-toggle"
        switcher: str = "file-switcher"

    ids = AIOids

    def __init__(self, aio_id: str | None = None) -> None:
        """Initialize the FileSwitcher component.

        Args:
            aio_id: Optional id for the AIO component.
        """
        select_dropdown = Dropdown(
            items=[
                {"id": "old", "title": "Previous"},
                {"id": "latest", "title": "Latest"},
            ],
            id=self.ids.toogle,
            value="latest",
            description="Select the file to use",
            showDescription=True,
        )
        super().__init__(
            [select_dropdown, dcc.Store(id=self.ids.store)], id=self.ids.switcher
        )

        @callback(Output(self.ids.store, "data"), Input(self.ids.toogle, "value"))
        def switch_file(item: str):  # type: ignore
            return item
