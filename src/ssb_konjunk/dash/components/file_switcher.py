from typing import cast

from ssb_dash_components import Dropdown

from dash import Input
from dash import Output
from dash import callback
from dash import dcc
from dash import html


class FileSwitcher(html.Div):
    """Switches the files from the previous version and the newest version."""

    class AIOids:
        """The ids of the components."""

        store: str = "file-switcher-store"
        toogle: str = "file-switcher-toggle"
        switcher: str = "file-switcher"

    ids = AIOids

    def __init__(
        self,
        options: None | list[str] | list[dict] = None,
        aio_id: str | None = None,
        searchable: bool = False,
    ) -> None:
        """Initialize the FileSwitcher component.

        Args:
            aio_id: Optional id for the AIO component.
        """
        if options is None:
            options = [
                {"id": "old", "title": "Previous"},
                {"id": "latest", "title": "Latest"},
            ]

        if isinstance(options, list) and all(isinstance(item, str) for item in options):
            options = [{"id": item, "title": item} for item in options]

        assert isinstance(options, list) and all(
            isinstance(item, dict) for item in options
        )

        complete_options = cast(list[Dropdown.Items], options)
        select_dropdown = Dropdown(
            items=complete_options,
            id=self.ids.toogle,
            value="latest",
            description="Select the file to use",
            showDescription=True,
            searchable=searchable,
        )
        super().__init__(
            [select_dropdown, dcc.Store(id=self.ids.store)], id=self.ids.switcher
        )

        @callback(Output(self.ids.store, "data"), Input(self.ids.toogle, "value"))
        def switch_file(item: str):  # type: ignore
            return item
