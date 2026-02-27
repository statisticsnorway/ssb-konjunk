from dash import dcc, html, callback, Input, Output

from ssb_dash_components import Dropdown


class FileSwitcher(html.Div):

    class AIOids:
        store: str = "file-switcher-store"
        toogle: str = "file-switcher-toggle"
        switcher: str = "file-switcher"

    # Make the ids class a public class
    ids = AIOids

    def __init__(self, aio_id=None): # type: ignore

        select_dropdown = Dropdown(
            items=[
                {"id": "old", "title": "Previous"},
                {"id": "latest", "title": "Latest"},
            ],
            id=self.ids.toogle,
            value="latest",
            description="Select the file to use",
            showDescription= True
        )
        super().__init__([select_dropdown, dcc.Store(id=self.ids.store)], id=self.ids.switcher)

        @callback(
            Output(self.ids.store, "data"),
            Input(self.ids.toogle, "value")
        )
        def switch_file(item): # type: ignore
            return item