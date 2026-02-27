from dash import html

from ssb_dash_components import Header


def header(title: str) -> html.Div:
    return html.Div(
        [
            Header(html.H1(title)),
        ],
        className="divider",
    )
