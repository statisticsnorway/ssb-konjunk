from typing import Any

from dash import html


def Card(children: Any) -> html.Div:
    """Pakker gitt innhold inn i et Dash-kort.

    Args:
        children (Any): Dash-komponenter som skal vises inni kortet.

    Returns:
        html.Div: En Dash `Div` med klasse 'card' som inneholder `children`.
    """
    return html.Div(children, className="card")
