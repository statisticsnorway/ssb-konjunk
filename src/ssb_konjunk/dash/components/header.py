from ssb_dash_components import Header

from dash import html


def header(title: str) -> html.Div:
    """Genererer en HTML-header-komponent med gitt tittel.

    Args:
        title (str): Teksten som skal vises i headeren.

    Returns:
        html.Div: En Dash HTML-div som inneholder headeren.
    """
    return html.Div(
        [
            Header(html.H1(title)),
        ],
        className="divider",
    )
