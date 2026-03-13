from plotly.graph_objects import Figure

from dash import dcc
from dash import html

from .card import Card
from .indirect import indirect


def generate_table_card(
    table: html.Div, fig: None | Figure, indirect_num: float | int | None
) -> html.Div:
    """Genererer et Dash-kort med en tabell og eventuelt en graf.

    Kortet inneholder alltid tabellen, og hvis `fig` er gitt,
    legges grafen til ved siden av tabellen. Hvis `indirect_num` er
    gitt, vises også et ekstra element under grafen.

    Args:
        table (html.Div): Dash-komponenten som representerer tabellen.
        fig (Figure | None): Plotly-figur som skal vises. Hvis None, vises kun tabellen.
        indirect_num (float | int | None): Valgfri verdi som brukes i `indirect`-komponenten.

    Returns:
        html.Div: Et Dash-kort (`Card`) som inneholder tabellen og eventuelt grafen.
    """
    if fig:
        if indirect_num:
            indirect_div = indirect(indirect_num)
        else:
            indirect_div = html.Div()
        return Card(
            html.Div(
                children=[
                    html.Div([table], className="column"),
                    html.Div(
                        [
                            html.Div(
                                children=[
                                    dcc.Graph(
                                        figure=fig,
                                        className="graph-class",
                                        responsive=True,
                                        config={
                                            "autosizable": True,
                                            "fillFrame": False,
                                        },
                                    ),
                                    indirect_div,
                                ],
                                className="graph-container",
                            )
                        ],
                        className="column",
                    ),
                ],
                className="row-container",
            )
        )
    else:
        return Card(
            html.Div(
                [
                    html.Div([table], className="column"),
                ],
                className="row-container",
            )
        )
