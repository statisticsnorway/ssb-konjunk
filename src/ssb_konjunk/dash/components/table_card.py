from dash import dcc
from dash import html
from plotly.graph_objects import Figure

from .card import Card
from .indirect import indirect


def generate_table_card(
    table: html.Div, fig: None | Figure, indirect_num: float | int | None
) -> html.Div:
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
