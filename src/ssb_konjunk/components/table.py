from typing import Optional

from dash import dcc, html

import pandas as pd

from .figure import generate_sparkline


def generate_custom_table(
    title: str,
    dataframe: pd.DataFrame,
    header_1: Optional[list[str]] = None,
    header_2: Optional[list[str]] = None,
    sparkline_data: pd.DataFrame | None = None,
    max_rows=100,
    color_last=True,
    indent_first=True,
):

    rows = []
    for i in range(min(len(dataframe), max_rows)):
        row_data = dataframe.iloc[i]

        cells = []
        for j, col in enumerate(dataframe.columns):
            val = dataframe.iloc[i][col]
            style = {}
            if (j == 0) and indent_first:
                # Done to preserve whitespace and indenting
                style["textAlign"] = "left"
                style["whiteSpace"] = "pre-wrap"
            if (j == (len(dataframe.columns) - 1)) and color_last:
                style["backgroundColor"] = "rgba(0, 130, 77, 0.3)"
            cells.append(html.Td(val, style=style))

        if sparkline_data is not None:
            nar = row_data["nar"].split(" - ")[0].strip()
            sparkline_filtered = sparkline_data[sparkline_data["nar"] == nar]
            sparkline_filtered = sparkline_filtered.drop("nar", axis=1)
            sparkline_filtered = sparkline_filtered.values[0].tolist()
            cells.append(
                html.Td(
                    dcc.Graph(
                        figure=generate_sparkline(sparkline_filtered),
                        config={"displayModeBar": False},
                    )
                )
            )
        rows.append(html.Tr(cells))

    return html.Div(
        [
            html.P(title, className="small-header"),
            html.Table(
                [
                    (
                        html.Thead(html.Tr([html.Th(col) for col in header_1]))
                        if header_1 is not None
                        else None
                    ),
                    (
                        html.Thead(html.Tr([html.Th(col) for col in header_2]))
                        if header_2 is not None
                        else None
                    ),
                    html.Tbody(rows),
                ]
            ),
        ],
        className="table-container",
    )
