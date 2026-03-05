import pandas as pd
from dash import dcc
from dash import html

from .figure import generate_sparkline


def generate_custom_table(
    title: str,
    dataframe: pd.DataFrame,
    header_1: list[str] | None = None,
    header_2: list[str] | None = None,
    sparkline_data: pd.DataFrame | None = None,
    max_rows: int = 100,
    color_last: bool = True,
    indent_first: bool = True,
) -> html.Div:
    """Genererer en tilpasset HTML-tabell med valgfri overskrift og sparklines.

    Tabellrader bygges fra `dataframe`. Kan legge til ekstra graf (sparkline)
    per rad hvis `sparkline_data` er gitt. Støtter formatering som innrykk
    på første kolonne og farging av siste kolonne.

    Args:
        title (str): Tittel som vises over tabellen.
        dataframe (pd.DataFrame): Data som fyller tabellen.
        header_1 (list[str] | None, optional): Første overskriftsrad. Defaults to None.
        header_2 (list[str] | None, optional): Andre overskriftsrad. Defaults to None.
        sparkline_data (pd.DataFrame | None, optional): Data for sparklines per rad. Defaults to None.
        max_rows (int, optional): Maksimalt antall rader som vises. Defaults to 100.
        color_last (bool, optional): Om siste kolonne skal ha grønn bakgrunn. Defaults to True.
        indent_first (bool, optional): Om første kolonne skal ha innrykk. Defaults to True.

    Returns:
        html.Div: En Dash HTML-div som inneholder tabellen med eventuelle sparklines.
    """
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
