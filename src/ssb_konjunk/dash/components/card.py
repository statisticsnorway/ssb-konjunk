from typing import Any

from dash import html


def Card(children: Any) -> html.Div:
    return html.Div(children, className="card")
