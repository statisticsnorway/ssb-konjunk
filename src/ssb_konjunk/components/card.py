from dash import html
from typing import Any

def Card(children: Any) -> html.Div: 
    return html.Div(children, className="card")