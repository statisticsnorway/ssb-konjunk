from dash import html
from typing import Any

def Card(children: Any): 
    return html.Div(children, className="card")