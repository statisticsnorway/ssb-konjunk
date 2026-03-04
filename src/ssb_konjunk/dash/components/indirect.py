from dash import html
from ssb_dash_components.Glossary import Glossary


def indirect(number: float | int) -> html.Div:
    return html.Div(
        html.Div(
            children=[
                Glossary(
                    children=[html.P("Indirekte", className="indirect-label")],
                    explanation="Summering av siste kolonne på laveste NACE-nivå",
                    iconType="Info",
                ),
                html.P(f"{number:.2f}", className="main-number"),
            ],
            className="indirect-wrapper",
        ),
        className="indirect-container",
    )
