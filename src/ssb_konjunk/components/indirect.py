from dash import html

from ssb_dash_components.Glossary import Glossary


def indirect(number: float | int):
    return html.Div(
        html.Div(
            children=[
                Glossary(
                    children=[html.P("Indirekte", className="indirect-label")],
                    explanation="Summering av siste kolonne på laveste NACE-nivå",
                    iconType="Info",
                ),
                html.P("{:.2f}".format(number), className="main-number"),
            ],
            className="indirect-wrapper",
        ),
        className="indirect-container",
    )
