from ssb_dash_components.Glossary import Glossary

from dash import html


def indirect(number: float | int) -> html.Div:
    """Genererer en HTML-komponent som viser et indirekte tall med forklaring.

    Funksjonen lager en liten visuell komponent med teksten "Indirekte",
    en forklaring (tooltip) og selve tallet formatert med to desimaler.

    Args:
        number (float | int): Tallet som skal vises.

    Returns:
        html.Div: En Dash HTML-div som inneholder forklaring og tallet.
    """
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
