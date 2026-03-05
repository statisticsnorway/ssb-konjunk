import uuid

import dash_bootstrap_components as dbc


def create_alert(title: str, alert_id: str | None = None) -> dbc.Alert:
    """Oppretter en Dash Alert-komponent med gitt tittel.

    Args:
        title (str): Teksten som vises i alerten.
        alert_id (str | None, optional): Valgfri ID for alert-komponenten. Defaults to None.

    Returns:
        dbc.Alert: En Dash Bootstrap Alert-komponent klar til bruk.
    """
    if alert_id is None:
        alert_id = str(uuid.uuid4())
    return dbc.Alert(
        title,
        id=alert_id,
        is_open=True,
        className="visualizer-alert",
        duration=4000,
        fade=True,
    )
