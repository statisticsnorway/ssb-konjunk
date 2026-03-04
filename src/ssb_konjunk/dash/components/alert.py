import uuid
from typing import Optional
import dash_bootstrap_components as dbc


def create_alert(title: str, id: Optional[str] = None):
    if id is None:
        id = str(uuid.uuid4())
    return dbc.Alert(
            title,
            id=id,
            is_open=True,
            className="visualizer-alert",
            duration=4000,
            fade=True,
        )
    