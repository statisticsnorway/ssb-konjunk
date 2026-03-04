from dash import dcc
from dash import html
from dash import page_registry

from .file_switcher import FileSwitcher


def header_nav() -> html.Div:
    """Funksjon for å generer navigasjonsmenyen på toppen.

    Args:
        None

    Returns:
        html.Div

    """
    tables = []
    other = []
    for page in page_registry.values():
        if "Tabell" in page["name"]:
            tables.append(page)
        else:
            other.append(page)
    other.extend(tables)
    return html.Div(
        [
            html.Div(
                [
                    html.Div(
                        [
                            html.Div(
                                dcc.Link(
                                    f"{page['name']}",
                                    href=page["relative_path"],
                                    className="nav-item",
                                ),
                                className="nav-item-container",  # hover:text-gray-400
                            )
                            for page in other
                        ],
                        className="nav-row-container",
                    ),
                    html.Div(FileSwitcher()),
                ],
                className="nav-container",
            ),
        ]
    )
