import pandas as pd
import plotly.graph_objects as go


def generate_fig(
    title: str, x: list[int] | pd.Series | pd.Index, y: list[int] | pd.Series | pd.Index
) -> go.Figure:
    """Genererer en horisontal stolpediagram-figur med gitt data og tittel.

    Args:
        title (str): Tittelen som vises på figuren.
        x (list[int] | pd.Series | pd.Index): Verdier på X-aksen.
        y (list[int] | pd.Series | pd.Index): Verdier på Y-aksen.

    Returns:
        go.Figure: En Plotly `Figure` med horisontale stolper.
    """
    layout = go.Layout(
        autosize=True,
        xaxis=go.layout.XAxis(linecolor="black", linewidth=1, mirror=True),
        yaxis=go.layout.YAxis(linecolor="black", linewidth=1, mirror=True),
        margin=go.layout.Margin(l=50, r=50, b=50, t=50, pad=4),
        template="simple_white",
        title=title,
    )
    fig = go.Figure(
        go.Bar(
            x=x,
            y=y,
            orientation="h",
            marker=dict(
                color="rgba(0, 130, 77, 0.6)",
                line=dict(color="rgba(0, 130, 77, 1.0)", width=3),
            ),
            hovertemplate="<b>%{y}</b> <br></br> <b>%{x}</b> <extra></extra>",
        ),
        layout=layout,
    )
    fig.update_layout(plot_bgcolor="rgba(255, 255, 255, .0)")
    return fig


def generate_sparkline(data: list[int]) -> go.Figure:
    """Genererer en liten sparkline-graf fra en liste med tall.

    Args:
        data (list[int]): Liste med tall som skal vises i sparklinen.

    Returns:
        go.Figure: En Plotly `Figure` som viser sparkline-grafen.
    """
    # Create a sparkline chart
    if len(data) > 6:
        data = data[-6:]

    marker_size = 3
    fig = go.Figure(
        go.Scatter(
            y=data,
            mode="lines+markers",
            line=dict(color="rgba(39, 66, 71, 0.6)", width=1),
            marker=dict(size=marker_size, color="rgba(39, 66, 71, 1)"),
            hovertemplate="<b>%{y}</b><extra></extra>",
        )
    )

    # Update layout to make it look like a sparkline
    fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=0),
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        height=30,
        width=100,
        plot_bgcolor="rgba(255, 255, 255, .0)",
    )
    return fig
