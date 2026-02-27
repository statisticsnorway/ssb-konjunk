import plotly.graph_objects as go


def generate_fig(title: str, x, y):
    #print('x', x)
    #print('y', y)
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


def generate_sparkline(data: list[int]):
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
