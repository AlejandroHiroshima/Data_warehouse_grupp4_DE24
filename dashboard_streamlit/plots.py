import plotly_express as px

def create_bar_chart(df, x_col, y_col, title, xaxis_title = None, yaxis_title = None):
    fig = px.bar(df, x = x_col, y= y_col)
    fig.update_layout(
        xaxis_title= f"<b>{xaxis_title}<b>",
        yaxis_title= f""
    )