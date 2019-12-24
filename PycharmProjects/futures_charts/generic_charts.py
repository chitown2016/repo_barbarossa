import plotly.graph_objects as go
from plotly.subplots import make_subplots
from plotly import __version__
import cufflinks as cf
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
init_notebook_mode (connected=True)
cf.go_offline()

def get_candlestick_chart(**kwargs):

    data2plot = kwargs['data2plot']

    if 'open_price' in data2plot.columns:
        data2plot.rename(columns={'open_price':'open'},inplace=True)

    if 'high_price' in data2plot.columns:
        data2plot.rename(columns={'high_price':'high'},inplace=True)

    if 'low_price' in data2plot.columns:
        data2plot.rename(columns={'low_price':'low'},inplace=True)

    if 'close_price' in data2plot.columns:
        data2plot.rename(columns={'close_price':'close'},inplace=True)

    fig = make_subplots(rows=2, cols=1)
    fig.add_trace(go.Candlestick(x=data2plot.index,open=data2plot['open'],
                          high=data2plot['high'],
                          low=data2plot['low'],
                          close=data2plot['close']), row=1, col=1)
    fig.update(layout_xaxis_rangeslider_visible=False)
    if 'rsi_14' in data2plot.columns:
        fig.add_trace(go.Scatter(x=data2plot.index,y=data2plot['rsi_14']), row=2, col=1)
        fig.update(layout_xaxis_rangeslider_visible=False)
    fig.show()



