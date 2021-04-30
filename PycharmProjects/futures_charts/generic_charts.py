import plotly.graph_objects as go
from plotly.subplots import make_subplots
from plotly import __version__
import cufflinks as cf
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
init_notebook_mode (connected=True)
cf.go_offline()


def get_candlestick_chart(**kwargs):

    data2plot = kwargs['data2plot']

    if 'num_panels' in kwargs.keys():
        num_panels = kwargs['num_panels']
    else:
        num_panels = 1

    if 'open_price' in data2plot.columns:
        data2plot.rename(columns={'open_price': 'open'}, inplace=True)

    if 'high_price' in data2plot.columns:
        data2plot.rename(columns={'high_price': 'high'}, inplace=True)

    if 'low_price' in data2plot.columns:
        data2plot.rename(columns={'low_price': 'low'}, inplace=True)

    if 'close_price' in data2plot.columns:
        data2plot.rename(columns={'close_price': 'close'}, inplace=True)

    if num_panels>1:
        fig = make_subplots(rows=num_panels, cols=1,row_width=[0.2, 0.4])
    else:
        fig = make_subplots(rows=num_panels, cols=1)
    fig.add_trace(go.Candlestick(x=data2plot.index,open=data2plot['open'],
                          high=data2plot['high'],
                          low=data2plot['low'],
                          close=data2plot['close'],name='underlying'), row=1, col=1)
    fig.update(layout_xaxis_rangeslider_visible=False)

    if 'main_panel_indicator_list' in kwargs.keys():
        main_panel_indicator_list = kwargs['main_panel_indicator_list']
        color_list = ['black', 'purple', 'blue']
        for i in range(len(main_panel_indicator_list)):
            fig.add_trace(go.Scatter(x=data2plot.index, y=data2plot[main_panel_indicator_list[i]],
                                     line=dict(color=color_list[i]),
                                     name=main_panel_indicator_list[i]), row=1, col=1)

    if 'rsi_14' in data2plot.columns:
        fig.add_trace(go.Scatter(x=data2plot.index,y=data2plot['rsi_14']), row=2, col=1)
        fig.update(layout_xaxis_rangeslider_visible=False)

    if 'indicator_list' in kwargs.keys():
        indicator_list = kwargs['indicator_list']
        color_list = ['black', 'purple', 'blue']
        for i in range(len(indicator_list)):
            fig.add_trace(go.Scatter(x=data2plot.index, y=data2plot[indicator_list[i]],line=dict(color=color_list[i]),name=indicator_list[i]), row=2, col=1)
            fig.update(layout_xaxis_rangeslider_visible=False)

    fig['layout'].update(height=900, width=1000, title='Subplots with Shared X-Axes')
    fig.show()



