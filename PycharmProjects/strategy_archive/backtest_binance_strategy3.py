
import backtesting.time_series_backtester as tsb
import binance_trading.binance_backtest_utilities as bbu
import signals.technical_indicators as ti
import futures_charts.generic_charts as gc
import get_price.get_binance_price as gbp
import binance_trading.get_data as gd
import numpy as np
import pandas as pd

# Buy oversold coins


class BinanceStrategy(tsb.Strategy):

    comission_method = 'percent'
    comission = 0.1
    execution_price_field = 'close'
    variables_2record = ['average_volume', 'atr', 'trend_filter1', 'trend_filter2']
    percent_threshold = 5
    return_lookback = 4

    @classmethod
    def get_data_4strategy(cls, **kwargs):

        print(kwargs['ticker'])
        if 'bar_frequency' in kwargs.keys():
            bar_frequency = kwargs['bar_frequency']
        else:
            bar_frequency = 'weekly'

        if bar_frequency in ['4H', '1H']:
            ticker_data = gbp.get_binance_price_preloaded(**kwargs, interval=bar_frequency)
            volume_window = 42

        if len(ticker_data.index) == 0:
            return {'ticker_data': ticker_data, 'num_length': 0}

        ticker_data = ti.get_ewma(data_frame_input=ticker_data, period=8, field_name='open')
        ticker_data = ti.get_ewma(data_frame_input=ticker_data, period=75, field_name='open')

        ticker_data['dollar_volume'] = ticker_data['close']*ticker_data['volume']
        ticker_data['average_volume'] = ticker_data['dollar_volume'].rolling(window=volume_window, center=False).mean()
        ticker_data['average_volume'] = ticker_data['average_volume'].shift(1)

        ticker_data['ma50'] = ticker_data['close'].rolling(window=50, center=False, min_periods=40).mean()
        ticker_data['ma200'] = ticker_data['close'].rolling(window=200, center=False, min_periods=180).mean()

        ticker_data['trend_filter1'] = False
        ticker_data.loc[ticker_data['ma50'] > ticker_data['ma200'], 'trend_filter1'] = True

        ticker_data['ma300'] = ticker_data['close'].rolling(window=300, center=False, min_periods=250).mean()
        ticker_data['ma1200'] = ticker_data['close'].rolling(window=1200, center=False, min_periods=1100).mean()

        ticker_data['trend_filter2'] = False
        ticker_data.loc[ticker_data['ma300'] > ticker_data['ma1200'], 'trend_filter2'] = True

        ticker_data = ti.get_atr(data_frame_input=ticker_data, period=volume_window,percent_q=True)
        ticker_data['atr'] = ticker_data['atr_' + str(volume_window)].shift(1)

        return {'ticker_data': ticker_data, 'num_length': len(ticker_data.index)}

    @classmethod
    def check_long_entry_condition(cls, **kwargs):

        i = kwargs['i']
        ticker_data = kwargs['data_input']['ticker_data']

        if ticker_data.index[i].hour == 16:
            if 100*(ticker_data['close'].iloc[i] - ticker_data['close'].iloc[i-cls.return_lookback])/ticker_data['close'].iloc[i-cls.return_lookback]<-cls.percent_threshold:
                return True
            else:
                return False
        else:
            return False


    @classmethod
    def check_long_exit_condition(cls, **kwargs):

        i = kwargs['i']
        ticker_data = kwargs['data_input']['ticker_data']

        if ticker_data.index[i].hour == 8:
            return True

    @classmethod
    def plot_strategy_triggers(cls, **kwargs):
        data_out = cls.get_data_4strategy(**kwargs)
        ticker_data = data_out['ticker_data']
        ticker_data.reset_index(drop=True, inplace=True)
        gc.get_candlestick_chart(data2plot=ticker_data, main_panel_indicator_list=['ewma_8', 'ewma_75'])

def get_accumulated_results(**kwargs):

    quote_type = kwargs['quote_type']  # USDT or BTC

    symbol_list = bbu.get_symbol_list(quote_type=quote_type)
    trades_list = []

    kwargs2 = dict()
    if 'date_from' in kwargs.keys():
        kwargs2['date_from'] = kwargs['date_from']
    kwargs2['bar_frequency'] = kwargs['bar_frequency']

    for i in range(len(symbol_list)):
        output = BinanceStrategy.collect_results4ticker(**kwargs2, ticker=symbol_list[i], date_to=20200424)
        trades_list.append(output['trades_frame'])

    merged_frame = pd.concat(trades_list)

    return merged_frame






