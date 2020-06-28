
import backtesting.time_series_backtester as tsb
import binance_trading.binance_backtest_utilities as bbu
import futures_charts.generic_charts as gc
import get_price.get_binance_price as gbp
import binance_trading.get_data as gd
import numpy as np
import pandas as pd

# MA 10-18 Crossover

class BinanceStrategy(tsb.Strategy):

    comission_method = 'percent'
    comission = 0.1
    execution_price_field = 'close'

    @classmethod
    def get_data_4strategy(cls, **kwargs):

        print(kwargs['ticker'])
        if 'bar_frequency' in kwargs.keys():
            bar_frequency = kwargs['bar_frequency']
        else:
            bar_frequency = 'weekly'

        if bar_frequency == 'weekly':
            ticker_data = gd.get_weekly_price_data4ticker(**kwargs)
            btc_data = gd.get_weekly_price_data4ticker(ticker='BTCUSDT', date_to=kwargs['date_to'])
            volume_window = 4
        elif bar_frequency == 'daily':
            ticker_data = gd.get_daily_price_data4ticker(**kwargs)
            if len(ticker_data.index) == 0:
                return {'ticker_data': ticker_data, 'btc_data': pd.DataFrame(), 'num_length': 0}
            btc_data = gd.get_daily_price_data4ticker(ticker='BTCUSDT', date_to=kwargs['date_to'])
            volume_window = 30
            ticker_data.index = ticker_data['openDatetime']
            btc_data.index = btc_data['openDatetime']
        elif bar_frequency in ['4H', '1H']:
            ticker_data = gbp.get_binance_price_preloaded(**kwargs, interval=bar_frequency)
            btc_data = gbp.get_binance_price_preloaded(ticker='BTCUSDT', date_to=kwargs['date_to'],
                                                       date_from=kwargs['date_from'], interval=bar_frequency)
            volume_window = 180

        if len(ticker_data.index) == 0:
            return {'ticker_data': ticker_data, 'btc_data': pd.DataFrame(), 'num_length': 0}

        ticker_data['ma10'] = ticker_data['open'].rolling(window=10, center=False).mean()
        ticker_data['ma18'] = ticker_data['open'].rolling(window=18, center=False).mean()
        ticker_data['dollar_volume'] = ticker_data['close']*ticker_data['volume']
        ticker_data['average_volume'] = ticker_data['dollar_volume'].rolling(window=volume_window, center=False).mean()
        ticker_data['average_volume'] = ticker_data['average_volume'].shift(1)

        btc_data['ma50'] = btc_data['open'].rolling(window=50, center=False).mean()

        common_index = btc_data.index.intersection(ticker_data.index)
        btc_data = btc_data.loc[common_index]
        ticker_data = ticker_data.loc[common_index]

        return {'ticker_data': ticker_data, 'btc_data': btc_data, 'num_length': len(ticker_data.index)}

    @classmethod
    def check_long_entry_condition(cls, **kwargs):

        i = kwargs['i']
        ticker_data = kwargs['data_input']['ticker_data']
        btc_data = kwargs['data_input']['btc_data']

        if (ticker_data['ma10'].iloc[i] > ticker_data['ma18'].iloc[i]) \
                and (ticker_data['ma10'].iloc[i - 1] <= ticker_data['ma18'].iloc[i - 1])\
                and (btc_data['ma50'].iloc[i] > btc_data['ma50'].iloc[i - 1]):
            return True
        else:
            return False

    @classmethod
    def check_long_exit_condition(cls, **kwargs):

        i = kwargs['i']
        ticker_data = kwargs['data_input']['ticker_data']

        if (ticker_data['ma10'].iloc[i] < ticker_data['ma18'].iloc[i]) \
                and (ticker_data['ma10'].iloc[i - 1] >= ticker_data['ma18'].iloc[i - 1]):
            return True
        else:
            return False

    @classmethod
    def plot_strategy_triggers(cls, **kwargs):
        data_out = cls.get_data_4strategy(**kwargs)
        ticker_data = data_out['ticker_data']
        ticker_data.reset_index(drop=True, inplace=True)
        gc.get_candlestick_chart(data2plot=ticker_data, main_panel_indicator_list=['ma10', 'ma18'])

    @classmethod
    def plot_trend_filter(cls,**kwargs):
        data_out = cls.get_data_4strategy(**kwargs)
        btc_data = data_out['btc_data']
        btc_data.reset_index(drop=True, inplace=True)
        gc.get_candlestick_chart(data2plot=btc_data, main_panel_indicator_list=['ma50'])


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






