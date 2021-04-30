

import backtesting.time_series_backtester as tsb
import get_price.forex_data as fd
import numpy as np

# https://medium.com/geekculture/the-equilibrium-indicator-profiting-from-mean-reversion-ccecb647b988
# Equilibrium indicator with houly forex data

class Kaabar1(tsb.Strategy):

    comission_method = 'fixed'
    comission = 0.000025
    #comission = 0
    execution_price_field = 'close'
    stop_price = np.nan
    target_price = np.nan

    @classmethod
    def get_data_4strategy(cls, **kwargs):

        ticker_data = fd.get_presaved_forex_data(ticker=kwargs['ticker'], interval='1H')
        #ticker_data = ticker_data.iloc[:10000]

        ticker_data['ma5'] = ticker_data['close'].rolling(window=5, min_periods=5, center=False).mean()
        ticker_data['close_ma5'] = ticker_data['close'] - ticker_data['ma5']
        ticker_data['ei'] = ticker_data['close_ma5'].ewm(span=5, adjust=False).mean()

        ticker_data['close_1'] = ticker_data['close'].shift(1)
        ticker_data['aux1'] = ticker_data['high'] - ticker_data['low']
        ticker_data['aux2'] = abs(ticker_data['high'] - ticker_data['close_1'])
        ticker_data['aux3'] = abs(ticker_data['low'] - ticker_data['close_1'])

        ticker_data['aux4'] = ticker_data[['aux1', 'aux2', 'aux3']].max(axis=1)
        ticker_data['e_ATR'] = ticker_data['aux4'].ewm(span=10, adjust=False).mean()

        ticker_data['50ewm'] = ticker_data['close'].ewm(span=50, adjust=False).mean()
        ticker_data['200ewm'] = ticker_data['close'].ewm(span=200, adjust=False).mean()

        ticker_data['h'] = [x.hour for x in ticker_data.index]

        return {'ticker_data': ticker_data, 'num_length': len(ticker_data.index)}

    @classmethod
    def check_long_entry_condition(cls, **kwargs):

        i = kwargs['i']
        ticker_data = kwargs['data_input']['ticker_data']

        if (ticker_data['ei'].iloc[i] < -0.001) \
                and (ticker_data['ei'].iloc[i-1] > -0.001) \
                and (ticker_data['ei'].iloc[i-2] > -0.001):
            cls.stop_price = ticker_data['close'].iloc[i]-0.2*ticker_data['e_ATR'].iloc[i]
            cls.target_price = ticker_data['close'].iloc[i] + ticker_data['e_ATR'].iloc[i]
            return True
        else:
            return False

    @classmethod
    def check_short_entry_condition(cls, **kwargs):

        i = kwargs['i']
        ticker_data = kwargs['data_input']['ticker_data']

        if (ticker_data['ei'].iloc[i] > 0.001) \
                and (ticker_data['ei'].iloc[i-1] < 0.001) \
                and (ticker_data['ei'].iloc[i-2] < 0.001):
            cls.stop_price = ticker_data['close'].iloc[i] + 0.2*ticker_data['e_ATR'].iloc[i]
            cls.target_price = ticker_data['close'].iloc[i] - ticker_data['e_ATR'].iloc[i]
            return True
        else:
            return False

    @classmethod
    def check_long_exit_condition(cls, **kwargs):

        i = kwargs['i']
        ticker_data = kwargs['data_input']['ticker_data']

        if (ticker_data['ei'].iloc[i] > 0.001) \
                and (ticker_data['ei'].iloc[i-1] < 0.001) \
                and (ticker_data['ei'].iloc[i-2] < 0.001):
            return True
        else:
            return False

    @classmethod
    def check_short_exit_condition(cls, **kwargs):

        i = kwargs['i']
        ticker_data = kwargs['data_input']['ticker_data']

        if (ticker_data['ei'].iloc[i] < -0.001) \
                and (ticker_data['ei'].iloc[i - 1] > -0.001) \
                and (ticker_data['ei'].iloc[i - 2] > -0.001):
            return True
        else:
            return False

    @classmethod
    def check_long_target_exit_condition(cls, **kwargs):

        i = kwargs['i']
        ticker_data = kwargs['data_input']['ticker_data']

        if ticker_data['high'].iloc[i] > cls.target_price:
            return True
        else:
            return False

    @classmethod
    def check_long_target_stop_condition(cls, **kwargs):

        i = kwargs['i']
        ticker_data = kwargs['data_input']['ticker_data']

        if ticker_data['low'].iloc[i] < cls.stop_price:
            return True
        else:
            return False

    @classmethod
    def check_short_target_exit_condition(cls, **kwargs):

        i = kwargs['i']
        ticker_data = kwargs['data_input']['ticker_data']

        if ticker_data['low'].iloc[i] < cls.target_price:
            return True
        else:
            return False

    @classmethod
    def check_short_target_stop_condition(cls, **kwargs):

        i = kwargs['i']
        ticker_data = kwargs['data_input']['ticker_data']

        if ticker_data['high'].iloc[i] > cls.stop_price:
            return True
        else:
            return False


def get_results(**kwargs):

    return Kaabar1.collect_results4ticker(ticker='EURUSD')



