
import pandas as pd
import numpy as np

class Strategy:

    @classmethod
    def collect_results4ticker(cls, **kwargs):

        data_out = cls.get_data_4strategy(**kwargs)

        results_frame = pd.DataFrame()
        if data_out['num_length'] == 0:
            return {'trades_frame': results_frame, 'price_data': data_out}

        current_position = 0
        entry_price_list = []
        exit_price_list = []
        entry_index_list = []
        exit_index_list = []
        quantity_list = []

        for i in range(data_out['num_length']):

            if cls.check_long_entry_condition(i=i, data_input=data_out) and (current_position == 0):
                current_position = 1
                entry_price_list.append(cls.get_execution_price(i=i, data_input=data_out, direction=1,
                                                                commission_method=cls.comission_method,
                                                                commission=cls.comission,
                                                                price_field=cls.execution_price_field))
                entry_index_list.append(i)
                quantity_list.append(cls.get_quantity(i=1, data_input=data_out, direction=1))
            elif cls.check_short_entry_condition(i=i, data_input=data_out) and (current_position == 0):
                current_position = -1
                entry_price_list.append(cls.get_execution_price(i=i, data_input=data_out, direction=-1,
                                                                commission_method=cls.comission_method,
                                                                commission=cls.comission,
                                                                price_field=cls.execution_price_field))
                entry_index_list.append(i)
                quantity_list.append(cls.get_quantity(i=1, data_input=data_out, direction=-1))
            elif cls.check_long_exit_condition(i=i, data_input=data_out) and (current_position == 1):
                current_position = 0
                exit_price_list.append(cls.get_execution_price(i=i, data_input=data_out, direction=-1,
                                                               commission_method=cls.comission_method,
                                                               commission=cls.comission,
                                                               price_field=cls.execution_price_field))
                exit_index_list.append(i)
            elif cls.check_short_exit_condition(i=i, data_input=data_out) and (current_position == -1):
                current_position = 0
                exit_price_list.append(cls.get_execution_price(i=i, data_input=data_out, direction=1,
                                                               commission_method=cls.comission_method,
                                                               commission=cls.comission,
                                                               price_field=cls.execution_price_field))
                exit_index_list.append(i)

        if current_position != 0:
            exit_price_list.append(
                cls.get_execution_price(i=-1, data_input=data_out, direction=-np.sign(quantity_list[-1]),
                                        commission_method=cls.comission_method,
                                        commission=cls.comission,
                                        price_field=cls.execution_price_field))
            exit_index_list.append(data_out['num_length']-1)

        results_frame = pd.DataFrame()
        results_frame['entry_price'] = entry_price_list
        results_frame['exit_price'] = exit_price_list
        results_frame['entry_index'] = entry_index_list
        results_frame['exit_index'] = exit_index_list
        results_frame['quantity'] = quantity_list
        results_frame['pnl_percent'] = np.sign(results_frame['quantity']) * \
                                       100 * (results_frame['exit_price'] - results_frame['entry_price']) / \
                                       results_frame['entry_price']
        results_frame['ticker'] = kwargs['ticker']
        for i in range(len(cls.variables_2record)):
            results_frame[cls.variables_2record[i]] = data_out['ticker_data'][cls.variables_2record[i]].iloc[results_frame.entry_index].values
        results_frame['entry_date'] = data_out['ticker_data'].index[results_frame.entry_index].values

        return {'trades_frame': results_frame, 'price_data': data_out}

    @classmethod
    def get_trade_instrument_data(cls, **kwargs):

        i = kwargs['i']
        ticker_data = kwargs['data_input']['ticker_data']
        field = kwargs['field']
        return ticker_data[field].iloc[i]

    @classmethod
    def get_execution_price(cls, **kwargs):

        i = kwargs['i']
        raw_price = cls.get_trade_instrument_data(i=i, data_input=kwargs['data_input'], field=kwargs['price_field'])
        direction = kwargs['direction']

        if 'commission_method' in kwargs.keys():
            commission_method = kwargs['commission_method']
        else:
            commission_method = 'percent'

        if 'commission' in kwargs.keys():
            commission = kwargs['commission']
        else:
            commission = 0

        if direction > 0:
            if commission_method == 'percent':
                execution_price = raw_price * (1 + (commission / 100))
            else:
                execution_price = raw_price
        elif direction < 0:
            if commission_method == 'percent':
                execution_price = raw_price * (1 - (commission / 100))
            else:
                execution_price = raw_price

        return execution_price

    @classmethod
    def get_quantity(cls, **kwargs):

        return kwargs['direction']

    @classmethod
    def get_date_column(cls, **kwargs):

        return kwargs['data_input']['ticker_data'].index

    @classmethod
    def check_long_entry_condition(cls, **kwargs):
        return False

    @classmethod
    def check_long_exit_condition(cls, **kwargs):
        return False

    @classmethod
    def check_short_entry_condition(cls, **kwargs):
        return False

    @classmethod
    def check_short_exit_condition(cls, **kwargs):
        return False

    @classmethod
    def get_backtest_stats(cls, **kwargs):

        results4ticker = cls.collect_results4ticker(**kwargs)
        trades_frame = results4ticker['trades_frame']

        if len(trades_frame.index) == 0:
            return {'total_pnl': 0, 'max_dd': 0, 'num_trades': 0}

        trades_frame['cum_portfolio'] = trades_frame['pnl_percent'].cumsum()
        trades_frame['portfolio_rolling_max'] = trades_frame['cum_portfolio'].rolling(window=len(trades_frame.index),
                                                                                      min_periods=1).max()
        trades_frame['drawdown'] = trades_frame['portfolio_rolling_max'] - trades_frame['cum_portfolio']

        return {'total_pnl': trades_frame['pnl_percent'].sum(),
                'max_dd': trades_frame['drawdown'].max(),
                'num_trades': len(trades_frame.index)}


