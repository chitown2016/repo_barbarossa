__author__ = 'kocat_000'

import shared.statistics as stats
import pandas as pd
import numpy as np
import math as m
from dateutil.relativedelta import relativedelta
import shared.calendar_utilities as cu
import contract_utilities.expiration as exp
from pandas.tseries.offsets import CustomBusinessDay
import shared.constants as const
import get_price.get_futures_price as gfp
import contract_utilities.contract_meta_info as cmi

fixed_weight_future_spread_list = [['CL', 'HO'], ['CL', 'RB'],
                                   ['HO', 'CL'], ['RB', 'CL'],
                                   ['HO', 'RB', 'CL'],
                                   ['B', 'CL'], ['CL', 'B'],
                                   ['S', 'BO', 'SM'],
                                   ['C', 'W'], ['W', 'C'],
                                   ['W', 'KW'], ['KW', 'W']]


def calc_theo_spread_move_from_ratio_normalization(**kwargs):

    ratio_time_series = kwargs['ratio_time_series']
    starting_quantile = kwargs['starting_quantile']
    num_price = kwargs['num_price']
    den_price = kwargs['den_price']
    favorable_quantile_move_list = kwargs['favorable_quantile_move_list']

    ratio_target_list = [np.NAN]*len(favorable_quantile_move_list)

    if starting_quantile > 50:

        ratio_target_list = stats.get_number_from_quantile(y=ratio_time_series,
                                                       quantile_list=[starting_quantile-x for x in favorable_quantile_move_list])
    elif starting_quantile < 50:

        ratio_target_list = stats.get_number_from_quantile(y=ratio_time_series,
                                                       quantile_list=[starting_quantile+x for x in favorable_quantile_move_list])

    theo_spread_move_list = \
        [calc_spread_move_from_new_ratio(num_price=num_price,
                                         den_price=den_price,
                                         new_ratio=x)
         if np.isfinite(x) else np.NAN for x in ratio_target_list]

    return {'ratio_target_list': ratio_target_list, 'theo_spread_move_list': theo_spread_move_list}

def calc_theo_weighted_butterfly_move(**kwargs):

    price_time_series = kwargs['price_time_series']
    starting_quantile = kwargs['starting_quantile']
    weighted_butterfly_price = kwargs['weighted_butterfly_price']
    favorable_quantile_move_list = kwargs['favorable_quantile_move_list']

    price_target_list = [np.NAN] * len(favorable_quantile_move_list)

    if starting_quantile > 50:

        price_target_list = stats.get_number_from_quantile(y=price_time_series,
                                                       quantile_list=[starting_quantile-x for x in favorable_quantile_move_list])
    elif starting_quantile < 50:

        price_target_list = stats.get_number_from_quantile(y=price_time_series,
                                                       quantile_list=[starting_quantile+x for x in favorable_quantile_move_list])

    theo_butterfly_move_list = [x - weighted_butterfly_price for x in price_target_list]

    return {'price_target_list': price_target_list, 'theo_butterfly_move_list': theo_butterfly_move_list}

def calc_spread_move_from_new_ratio(**kwargs):

    equal_move = (kwargs['num_price']-kwargs['new_ratio']*kwargs['den_price'])/(1+kwargs['new_ratio'])
    return -2*equal_move


def get_signal_correlation(**kwargs):

    strategy_class = kwargs['strategy_class']
    signal_name = kwargs['signal_name']
    #print(signal_name)
    if strategy_class == 'futures_butterfly':
        if signal_name in ['Q', 'QF','QF3','QF4','QF5','QF6', 'z1', 'z2', 'z3', 'z4','z5','z6','z7','z8','z9','z10','z11','z12','z13']:
            correlation = -1
        elif signal_name == 'mom5':
            correlation = 1
    elif strategy_class == 'spread_carry':
        if signal_name in ['q', 'q5', 'q1']:
            correlation = -1
        elif signal_name in ['q_carry', 'reward_risk']:
            correlation = 1
    elif strategy_class == 'curve_pca':
        if signal_name == 'z':
            correlation = 1
    elif strategy_class == 'ifs':
        if signal_name in ['z2', 'z5', 'z6','z10']:
            correlation = -1
        elif signal_name in ['z1']:
            correlation = 1
    elif strategy_class == 'ics':
        if signal_name in ['z1', 'z2', 'z5', 'z6']:
            correlation = -1
    elif strategy_class == 'ocs':
        if signal_name in ['ts_slope5', 'ts_slope10', 'momentum5','momentum10','underlying_zscore']:
            correlation = -1
        elif signal_name in ['linear_deviation5', 'linear_deviation10','q_carry', 'q_carry_average',
                             'q_carry2','q_carry3','q_carry4','q_carry5','q_carry6',
                             'butterfly_q','butterfly_q2','butterfly_q3','butterfly_q4','butterfly_q5','butterfly_q6',
                             'butterfly_z1','butterfly_z2','butterfly_z3']:
            correlation = 1
    elif strategy_class == 'ibo':
        if signal_name in ['delta_60','delta_120','delta_180','ewma_spread','z1', 'z2', 'z5', 'z6']:
            correlation = -1
        elif signal_name in ['ewma10_50_spread', 'ewma20_100_spread','z1', 'z2', 'z5', 'z6']:
            correlation = 1
    elif strategy_class == 'ts':
        if signal_name in ['ma10_spread','ma20_spread','ma50_spread','ma100_spread','ts_slope5', 'ts_slope10', 'ts_slope20',
                           'ma10Hybrid_spread','ma20Hybrid_spread','morning_spread','NormCloseChange_15','NormCloseChange_60']:
            correlation = -1
        elif signal_name in ['linear_deviation5', 'linear_deviation10','linear_deviation20']:
            correlation = 1
    elif strategy_class == 'cot':
        if signal_name in ['change_10_norm', 'comm_net_change_1_normalized','regress_forecast1','regress_forecast2','regress_forecast3',
                           'svr_forecast1','svr_forecast2','vote1_instant','vote12_instant','vote13_instant']:
            correlation = 1
        elif signal_name in ['change_1_norm']:
            correlation = -1
    elif strategy_class == 'arma':
        if signal_name in ['normalized_forecast']:
            correlation = 1
    elif strategy_class == 'ofs':
        if signal_name in ['regress_forecast1Instant1','regress_forecast1Instant2','regress_forecast1Instant3','regress_forecast1Instant4',
                           'regress_forecast11','regress_forecast12','regress_forecast13','regress_forecast14',
                           'regress_forecast51','regress_forecast52','regress_forecast53','regress_forecast54',
                           'regress_forecast101','regress_forecast102','regress_forecast103','change_2Delta']:
            correlation = 1
        elif signal_name in ['change_1Normalized','change_5Normalized','change_10Normalized','change_20Normalized','change_40Normalized']:
            correlation = -1

    return correlation


def get_bus_dates_from_agg_method_and_contracts_back(**kwargs):

    ref_date = kwargs['ref_date']
    aggregation_method = kwargs['aggregation_method']
    contracts_back = kwargs['contracts_back']

    ref_datetime = cu.convert_doubledate_2datetime(ref_date)

    if aggregation_method == 12:
        cal_date_list = [ref_datetime - relativedelta(years=x) for x in range(1, contracts_back+1)]
    elif aggregation_method == 1:
        cal_date_list = [ref_datetime - relativedelta(months=x) for x in range(1, contracts_back+1)]

    bday_us = CustomBusinessDay(calendar=exp.get_calendar_4ticker_head(const.reference_tickerhead_4business_calendar))

    if 'shift_bus_days' in kwargs.keys():
        shift_bus_days = kwargs['shift_bus_days']
        if shift_bus_days >= 0:
            bus_date_list = [pd.date_range(x, periods=shift_bus_days+1, freq=bday_us)[shift_bus_days].to_pydatetime() for x in cal_date_list]
        elif shift_bus_days < 0:
            bus_date_list = [pd.date_range(start=x-relativedelta(days=(max(m.ceil(-shift_bus_days*7/5)+5, -shift_bus_days+5))), end=x, freq=bday_us)[shift_bus_days-1].to_pydatetime() for x in cal_date_list]
    else:
        bus_date_list = [pd.date_range(x, periods=1, freq=bday_us)[0].to_pydatetime() for x in cal_date_list]

    return bus_date_list


def get_tickers_from_agg_method_and_contracts_back(**kwargs):

    ticker = kwargs['ticker']
    aggregation_method = kwargs['aggregation_method']
    contracts_back = kwargs['contracts_back']

    contact_specs_out = cmi.get_contract_specs(ticker)

    ref_date = 10000*contact_specs_out['ticker_year']+100*contact_specs_out['ticker_month_num']+1
    ref_datetime = cu.convert_doubledate_2datetime(ref_date)

    if aggregation_method == 12:
        cal_date_list = [ref_datetime - relativedelta(years=x) for x in range(1, contracts_back+1)]
    elif aggregation_method == 1:
        cal_date_list = [ref_datetime - relativedelta(months=x) for x in range(1, contracts_back+1)]

    ticker_list = [contact_specs_out['ticker_head'] + cmi.full_letter_month_list[x.month-1] +
                   str(x.year) for x in cal_date_list]

    return ticker_list


def get_rolling_futures_price(**kwargs):

    if 'roll_tr_dte_aim' in kwargs.keys():
        roll_tr_dte_aim = kwargs['roll_tr_dte_aim']
    else:
        roll_tr_dte_aim = 50

    futures_dataframe = gfp.get_futures_price_preloaded(**kwargs)

    futures_dataframe = futures_dataframe[futures_dataframe['cal_dte'] < 270]
    futures_dataframe.sort_values(['cont_indx', 'settle_date'], ascending=[True,True], inplace=True)

    grouped = futures_dataframe.groupby('cont_indx')
    shifted = grouped.shift(1)

    futures_dataframe['log_return'] = np.log(futures_dataframe['close_price']/shifted['close_price'])

    futures_dataframe['tr_dte_diff'] = abs(roll_tr_dte_aim-futures_dataframe['tr_dte'])
    futures_dataframe.sort_values(['settle_date','tr_dte_diff'], ascending=[True,True], inplace=True)
    futures_dataframe.drop_duplicates('settle_date', inplace=True,keep='first')

    return futures_dataframe


def get_spread_weights_4contract_list(**kwargs):

    ticker_head_list = kwargs['ticker_head_list']

    if ticker_head_list in [['CL', 'HO'], ['CL', 'RB']]:
        portfolio_weights = [1, -1]
        spread_weights = [1, -42]
    elif ticker_head_list in [['HO', 'CL'], ['RB', 'CL']]:
        portfolio_weights = [1, -1]
        spread_weights = [42, -1]
    elif ticker_head_list == ['HO', 'RB', 'CL']:
        portfolio_weights = [1, 1, -2]
        spread_weights = [42, 42, -2]
    elif ticker_head_list in [['B', 'CL'], ['CL', 'B']]:
        portfolio_weights = [1, -1]
        spread_weights = [1, -1]
    elif ticker_head_list == ['S', 'BO', 'SM']:
        portfolio_weights = [1, -1, -1]
        spread_weights = [1, -11, -2.2]
    elif ticker_head_list in [['C', 'W'], ['W', 'C']]:
        portfolio_weights = [1, -1]
        spread_weights = [1, -1]
    elif ticker_head_list in [['W', 'KW'], ['KW', 'W']]:
        portfolio_weights = [1, -1]
        spread_weights = [1, -1]

    return {'portfolio_weights': portfolio_weights, 'spread_weights': spread_weights}
















