__author__ = 'kocat_000'

import sys
sys.path.append(r'C:\Users\kocat_000\quantFinance\PycharmProjects')
import contract_utilities.contract_lists as cl
import shared.calendar_utilities as cu
import contract_utilities.contract_meta_info as cmi
import get_price.get_futures_price as gfp
import pandas as pd
import numpy as np
import signals.futures_signals as fs
import os.path
import ta.strategy as ts


def get_futures_butterflies_4date(**kwargs):

    futures_dataframe = cl.generate_futures_list_dataframe(**kwargs)

    if 'volume_filter' in kwargs.keys():
        volume_filter = kwargs['volume_filter']
        futures_dataframe = futures_dataframe[futures_dataframe['volume']>volume_filter]

    futures_dataframe.reset_index(drop=True, inplace=True)

    unique_ticker_heads = cmi.futures_butterfly_strategy_tickerhead_list
    tuples = []

    for ticker_head_i in unique_ticker_heads:
        ticker_head_data = futures_dataframe[futures_dataframe['ticker_head'] == ticker_head_i]

        ticker_head_data.sort_values(['ticker_year','ticker_month'], ascending=[True, True], inplace=True)

        if len(ticker_head_data.index) >= 3:
            tuples = tuples + [(ticker_head_data.index[i-1], ticker_head_data.index[i],ticker_head_data.index[i+1]) for i in range(1, len(ticker_head_data.index)-1)]

        if len(ticker_head_data.index) >= 5:
            tuples = tuples + [(ticker_head_data.index[i-2], ticker_head_data.index[i],ticker_head_data.index[i+2]) for i in range(2, len(ticker_head_data.index)-2)]

        if len(ticker_head_data.index) >= 7:
            tuples = tuples + [(ticker_head_data.index[i-3], ticker_head_data.index[i],ticker_head_data.index[i+3]) for i in range(3, len(ticker_head_data.index)-3)]

    return pd.DataFrame([(futures_dataframe['ticker'][indx[0]],
    futures_dataframe['ticker'][indx[1]],
    futures_dataframe['ticker'][indx[2]],
    futures_dataframe['ticker_head'][indx[0]],
    futures_dataframe['ticker_class'][indx[0]],
    futures_dataframe['tr_dte'][indx[0]],
    futures_dataframe['tr_dte'][indx[1]],
    futures_dataframe['tr_dte'][indx[2]],
    futures_dataframe['multiplier'][indx[0]],
    futures_dataframe['aggregation_method'][indx[0]],
    futures_dataframe['contracts_back'][indx[0]]) for indx in tuples],
                        columns=['ticker1','ticker2','ticker3','tickerHead','tickerClass','trDte1','trDte2','trDte3','multiplier','agg','cBack'])


def generate_futures_butterfly_sheet_4date(**kwargs):

    date_to = kwargs['date_to']

    output_dir = ts.create_strategy_output_dir(strategy_class='futures_butterfly', report_date=date_to)

    if os.path.isfile(output_dir + '/summary.pkl'):
        butterflies = pd.read_pickle(output_dir + '/summary.pkl')
        return {'butterflies': butterflies,'success': True}

    if 'volume_filter' not in kwargs.keys():
        kwargs['volume_filter'] = 100

    butterflies = get_futures_butterflies_4date(**kwargs)

    butterflies = butterflies[butterflies['trDte1'] >= 35]
    #butterflies = butterflies.iloc[:5]

    butterflies.reset_index(drop=True,inplace=True)
    num_butterflies = len(butterflies)


    butterflies['QF'] = [np.nan]*num_butterflies
    butterflies['rr'] = [np.nan] * num_butterflies
    butterflies['z1'] = [np.nan]*num_butterflies
    butterflies['z2'] = [np.nan]*num_butterflies
    butterflies['bf_price'] = [np.nan]*num_butterflies
    butterflies['bfw_price'] = [np.nan] * num_butterflies
    butterflies['second_spread_weight'] = [np.nan]*num_butterflies
    butterflies['downside'] = [np.nan]*num_butterflies
    butterflies['upside'] = [np.nan]*num_butterflies
    butterflies['recent_5day_pnl'] = [np.nan]*num_butterflies


    futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in cmi.futures_butterfly_strategy_tickerhead_list}

    date5_years_ago = cu.doubledate_shift(date_to,5*365)
    datetime5_years_ago = cu.convert_doubledate_2datetime(date5_years_ago)

    for i in range(num_butterflies):
        bf_signals_output = fs.get_futures_butterfly_signals(ticker_list=[butterflies['ticker1'][i], butterflies['ticker2'][i], butterflies['ticker3'][i]],
                                          tr_dte_list=[butterflies['trDte1'][i], butterflies['trDte2'][i], butterflies['trDte3'][i]],
                                          aggregation_method=butterflies['agg'][i],
                                          contracts_back=butterflies['cBack'][i],
                                          date_to=date_to,
                                          futures_data_dictionary=futures_data_dictionary,
                                          contract_multiplier=butterflies['multiplier'][i],
                                          datetime5_years_ago=datetime5_years_ago)

        if not bf_signals_output['success']:
            continue

        butterflies.loc[i, 'QF'] = bf_signals_output['qf']
        butterflies.loc[i, 'rr'] = bf_signals_output['rr']
        butterflies.loc[i, 'z1'] = bf_signals_output['zscore1']
        butterflies.loc[i, 'z2'] = bf_signals_output['zscore2']
        butterflies.loc[i, 'bf_price'] = bf_signals_output['bf_price']
        butterflies.loc[i, 'bfw_price'] = bf_signals_output['bfw_price']
        butterflies.loc[i, 'second_spread_weight'] = bf_signals_output['second_spread_weight']
        butterflies.loc[i, 'downside'] = bf_signals_output['downside']
        butterflies.loc[i, 'upside'] = bf_signals_output['upside']
        butterflies.loc[i, 'recent_5day_pnl'] = bf_signals_output['recent_5day_pnl']


    butterflies['z1'] = butterflies['z1'].astype(float).round(2)
    butterflies['z2'] = butterflies['z2'].astype(float).round(2)
    butterflies['second_spread_weight'] = butterflies['second_spread_weight'].astype(float).round(2)
    butterflies['downside'] = butterflies['downside'].astype(float).round(3)
    butterflies['upside'] = butterflies['upside'].astype(float).round(3)
    butterflies['recent_5day_pnl'] = butterflies['recent_5day_pnl'].astype(float).round(3)


    butterflies.to_pickle(output_dir + '/summary.pkl')

    return {'butterflies': butterflies,'success': True}





