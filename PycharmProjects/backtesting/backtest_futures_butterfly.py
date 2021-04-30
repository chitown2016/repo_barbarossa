__author__ = 'kocat_000'


import pandas as pd
import shared.directory_names_aux as dna
import shared.calendar_utilities as cu
import contract_utilities.contract_meta_info as cmi
import get_price.get_futures_price as gfp
import opportunity_constructs.futures_butterfly as opfb
pd.options.mode.chained_assignment = None
import pickle as pick
import numpy as np
import os
import ta.strategy as ts
import signals.futures_filters as sf


def construct_futures_butterfly_portfolio(**kwargs):

    rule_no = kwargs['rule_no']
    backtest_output = kwargs['backtest_output']
    pnl_field = kwargs['pnl_field']

    if rule_no in [1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]:
        stop_loss = -100000000000
    elif rule_no == 2:
        stop_loss = -1000

    backtest_results_folder = dna.get_directory_name(ext='backtest_results')

    file_name = backtest_results_folder + '/futures_butterfly/portfolio' + str(rule_no) + '_' + pnl_field + '.pkl'

    if os.path.isfile(file_name):
        return pd.read_pickle(file_name)
    elif not os.path.exists(backtest_results_folder + '/futures_butterfly'):
        os.makedirs(backtest_results_folder + '/futures_butterfly')

    date_list = kwargs['date_list']
    ticker_head_list = cmi.futures_butterfly_strategy_tickerhead_list

    total_pnl_frame = pd.DataFrame({'report_date': date_list})
    total_pnl_frame['portfolio'] = 0

    for i in range(len(ticker_head_list)):
        total_pnl_frame[ticker_head_list[i]] = 0

    for i in range(len(date_list)):
        pnl_tickerhead_frame = pd.DataFrame({'ticker_head': ticker_head_list})
        pnl_tickerhead_frame['buy_mean_pnl'] = 0
        pnl_tickerhead_frame['sell_mean_pnl'] = 0
        pnl_tickerhead_frame['total_pnl'] = 0
        daily_sheet = backtest_output[i]

        for j in range(len(ticker_head_list)):

            ticker_head_results = daily_sheet[daily_sheet['tickerHead'] == ticker_head_list[j]]

            filter_output_long = sf.get_futures_butterfly_filters(data_frame_input=ticker_head_results, filter_list=['long'+str(rule_no)])
            filter_output_short = sf.get_futures_butterfly_filters(data_frame_input=ticker_head_results, filter_list=['short'+str(rule_no)])

            selected_short_trades = ticker_head_results[filter_output_short['selection_indx'] &
                                             (np.isfinite(ticker_head_results[pnl_field]))]

            selected_long_trades = ticker_head_results[filter_output_long['selection_indx'] &
                                           (np.isfinite(ticker_head_results[pnl_field]))]

            if len(selected_short_trades.index) > 0:
                selected_short_trades.loc[selected_short_trades['hold_pnl1short'] < stop_loss, pnl_field]= \
                    selected_short_trades.loc[selected_short_trades['hold_pnl1short'] < stop_loss, 'hold_pnl2short']

                pnl_tickerhead_frame['sell_mean_pnl'][j] = selected_short_trades[pnl_field].mean()

            if len(selected_long_trades.index) > 0:

                selected_long_trades.loc[selected_long_trades['hold_pnl1long'] <stop_loss, pnl_field] = \
                    selected_long_trades.loc[selected_long_trades['hold_pnl1long'] <stop_loss, 'hold_pnl2long']

                pnl_tickerhead_frame['buy_mean_pnl'][j] = selected_long_trades[pnl_field].mean()

            pnl_tickerhead_frame['total_pnl'][j] = pnl_tickerhead_frame['buy_mean_pnl'][j] + pnl_tickerhead_frame['sell_mean_pnl'][j]
            total_pnl_frame[ticker_head_list[j]][i] = pnl_tickerhead_frame['total_pnl'][j]

        total_pnl_frame['portfolio'][i] = pnl_tickerhead_frame['total_pnl'].sum()
    total_pnl_frame.to_pickle(file_name)

    return total_pnl_frame


def analyze_portfolio_contributors(**kwargs):
    date_list = kwargs['date_list']
    backtest_output = kwargs['backtest_output']
    rule_no = kwargs['rule_no']
    pnl_field =kwargs['pnl_field']

    ticker_head_list = cmi.futures_butterfly_strategy_tickerhead_list

    total_pnl_frame = construct_futures_butterfly_portfolio(date_list=date_list, rule_no=rule_no,
                                                            backtest_output=backtest_output,
                                                            pnl_field=pnl_field)

    if 'date_from' in kwargs.keys() and  'date_to' in kwargs.keys():
        frame_selected = total_pnl_frame[(total_pnl_frame['report_date']>=kwargs['date_from']) & (total_pnl_frame['report_date']<=kwargs['date_to'])]
    else:
        frame_selected = total_pnl_frame

    ticker_head_pnls = frame_selected[ticker_head_list].sum()

    ticker_head_total_pnls = pd.DataFrame({'ticker_head' : ticker_head_pnls.keys(),'pnl': ticker_head_pnls.values ,'abs_pnl' : np.absolute(ticker_head_pnls)})

    return {'total_pnl_frame': frame_selected, 'ticker_head_total_pnls': ticker_head_total_pnls.sort_values(by='abs_pnl', ascending=False)}

def get_individual_trade_path(**kwargs):

    report_date = kwargs['report_date']
    ticker_list = kwargs['ticker_list']
    weight = kwargs['weight']
    num_days2track = kwargs['num_days2track']
    data_list = []

    for j in range(3):
        ticker_frame = gfp.get_futures_price_preloaded(ticker=ticker_list[j],
                                                       settle_date_from=report_date)
        ticker_frame.set_index('settle_date', drop=False, inplace=True)
        data_list.append(ticker_frame)
    merged_data = pd.concat(data_list, axis=1, join='inner')

    summary_frame = pd.DataFrame()

    summary_frame['c1'] = merged_data['close_price'].iloc[:, 0]
    summary_frame['c2'] = merged_data['close_price'].iloc[:, 1]
    summary_frame['c3'] = merged_data['close_price'].iloc[:, 2]

    summary_frame['s1'] = summary_frame['c1'] - summary_frame['c2']
    summary_frame['s2'] = summary_frame['c2'] - summary_frame['c3']

    summary_frame['butterfly'] = summary_frame['s1']-weight*summary_frame['s2']
    summary_frame = summary_frame.iloc[:num_days2track]

    return summary_frame



















