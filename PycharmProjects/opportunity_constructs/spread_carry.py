__author__ = 'kocat_000'

import warnings
with warnings.catch_warnings():
    warnings.filterwarnings("ignore",category=FutureWarning)
    import h5py

import get_price.get_futures_price as gfp
import signals.futures_signals as fs
import shared.calendar_utilities as cu
import ta.strategy as ts
import pandas as pd
import os.path

max_tr_dte_limits = {'LN': 300,
                     'LC': 300,
                     'FC': 300,
                     'C': 300,
                     'S': 300,
                     'SM': 300,
                     'BO': 300,
                     'W': 300,
                     'KW': 300,
                     'SB': 300,
                     'KC': 300,
                     'CC': 300,
                     'CT': 300,
                     'OJ': 300,
                     'CL': 300,
                     'B' : 300,
                     'HO': 300,
                     'RB': 300,
                     'NG': 300,
                     'ED': 750}


def get_spread_carry_4tickerhead(**kwargs):

    ticker_head = kwargs['ticker_head']
    report_date = kwargs['report_date']

    if 'min_tr_dte' in kwargs.keys():
        min_tr_dte = kwargs['min_tr_dte']
    else:
        min_tr_dte = 15

    if 'futures_data_dictionary' in kwargs.keys():
        futures_data_dictionary = kwargs['futures_data_dictionary']
    else:
        futures_data_dictionary = {ticker_head: gfp.get_futures_price_preloaded(ticker_head=ticker_head)}

    if 'datetime5_years_ago' in kwargs.keys():
        datetime5_years_ago = kwargs['datetime5_years_ago']
    else:
        date5_years_ago = cu.doubledate_shift(report_date,5*365)
        datetime5_years_ago = cu.convert_doubledate_2datetime(date5_years_ago)

    daily_data = gfp.get_futures_price_preloaded(ticker_head=ticker_head,
                                           settle_date=report_date,
                                           futures_data_dictionary=futures_data_dictionary)

    if ticker_head=='ED':
        daily_data = daily_data[(daily_data['tr_dte'] >= 50) & (daily_data['tr_dte'] <= max_tr_dte_limits[ticker_head])]
        daily_data = daily_data[daily_data.ticker_month % 3 == 0]
    else:
        daily_data = daily_data[(daily_data['tr_dte'] >= min_tr_dte) & (daily_data['tr_dte'] <= max_tr_dte_limits[ticker_head])]

    if len(daily_data.index) > 1:

        carry_signals = fs.get_futures_spread_carry_signals(ticker_list=daily_data['ticker'].values,
                                        tr_dte_list=daily_data['tr_dte'].values,
                                        futures_data_dictionary=futures_data_dictionary,
                                        datetime5_years_ago=datetime5_years_ago,
                                        date_to=report_date)

        return {'success': True, 'carry_signals': carry_signals}
    else:
        return {'success': False, 'carry_signals': pd.DataFrame()}


def generate_spread_carry_sheet_4date(**kwargs):

    report_date = kwargs['report_date']

    output_dir = ts.create_strategy_output_dir(strategy_class='spread_carry', report_date=report_date)

    if os.path.isfile(output_dir + '/summary.pkl'):
        spread_report = pd.read_pickle(output_dir + '/summary.pkl')
        return {'spread_report': spread_report,'success': True}

    if 'futures_data_dictionary' in kwargs.keys():
        futures_data_dictionary = kwargs['futures_data_dictionary']
    else:
        futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in max_tr_dte_limits.keys()}

    spread_list = [get_spread_carry_4tickerhead(ticker_head=x,report_date=report_date,futures_data_dictionary=futures_data_dictionary) for x in max_tr_dte_limits.keys()]

    success_list = [x['success'] for x in spread_list]
    carry_signals_list = [x['carry_signals'] for x in spread_list]

    spread_report = pd.concat([carry_signals_list[x] for x in range(len(spread_list)) if success_list[x]])

    spread_report['carry'] = spread_report['carry'].round(2)
    spread_report['q_carry_average'] = spread_report['q_carry_average'].round()
    spread_report['reward_risk'] = spread_report['reward_risk'].round(2)
    spread_report['upside'] = spread_report['upside'].round(2)
    spread_report['downside'] = spread_report['downside'].round(2)

    spread_report.rename(columns={'ticker_head': 'tickerHead'}, inplace=True)

    spread_report.to_pickle(output_dir + '/summary.pkl')

    return  {'spread_report': spread_report,'success': True}
