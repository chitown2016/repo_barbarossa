
import pandas as pd
import numpy as np
import pickle as pckl
import contract_utilities.contract_lists as cl
import ta.strategy as ts
import signals.intraday_futures_signals as ifs
import os.path


def get_tickers_4date(**kwargs):

    liquid_futures_frame = cl.get_liquid_outright_futures_frame(settle_date=kwargs['date_to'])
    liquid_futures_frame.reset_index(drop=True,inplace=True)
    return liquid_futures_frame[['ticker', 'ticker_head']]


def generate_candlestick_sheet_4date(**kwargs):

    date_to = kwargs['date_to']
    output_dir = ts.create_strategy_output_dir(strategy_class='candlestick', report_date=date_to)

    if os.path.isfile(output_dir + '/summary.pkl'):
        sheet_4date = pd.read_pickle(output_dir + '/summary.pkl')
        return {'sheet_4date': sheet_4date, 'success': True}

    sheet_4date = get_tickers_4date(**kwargs)

    num_tickers = len(sheet_4date.index)

    signals_output = [ifs.get_intraday_trend_signals(ticker=sheet_4date.iloc[x]['ticker'],date_to=date_to) for x in range(num_tickers)]
    sheet_4date['contract_noise'] = [x['contract_noise'] for x in signals_output]
    sheet_4date['daily_noise'] = [x['daily_noise'] for x in signals_output]
    sheet_4date['average_volume'] = [x['average_volume'] for x in signals_output]
    sheet_4date['ewma10_50_spread'] = [x['ewma10_50_spread'] for x in signals_output]
    sheet_4date['ewma20_100_spread'] = [x['ewma20_100_spread'] for x in signals_output]
    sheet_4date['intraday_mean5'] = [x['intraday_mean5'] for x in signals_output]
    sheet_4date['intraday_std5'] = [x['intraday_std5'] for x in signals_output]
    sheet_4date['intraday_mean2'] = [x['intraday_mean2'] for x in signals_output]
    sheet_4date['intraday_std2'] = [x['intraday_std2'] for x in signals_output]
    sheet_4date['intraday_mean1'] = [x['intraday_mean1'] for x in signals_output]
    sheet_4date['intraday_std1'] = [x['intraday_std1'] for x in signals_output]

    sheet_4date.to_pickle(output_dir + '/summary.pkl')

    cov_output = ifs.get_intraday_outright_covariance(**kwargs)

    with open(output_dir + '/cov.pkl', 'wb') as handle:
        pckl.dump(cov_output, handle)

    return {'sheet_4date': sheet_4date, 'cov_output': cov_output, 'success': True}



