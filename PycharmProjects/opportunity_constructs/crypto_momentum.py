

import binance_trading.get_data as bgd
import shared.calendar_utilities as cu
import ta.strategy as ts
import os.path
import numpy as np
import pandas as pd


def get_crypto_tickers_4date(**kwargs):

    date = kwargs['date']
    ticker_frame = bgd.get_ticker_frame(date=date)

    btc_index = [x[-3:] == 'BTC' for x in ticker_frame['symbol']]
    btc_index2 = ticker_frame['symbol'] == 'BTCUSDT'
    btc_pair_frame = ticker_frame[btc_index|btc_index2]
    btc_pair_frame = btc_pair_frame[['symbol']]
    return btc_pair_frame.reset_index(drop=True,inplace=False)


def get_crypto_momentum_frame(**kwargs):

    report_date = kwargs['report_date']
    report_datetime = cu.convert_doubledate_2datetime(report_date)

    output_dir = ts.create_strategy_output_dir(strategy_class='crypto_momentum', report_date=report_date)

    if os.path.isfile(output_dir + '/summary.pkl'):
        btc_pair_frame = pd.read_pickle(output_dir + '/summary.pkl')
        return {'btc_pair_frame': btc_pair_frame, 'success': True}

    btc_pair_frame = get_crypto_tickers_4date(date=20190325)

    close_price_list = []
    volume_list = []
    perChange_10_list = []
    perChange_30_list = []
    perChange_90_list = []
    perChange10_list = []
    perChange30_list = []

    for i in range(len(btc_pair_frame.index)):
        daily_data = bgd.get_daily_price_data4ticker(ticker=btc_pair_frame['symbol'].iloc[i])
        daily_data['close_10'] = daily_data['close'].shift(10)
        daily_data['close_30'] = daily_data['close'].shift(30)
        daily_data['close_90'] = daily_data['close'].shift(90)

        daily_data['close10'] = daily_data['close'].shift(-10)
        daily_data['close30'] = daily_data['close'].shift(-30)

        daily_data['perChange_10'] = 100*(daily_data['close']-daily_data['close_10'])/daily_data['close_10']
        daily_data['perChange_30'] = 100 * (daily_data['close'] - daily_data['close_30']) / daily_data['close_30']
        daily_data['perChange_90'] = 100 * (daily_data['close'] - daily_data['close_90']) / daily_data['close_90']

        daily_data['perChange10'] = 100 * (daily_data['close10']-daily_data['close'])/daily_data['close']
        daily_data['perChange30'] = 100 * (daily_data['close30']-daily_data['close'])/daily_data['close']


        daily_data = daily_data[daily_data['openDate'] == report_datetime.date()]

        if len(daily_data.index)==1:
            close_price_list.append(daily_data['close'].iloc[0])
            volume_list.append(daily_data['quoteAssetVolume'].iloc[0])
            perChange_10_list.append(daily_data['perChange_10'].iloc[0])
            perChange_30_list.append(daily_data['perChange_30'].iloc[0])
            perChange_90_list.append(daily_data['perChange_90'].iloc[0])
            perChange10_list.append(daily_data['perChange10'].iloc[0])
            perChange30_list.append(daily_data['perChange30'].iloc[0])
        else:
            close_price_list.append(np.nan)
            volume_list.append(np.nan)
            perChange_10_list.append(np.nan)
            perChange_30_list.append(np.nan)
            perChange_90_list.append(np.nan)
            perChange10_list.append(np.nan)
            perChange30_list.append(np.nan)

    btc_pair_frame['close'] = close_price_list
    btc_pair_frame['volume'] = volume_list
    btc_pair_frame['volume'] = btc_pair_frame['volume'].astype('float')
    btc_pair_frame['perChange_10'] = perChange_10_list
    btc_pair_frame['perChange_30'] = perChange_30_list
    btc_pair_frame['perChange_90'] = perChange_90_list
    btc_pair_frame['perChange10'] = perChange10_list
    btc_pair_frame['perChange30'] = perChange30_list
    btc_pair_frame['report_date'] = report_date

    btc_pair_frame.to_pickle(output_dir + '/summary.pkl')

    return {'btc_pair_frame': btc_pair_frame, 'success': True}



