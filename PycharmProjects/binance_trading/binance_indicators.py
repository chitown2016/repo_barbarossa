

import binance_trading.get_data as gd
import shared.calendar_utilities as cu
import shared.statistics as stats
import pandas as pd
import numpy as np
pd.set_option('mode.chained_assignment', None)


def get_altcoin_demand(**kwargs):

    date_to = kwargs['date_to']
    num_days_back = kwargs['num_days_back']

    datetime_to = cu.convert_doubledate_2datetime(date_to)

    datetime_list = [cu.get_datetime_shift(reference_date=datetime_to, shift_in_days=x) for x in range(num_days_back)]
    datetime_list.reverse()

    date_list = [int(x.strftime('%Y%m%d')) for x in datetime_list]

    demand_list = []

    for i in range(len(date_list)):
        ticker_frame_out = gd.get_ticker_frame(date=date_list[i])
        btc_frame = ticker_frame_out.loc[[x[-3:] == 'BTC' for x in ticker_frame_out['symbol']]]
        btc_frame['quoteVolume'] = pd.to_numeric(btc_frame['quoteVolume'])
        btc_frame['priceChangePercent'] = pd.to_numeric(btc_frame['priceChangePercent'])
        btc_frame.sort_values(['quoteVolume'], inplace=True, ascending=False)
        btc_frame = btc_frame.iloc[:50]
        btc_frame['averageVolume'] = [get_average_volume(ticker=x, date_to=date_list[i]) for x in btc_frame['symbol']]
        btc_frame.sort_values(['averageVolume'], inplace=True, ascending=False)
        btc_frame = btc_frame.iloc[:10]
        demand_list.append(np.mean(btc_frame['priceChangePercent']))

    data_out = pd.DataFrame()
    data_out['date'] = date_list
    data_out['datetime'] = datetime_list
    data_out['demand'] = demand_list
    data_out['cumulative'] = data_out['demand'].cumsum()
    data_out['pydate'] = [x.date() for x in data_out['datetime']]

    return data_out


def get_coin_stats(**kwargs):

    ticker = kwargs['ticker']
    date_to = kwargs['date']

    data_out = gd.get_daily_price_data4ticker(ticker=ticker, date_to=date_to)
    data_out['quoteAssetVolume'] = pd.to_numeric(data_out['quoteAssetVolume'])
    data_out['close'] = pd.to_numeric(data_out['close'])
    data_out['pChange_1'] = 100 * data_out['close'].pct_change()

    if len(data_out.index) >= 30:
        average_volume = data_out['quoteAssetVolume'].iloc[-30:].mean()
    else:
        average_volume = np.nan

    if len(data_out.index) >= 7:
        std7 = np.std(data_out['pChange_1'].iloc[-7:])
    else:
        std7 = np.nan

    if len(data_out.index) >= 30:
        std30 = np.std(data_out['pChange_1'].iloc[-30:])
    else:
        std30 = np.nan

    if len(data_out.index) >= 8:
        pChange_7 = 100*(data_out['close'].iloc[-1] - data_out['close'].iloc[-8])/data_out['close'].iloc[-8]
    else:
        pChange_7 = np.nan

    if len(data_out.index) >= 31:
        pChange_30 = 100*(data_out['close'].iloc[-1]-data_out['close'].iloc[-31])/data_out['close'].iloc[-31]
    else:
        pChange_30 = np.nan

    if len(data_out.index) >= 61:
        pChange_60 = 100*(data_out['close'].iloc[-1]-data_out['close'].iloc[-61])/data_out['close'].iloc[-61]
    else:
        pChange_60 = np.nan

    if len(data_out.index) >= 91:
        pChange_90 = 100*(data_out['close'].iloc[-1]-data_out['close'].iloc[-91])/data_out['close'].iloc[-91]
    else:
        pChange_90 = np.nan

    if len(data_out.index) >= 200:
        data_out['ma150'] = data_out['close'].rolling(window=150, center=False).mean()
        data_out['ma150diff'] = data_out['close']-data_out['ma150']
        data_out['zscore'] = data_out['ma150diff']/np.std(data_out['ma150diff'])
        quantile_list = stats.get_number_from_quantile(y=data_out['zscore'].values,quantile_list=[20, 80])
        zscore_stop = quantile_list[0]
        zscore_target = quantile_list[1]
        zscore = data_out['zscore'].iloc[-1]
        if data_out['ma150'].iloc[-1]>data_out['ma150'].iloc[-40]:
            trend_direction1 = 1
        else:
            trend_direction1 = -1

        if max(data_out['high'].iloc[-60:]) == max(data_out['high'].iloc[-200:]):
            trend_direction2 = 1
        elif min(data_out['low'].iloc[-60:]) ==min (data_out['low'].iloc[-200:]):
            trend_direction2 = -1
        else:
            trend_direction2 = 0
    else:
        trend_direction1 = 0
        trend_direction2 = 0
        zscore = np.nan
        zscore_stop = np.nan
        zscore_target = np.nan

    if len(data_out.index) >= 60:
        entry_price = 1.005*max(data_out['high'].iloc[-7:])
        stop_price = 0.995*min(data_out['low'].iloc[-7:])
        recent_range = 100*(max(data_out['high'].iloc[-7:])-min(data_out['low'].iloc[-7:]))/min(data_out['low'].iloc[-7:])
        risk = recent_range+1
        target_price = 0.8*(max(data_out['high'].iloc[-90:])-max(data_out['high'].iloc[-7:]))+max(data_out['high'].iloc[-7:])
        reward = 100*(target_price-entry_price)/entry_price
        rr = reward/risk
    else:
        entry_price = np.nan
        target_price = np.nan
        stop_price = np.nan
        risk = np.nan
        reward = np.nan
        rr = np.nan
        
    return {'average_volume': average_volume,
            'std7': std7,
            'std30': std30,
            'pChange_7': pChange_7,
            'pChange_30': pChange_30,
            'pChange_60': pChange_60,
            'pChange_90': pChange_90,
            'trend_direction1': trend_direction1,
            'trend_direction2': trend_direction2,
            'zscore': zscore,
            'zscore_stop': zscore_stop,
            'zscore_target': zscore_target,
            'entry_price': entry_price,
            'target_price': target_price,
            'stop_price': stop_price,
            'risk': risk,
            'reward': reward,
            'rr': rr}







