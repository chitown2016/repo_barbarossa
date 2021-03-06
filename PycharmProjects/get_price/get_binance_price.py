__author__ = 'kocat_000'

import dateparser
import pytz
import pandas as pd
import datetime as dt
import binance_trading.utils as btu
import shared.calendar_utilities as cu
import shared.directory_names as dn
import os.path
import time


def date_to_milliseconds(date_str):
    """Convert UTC date to milliseconds
    If using offset strings add "UTC" to date string e.g. "now UTC", "11 hours ago UTC"
    See dateparse docs for formats http://dateparser.readthedocs.io/en/latest/
    :param date_str: date in readable format, i.e. "January 01, 2018", "11 hours ago UTC", "now UTC"
    :type date_str: str
    """
    # get epoch value in UTC
    epoch = dt.datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc)
    # parse our date string
    d = dateparser.parse(date_str)
    # if the date is not timezone aware apply UTC timezone
    if d.tzinfo is None or d.tzinfo.utcoffset(d) is None:
        d = d.replace(tzinfo=pytz.utc)

    # return the difference in time
    return int((d - epoch).total_seconds() * 1000.0)

def interval_to_milliseconds(interval):
    """Convert a Binance interval string to milliseconds
    :param interval: Binance interval string 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w
    :type interval: str
    :return:
         None if unit not one of m, h, d or w
         None if string not in correct format
         int value of interval in milliseconds
    """
    ms = None
    seconds_per_unit = {
        "m": 60,
        "h": 60 * 60,
        "d": 24 * 60 * 60,
        "w": 7 * 24 * 60 * 60
    }

    unit = interval[-1]
    if unit in seconds_per_unit:
        try:
            ms = int(interval[:-1]) * seconds_per_unit[unit] * 1000
        except ValueError:
            pass
    return ms

def get_klines(**kwargs):
    """Get Historical Klines from Binance
    See dateparse docs for valid start and end string formats http://dateparser.readthedocs.io/en/latest/
    If using offset strings for dates add "UTC" to date string e.g. "now UTC", "11 hours ago UTC"
    :param symbol: Name of symbol pair e.g BNBBTC
    :type symbol: str
    :param interval: Biannce Kline interval
    :type interval: str
    :param start_str: Start date string in UTC format
    :type start_str: str
    :param end_str: optional - end date string in UTC format
    :type end_str: str
    :return: list of OHLCV values
    """
    # create the Binance client, no need for api key

    ticker = kwargs['ticker']
    interval = kwargs['interval']
    start_str = kwargs['start_str']

    if 'end_str' in kwargs.keys():
        end_str = kwargs['end_str']
    else:
        end_str=None

    if 'client' in kwargs.keys():
        client = kwargs['client']
    else:
        client = btu.get_binance_client()

    # init our list
    output_data = []

    # setup the max limit
    limit = 500

    # convert interval to useful value in seconds
    timeframe = interval_to_milliseconds(interval)

    # convert our date strings to milliseconds
    start_ts = date_to_milliseconds(start_str)

    # if an end time was passed convert it
    end_ts = None
    if end_str:
        end_ts = date_to_milliseconds(end_str)

    idx = 0
    # it can be difficult to know when a symbol was listed on Binance so allow start time to be before list date
    symbol_existed = False
    while True:
        # fetch the klines from start_ts up to max 500 entries or the end_ts if set
        temp_data = client.get_klines(
            symbol=ticker,
            interval=interval,
            limit=limit,
            startTime=start_ts,
            endTime=end_ts
        )

        # handle the case where our start date is before the symbol pair listed on Binance
        if not symbol_existed and len(temp_data):
            symbol_existed = True

        if len(temp_data)==0:
            break

        if symbol_existed:
            # append this loops data to our output data
            output_data += temp_data

            # update our start timestamp using the last value in the array and add the interval timeframe
            start_ts = temp_data[len(temp_data) - 1][0] + timeframe
        else:
            # it wasn't listed yet, increment our start date
            start_ts += timeframe

        idx += 1
        # check if we received less than the required limit and exit the loop
        if len(temp_data) < limit:
            # exit the while loop
            break

        # sleep after every 3rd call to be kind to the API
        if idx % 3 == 0:
            time.sleep(1)

    candle_frame = pd.DataFrame(output_data, columns=['openTime', 'open', 'high','low','close','volume','closeTime',
                                'quoteAssetVolume','numTrades','takerBuyBaseAssetVolume','takerBuyQuoteAssetVolume', 'ignore'])

    candle_frame['open'] = candle_frame['open'].astype('float')
    candle_frame['high'] = candle_frame['high'].astype('float')
    candle_frame['low'] = candle_frame['low'].astype('float')
    candle_frame['close'] = candle_frame['close'].astype('float')
    candle_frame['volume'] = candle_frame['volume'].astype('float')

    candle_frame['openDatetime'] = [dt.datetime.fromtimestamp(x/1000, tz=pytz.UTC) for x in candle_frame['openTime']]
    candle_frame['openDate'] = [x.date() for x in candle_frame['openDatetime']]
    return candle_frame.drop(['ignore'], axis=1, inplace=False)

def get_binance_price_preloaded(**kwargs):

    interval = kwargs['interval']
    ticker = kwargs['ticker']
    date_from = kwargs['date_from']
    date_to = kwargs['date_to']

    file_name = ticker + '_1h.pkl'

    datetime_from = cu.convert_doubledate_2datetime(date_from)
    datetime_to = cu.convert_doubledate_2datetime(date_to)

    x = datetime_from

    price_frame_list = []

    while x <= datetime_to:

        folder_name = dn.get_dated_directory_extension(folder_date=int(x.strftime('%Y%m%d')), ext='binance')
        dated_file_name = folder_name + '/' + file_name
        if os.path.isfile(dated_file_name):
            price_frame_list.append(pd.read_pickle(dated_file_name))

        x = x + dt.timedelta(days=1)

    if len(price_frame_list) == 0:
        return pd.DataFrame()
    merged_data = pd.concat(price_frame_list)

    if len(merged_data.index) == 0:
        return pd.DataFrame()

    merged_data.set_index('openDatetime', drop=True, inplace=True)

    if interval.upper() != '1H':
        data_out = pd.DataFrame()
        data_out['open'] = merged_data['open'].resample('4H').first()
        data_out['close'] = merged_data['close'].resample('4H').last()
        data_out['high'] = merged_data['high'].resample('4H').max()
        data_out['low'] = merged_data['low'].resample('4H').min()
        data_out['volume'] = merged_data['volume'].resample('4H').sum()
    else:
        data_out = merged_data

    return data_out









