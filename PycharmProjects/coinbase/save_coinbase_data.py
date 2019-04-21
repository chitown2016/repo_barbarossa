
import coinbase.utils as coin_util
import shared.calendar_utilities as cu
import shared.directory_names as dn
import datetime as dt
import os.path
import pandas as pd
import time
import pytz

def download_coinbase_data_4date(**kwargs):

    utc_date = kwargs['utc_date']
    print(utc_date)
    candlestick_minutes = kwargs['candlestick_minutes']
    ticker = kwargs['ticker']

    coinbase_data_dir = dn.get_dated_directory_extension(folder_date=utc_date, ext='coinbase_data')
    file_name = coinbase_data_dir + '/' + ticker + '_' + str(candlestick_minutes) + '.pkl'

    if os.path.isfile(file_name):
        return pd.read_pickle(file_name)

    if 'coin_client' in kwargs.keys():
        coin_client = kwargs['coin_client']
    else:
        coin_client = coin_util.get_coinbase_client()

    start_date = cu.convert_doubledate_2datetime(utc_date)
    start_date = start_date.replace(hour=0, minute=0)
    end_date = start_date + dt.timedelta(days=1)
    end_date = end_date.replace(hour=0,minute=5)

    date_raw = coin_client.get_product_historic_rates(ticker, granularity=candlestick_minutes*60, start=start_date, end=end_date)
    time.sleep(0.5)

    frame_out = pd.DataFrame(date_raw,columns=['time','low','high','open','close','volume'])

    frame_out['time'] = [dt.datetime.utcfromtimestamp(x).replace(tzinfo=pytz.utc)  for x in frame_out['time']]

    frame_out.sort_values(by='time',ascending=True,inplace=True)
    frame_out.reset_index(drop=True,inplace=True)
    frame_out.to_pickle(file_name)

    return frame_out

def download_coinbase_data_4ticker(**kwargs):

    date_to = kwargs["date_to"]
    num_days_back = kwargs["num_days_back"]

    datetime_to = cu.convert_doubledate_2datetime(date_to)
    date_list = [int((datetime_to - dt.timedelta(days=x)).strftime('%Y%m%d')) for x in range(0, num_days_back)]

    [download_coinbase_data_4date(**kwargs,utc_date=x) for x in date_list]

def get_presaved_coinbase_data(**kwargs):

    date_to = kwargs["date_to"]
    num_days_back = kwargs["num_days_back"]
    datetime_to = cu.convert_doubledate_2datetime(date_to)
    date_list = [int((datetime_to - dt.timedelta(days=x)).strftime('%Y%m%d')) for x in range(0, num_days_back)]
    data_list = []

    for i in range(len(date_list)):

        coinbase_data_dir = dn.get_dated_directory_extension(folder_date=date_list[i], ext='coinbase_data')
        data_list.append(pd.read_pickle(coinbase_data_dir + '/' + kwargs["ticker"] + '_' + str(kwargs["candlestick_minutes"]) + '.pkl'))

    frame_out = pd.concat(data_list)

    frame_out.drop_duplicates(subset=['time'], keep='first', inplace=True)
    frame_out.sort_values(by='time', ascending=True, inplace=True)
    return frame_out.reset_index(drop=True, inplace=False)






