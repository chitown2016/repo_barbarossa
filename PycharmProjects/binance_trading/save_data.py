
import binance_trading.utils as btu
import get_price.get_binance_price as gbp
import shared.calendar_utilities as cu
import datetime as dt
import shared.directory_names as dn
import shared.directory_names_aux as dna
import pandas as pd
import os.path
import time as tm

def save_ticker_frame(**kwargs):

    if 'client' in kwargs.keys():
        client = kwargs['client']
    else:
        client = btu.get_binance_client()

    folder_date = cu.get_doubledate()

    folder_name = dn.get_dated_directory_extension(folder_date=folder_date,ext='binance')
    tickers = client.get_ticker()
    ticker_frame = pd.DataFrame(tickers)

    ticker_frame.to_pickle(folder_name + '/ticker_frame.pkl')


def save_price_data(**kwargs):

    if 'client' in kwargs.keys():
        client = kwargs['client']
    else:
        client = btu.get_binance_client()

    interval = kwargs['interval']
    ticker = kwargs['ticker']
    date_from = kwargs['date_from']
    date_to = kwargs['date_to']

    file_name = ticker + '_' + interval + '.pkl'

    datetime_from = cu.convert_doubledate_2datetime(date_from)
    datetime_to = cu.convert_doubledate_2datetime(date_to)

    x = datetime_from

    while x <= datetime_to:

        xplus = x + dt.timedelta(days=1)

        folder_name = dn.get_dated_directory_extension(folder_date=int(x.strftime('%Y%m%d')), ext='binance')
        dated_file_name = folder_name + '/' + file_name
        if os.path.isfile(dated_file_name):
            price_frame = pd.read_pickle(dated_file_name)
            print(len(price_frame.index))
        else:
            price_frame = gbp.get_klines(ticker=ticker, interval=interval, start_str=x.strftime('%m/%d/%y'),end_str=xplus.strftime('%m/%d/%y'), client=client)
            price_frame = price_frame[price_frame['openDate']==x.date()]
            price_frame.to_pickle(dated_file_name)
            tm.sleep(0.5)

        x = xplus


def save_daily_price_data4ticker(**kwargs):

    if 'client' in kwargs.keys():
        client = kwargs['client']
    else:
        client = btu.get_binance_client()

    ticker = kwargs['ticker']

    file_name = dna.get_directory_name(ext='binance') + '/daily/' + ticker + '.pkl'

    if os.path.isfile(file_name):
        old_frame = pd.read_pickle(file_name)
        new_frame = gbp.get_klines(ticker=ticker, interval='1d',start_str=(old_frame['openDatetime'].iloc[-1] - dt.timedelta(days=5)).strftime('%m/%d/%y'))
        new_frame['frame_indx'] = 1
        old_frame['frame_indx'] = 0
        merged_data = pd.concat([old_frame, new_frame], ignore_index=True)
        merged_data.sort_values(['openDate', 'frame_indx'], ascending=[True, False], inplace=True)

        merged_data.drop_duplicates(subset=['openDate'], keep='first', inplace=True)
        price_frame = merged_data.drop('frame_indx', 1, inplace=False)
    else:
        price_frame = gbp.get_klines(ticker=ticker, interval='1d', start_str='01/01/2017',client=client)

    price_frame.to_pickle(file_name)









