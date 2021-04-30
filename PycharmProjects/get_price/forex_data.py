
import shared.directory_names_aux as dna
import os as os
import shared.utils as su
import pandas as pd
import datetime as dt

# The data is from forexite  available at https://forextester.com/data/datasources

def presave_forex_data4ticker(**kwargs):

    ticker = kwargs['ticker']

    input_directory = dna.get_directory_name(ext='raw_forex_data')
    output_directory = dna.get_directory_name(ext='forex_data')

    raw_data = su.read_text_file(file_name=input_directory + '/' + ticker + '.txt')

    open_list = []
    high_list = []
    low_list = []
    close_list = []
    datetime_list = []

    price_frame = pd.DataFrame()

    for i in range(1, len(raw_data)):
        split_out = raw_data[i].split(',')
        open_list.append(split_out[3])
        high_list.append(split_out[4])
        low_list.append(split_out[5])
        close_list.append(split_out[6])
        datetime_list.append(dt.datetime.strptime(split_out[1] + split_out[2], '%Y%m%d%H%M%S'))

    price_frame['datetime'] = datetime_list
    price_frame['open'] = open_list
    price_frame['high'] = high_list
    price_frame['low'] = low_list
    price_frame['close'] = close_list

    price_frame['open'] = price_frame['open'].astype(float)
    price_frame[['open','high','low','close']] = price_frame[['open','high','low','close']].apply(pd.to_numeric)

    price_frame.to_pickle(output_directory + '/' + ticker + '.pkl')

    ohlc_dict = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'}
    price_frame.set_index('datetime', inplace=True, drop=True)
    candles1H = price_frame.resample('1H').apply(ohlc_dict).dropna(how='any')
    candles1H.to_pickle(output_directory + '/' + ticker + '_1H.pkl')


def presave_all_forex_data():

    input_directory = dna.get_directory_name(ext='raw_forex_data')
    file_list = os.listdir(input_directory)

    for i in range(len(file_list)):
        split_out = file_list[i].split('.')
        ticker = split_out[0]
        presave_forex_data4ticker(ticker=ticker)


def get_presaved_forex_data(**kwargs):

    ticker = kwargs['ticker']
    data_directory = dna.get_directory_name(ext='forex_data')

    file_name = data_directory + '/' + ticker + '.pkl'

    if 'interval' in kwargs.keys():
        file_name = data_directory + '/' + ticker + '_' + kwargs['interval'] + '.pkl'

    data_out = pd.read_pickle(file_name)
    return data_out



