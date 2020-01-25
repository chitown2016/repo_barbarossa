
import shared.directory_names_aux as dna
import shared.calendar_utilities as cu
import get_price.save_stock_data as ssd
import contract_utilities.expiration as exp
import pandas as pd
import os as os


def get_stock_price_preloaded(**kwargs):

    ticker = kwargs['ticker']

    if 'data_source' in kwargs.keys():
        data_source = kwargs['data_source']
    else:
        data_source = 'iex'

    if data_source == 'iex':
        file_dir = dna.get_directory_name(ext='iex_stock_data')
    else:
        file_dir = dna.get_directory_name(ext='stock_data')

    if 'stock_data_dictionary' in kwargs.keys():
        data_out = kwargs['stock_data_dictionary'][ticker]
    else:
        if not os.path.isfile(file_dir + '/' + ticker + '.pkl'):
            ssd.save_stock_data(symbol_list=[ticker],data_source=data_source)
        data_out = pd.read_pickle(file_dir + '/' + ticker + '.pkl')

    report_date = exp.doubledate_shift_bus_days()

    if cu.convert_doubledate_2datetime(report_date)>data_out['settle_datetime'].iloc[-1].to_pydatetime():
        ssd.save_stock_data(symbol_list=[ticker],data_source=data_source)
        data_out = pd.read_pickle(file_dir + '/' + ticker + '.pkl')

    if 'settle_date' in kwargs.keys():
        settle_date = kwargs['settle_date']
        if isinstance(settle_date,int):
            data_out = data_out[data_out['settle_datetime'] == cu.convert_doubledate_2datetime(settle_date)]
        elif isinstance(settle_date,dt.datetime):
            data_out = data_out[data_out['settle_datetime'] == settle_date]

    if 'settle_date_from' in kwargs.keys():
        data_out = data_out[data_out['settle_datetime']>=cu.convert_doubledate_2datetime(kwargs['settle_date_from'])]

    if 'settle_date_to' in kwargs.keys():
        data_out = data_out[data_out['settle_datetime']<=cu.convert_doubledate_2datetime(kwargs['settle_date_to'])]

    return data_out

