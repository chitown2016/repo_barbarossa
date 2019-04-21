

import shared.directory_names as dn
import pandas as pd


def get_ticker_frame(**kwargs):

    date = kwargs['date']

    folder_name = dn.get_dated_directory_extension(folder_date=date, ext='binance')
    return pd.read_pickle(folder_name + '/ticker_frame.pkl')


def get_daily_price_data4ticker(**kwargs):

    return pd.read_pickle(dn.get_directory_name(ext='binance') + '/daily/' + kwargs['ticker'] + '.pkl')


