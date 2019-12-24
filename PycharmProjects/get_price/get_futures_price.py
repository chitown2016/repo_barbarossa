__author__ = 'kocat_000'

import sys
sys.path.append(r'C:\Users\kocat_000\quantFinance\PycharmProjects')
import my_sql_routines.my_sql_utilities as msu
import contract_utilities.contract_meta_info as cmi
import shared.directory_names_aux as dna
import pandas as pd
import datetime as dt
import shared.calendar_utilities as cu
import os.path


def get_futures_prices_4date(**kwargs):

    date_to = kwargs['date_to']

    filter_string = 'WHERE dp.price_date=' + str(date_to)

    if 'date_from' in kwargs.keys():
        filter_string = 'WHERE dp.price_date<=' + str(date_to) + ' and dp.price_date>=' + str(kwargs['date_from'])

    con = msu.get_my_sql_connection(**kwargs)

    sql_query = 'SELECT dp.price_date, sym.ticker, dp.ticker_head, dp.ticker_month, ' + \
    'sym.ticker_year, dp.cal_dte, dp.tr_dte, ' \
    'dp.open_price, dp.high_price, dp.low_price, dp.close_price, dp.volume ' + \
    'FROM symbol as sym ' + \
    'INNER JOIN daily_price as dp ON dp.symbol_id = sym.id ' + \
     filter_string + \
    ' ORDER BY dp.ticker_head, dp.cal_dte '

    cur = con.cursor()
    cur.execute(sql_query)
    data = cur.fetchall()

    if 'con' not in kwargs.keys():
        con.close()

    return pd.DataFrame(data,columns=['settle_date', 'ticker', 'ticker_head', 'ticker_month', 'ticker_year', 'cal_dte', 'tr_dte',
                                      'open_price','high_price','low_price','close_price', 'volume'])


def get_futures_price_4ticker(**kwargs):

    if 'ticker' in kwargs.keys():
        filter_string = 'WHERE sym.ticker=\'' + kwargs['ticker'] + '\''
    elif 'ticker_head' in kwargs.keys():
        filter_string = 'WHERE dp.ticker_head=\'' + kwargs['ticker_head'] + '\''

    con = msu.get_my_sql_connection(**kwargs)

    if 'tr_dte_min' in kwargs.keys():
        filter_string = filter_string + ' and dp.tr_dte>=' + str(kwargs['tr_dte_min'])

    if 'tr_dte_max' in kwargs.keys():
        filter_string = filter_string + ' and dp.tr_dte<=' + str(kwargs['tr_dte_max'])

    if 'ticker_month' in kwargs.keys():
        filter_string = filter_string + ' and dp.ticker_month=' + str(kwargs['ticker_month'])

    if 'date_from' in kwargs.keys():
        filter_string = filter_string + ' and dp.price_date>=' + str(kwargs['date_from'])

    if 'date_to' in kwargs.keys():
        filter_string = filter_string + ' and dp.price_date<=' + str(kwargs['date_to'])

    sql_query = 'SELECT dp.price_date, sym.ticker, dp.ticker_head, dp.ticker_month, ' + \
    'sym.ticker_year, dp.cal_dte, dp.tr_dte, ' \
    'dp.open_price, dp.high_price, dp.low_price, dp.close_price, dp.volume ' + \
    'FROM symbol as sym ' + \
    'INNER JOIN daily_price as dp ON dp.symbol_id = sym.id ' + \
    filter_string +  \
    ' ORDER BY dp.price_date, dp.cal_dte'

    cur = con.cursor()
    cur.execute(sql_query)
    data = cur.fetchall()

    if 'con' not in kwargs.keys():
        con.close()

    return pd.DataFrame(data,columns=['settle_date', 'ticker', 'ticker_head', 'ticker_month', 'ticker_year', 'cal_dte', 'tr_dte',
                                      'open_price','high_price','low_price','close_price', 'volume'])


def get_futures_last_price_date_4ticker(**kwargs):

    if 'ticker' in kwargs.keys():
        sql_query = 'SELECT max(dp.price_date) FROM symbol as sym INNER JOIN daily_price as dp ON dp.symbol_id = sym.id WHERE sym.ticker=\'' + kwargs['ticker'] + '\''
    elif 'ticker_head' in kwargs.keys():
        sql_query = 'SELECT max(price_date) FROM futures_master.daily_price WHERE ticker_head=\'' + kwargs['ticker_head'] + '\''

    con = msu.get_my_sql_connection(**kwargs)

    cur = con.cursor()
    cur.execute(sql_query)
    data = cur.fetchall()

    if 'con' not in kwargs.keys():
        con.close()
    return int(data[0][0].strftime('%Y%m%d')) if data[0][0] is not None else None


def get_futures_price_preloaded(**kwargs):

    if 'ticker_head' in kwargs.keys():
        ticker_head = kwargs['ticker_head']
    else:
        ticker = kwargs['ticker']
        contract_specs_output = cmi.get_contract_specs(ticker)
        ticker_head = contract_specs_output['ticker_head']
        file_ticker = cmi.mini_contract_dictionary.get(ticker_head, ticker_head) + contract_specs_output['ticker_month_str'] + str(contract_specs_output['ticker_year'])

    if 'futures_data_dictionary' in kwargs.keys():
        data_out = kwargs['futures_data_dictionary'][ticker_head]
    else:
        presaved_futures_data_folder = dna.get_directory_name(ext='presaved_futures_data')
        file_ticker_head = cmi.mini_contract_dictionary.get(ticker_head, ticker_head)
        if os.path.isfile(presaved_futures_data_folder + '/' + file_ticker_head + '.pkl'):
            data_out = pd.read_pickle(presaved_futures_data_folder + '/' + file_ticker_head + '.pkl')
        else:
            data_out = pd.DataFrame()
            return data_out

    if 'settle_date' in kwargs.keys():
        settle_date = kwargs['settle_date']
        if isinstance(settle_date,int):
            data_out = data_out[data_out['settle_date'] == cu.convert_doubledate_2datetime(settle_date)]
        elif isinstance(settle_date,dt.datetime):
            data_out = data_out[data_out['settle_date'] == settle_date]

    if 'settle_date_from_exclusive' in kwargs.keys():
        data_out = data_out[data_out['settle_date']>cu.convert_doubledate_2datetime(kwargs['settle_date_from_exclusive'])]

    if 'settle_date_from' in kwargs.keys():
        data_out = data_out[data_out['settle_date']>=cu.convert_doubledate_2datetime(kwargs['settle_date_from'])]

    if 'settle_date_to' in kwargs.keys():
        settle_date_to = kwargs['settle_date_to']
        if isinstance(settle_date_to, int):
            data_out = data_out[data_out['settle_date']<=cu.convert_doubledate_2datetime(kwargs['settle_date_to'])]
        elif isinstance(settle_date_to, dt.datetime):
            data_out = data_out[data_out['settle_date'] <= settle_date_to]

    if 'ticker' in kwargs.keys():
        data_out = data_out[data_out['ticker']==file_ticker]

    return data_out

