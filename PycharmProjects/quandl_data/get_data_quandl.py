__author__ = 'kocat_000'

import sys
sys.path.append(r'C:\Users\kocat_000\quantFinance\PycharmProjects\contract_utilities')
import contract_utilities.contract_meta_info as cmi
import shared.calendar_utilities as cu
import pandas as pd
import quandl as qndl

quandl_database = {'LN': 'CME',
                   'LC': 'CME',
                   'FC': 'CME',
                    'C': 'CME',
                    'S': 'CME',
                   'SM': 'CME',
                   'BO': 'CME',
                    'W': 'CME',
                   'KW': 'CME',
                   'SB': 'ICE',
                   'KC': 'ICE',
                   'CC': 'ICE',
                   'CT': 'ICE',
                   'OJ': 'ICE',
                   'CL': 'CME',
                   'B' : 'ICE',
                   'HO': 'CME',
                   'RB': 'CME',
                   'NG': 'CME',
                   'ED': 'CME',
                   'BP': 'CME',
                   'GC': 'CME',
                   'CD': 'CME',
                   'ES': 'CME',
                   'AD': 'CME',
                   'TY': 'CME',
                   'TU': 'CME',
                   'FV': 'CME',
                   'EC': 'CME',
                   'JY': 'CME',
                   'NQ': 'CME',
                   'US': 'CME',
                   'SI': 'CME'}

quandl_database_new = {'LN': 'SRF/CME',
                   'LC': 'SRF/CME',
                   'FC': 'SRF/CME',
                    'C': 'SRF/CME',
                    'S': 'SRF/CME',
                   'SM': 'SRF/CME',
                   'BO': 'SRF/CME',
                    'W': 'SRF/CME',
                   'KW': 'SRF/CME',
                   'SB': 'SRF/ICE',
                   'KC': 'SRF/ICE',
                   'CC': 'SRF/ICE',
                   'CT': 'SRF/ICE',
                   'OJ': 'SRF/ICE',
                   'CL': 'SRF/CME',
                   'B' : 'SRF/ICE',
                   'HO': 'SRF/CME',
                   'RB': 'SRF/CME',
                   'NG': 'SRF/CME',
                   'ED': 'SRF/CME',
                   'BP': 'SRF/CME',
                   'GC': 'SRF/CME',
                   'CD': 'SRF/CME',
                   'ES': 'SRF/CME',
                   'AD': 'SRF/CME',
                   'TY': 'SRF/CME',
                   'TU': 'SRF/CME',
                   'FV': 'SRF/CME',
                   'EC': 'SRF/CME',
                   'JY': 'SRF/CME',
                   'NQ': 'SRF/CME',
                   'US': 'SRF/CME',
                   'SI': 'SRF/CME'}

authtoken = "zwBtPkKDycmg5jmYvK_s"


def get_quandl_database_4ticker(ticker):
    ticker_head = cmi.get_contract_specs(ticker)['ticker_head']
    return quandl_database[ticker_head]


def get_data(**kwargs):

    quandl_input = {'authtoken': authtoken}

    if 'date_from' in kwargs.keys():
        date_from_string = cu.convert_datestring_format({'date_string': str(kwargs['date_from']),
                                                         'format_from': 'yyyymmdd', 'format_to' : 'yyyy-mm-dd'})
        quandl_input['trim_start'] = date_from_string

    if 'date_to' in kwargs.keys():
        date_to_string = cu.convert_datestring_format({'date_string': str(kwargs['date_to']),
                                                         'format_from': 'yyyymmdd', 'format_to' : 'yyyy-mm-dd'})
        quandl_input['trim_end'] = date_to_string

    try:
        data_out = qndl.get(kwargs['quandl_ticker'], **quandl_input)
        success = True
    except:
        print('Error Loading ' + kwargs['quandl_ticker'] + ': ' + str(sys.exc_info()[0]))
        success = False
        data_out = pd.DataFrame()

    return {'success': success, 'data_out': data_out}


def get_daily_historic_data_quandl(**kwargs):

    ticker = kwargs['ticker']
    quandl_database_4ticker = get_quandl_database_4ticker(ticker)
    kwargs['quandl_ticker'] = quandl_database_4ticker + '/' + ticker
    #kwargs['quandl_ticker'] = quandl_database_4ticker + '_' + ticker

    quandl_out = get_data(**kwargs)
    data_out = quandl_out['data_out']
    success = quandl_out['success']

    new_column_names = ['Open Interest' if x =='Prev. Day Open Interest' else x for x in data_out.columns]
    data_out.columns = new_column_names

    return {'success': success, 'data_out': data_out}



