
import shared.directory_names as dn
import pandas as pd
import numpy as np
import contract_utilities.contract_meta_info as cmi
import read_exchange_files.read_cme_files as rcf
import read_exchange_files.cme_utilities as cmeu
import datetime as dt


def process_cme_futures_4tickerhead(**kwargs):

    ticker_head = kwargs['ticker_head']
    report_date = kwargs['report_date']

    ticker_class = cmi.ticker_class[ticker_head]

    name_type_output = cmeu.get_file_name_type_from_tickerclass(ticker_class, 'futures')
    file_name = name_type_output['file_name']
    file_type = name_type_output['file_type']

    settle_frame = pd.DataFrame()

    if file_type == 'txt':
        data_read_out = rcf.read_cme_settle_txt_files(file_name=file_name, report_date=report_date)

        title_frame = data_read_out['title_frame']
        settle_list = data_read_out['settle_list']
        open_list = data_read_out['open_list']
        high_list = data_read_out['high_list']
        low_list = data_read_out['low_list']

        volume_filtered_list = data_read_out['volume_filtered_list']
        interest_filtered_list = data_read_out['interest_filtered_list']

        month_strike_list = data_read_out['month_strike_list']

        selected_frame = title_frame[(title_frame['asset_type'] == 'futures') & (title_frame['ticker_head'] == ticker_head)]

        if len(selected_frame.index)==0:
            return {'settle_frame': settle_frame}

        contact_month_strings = month_strike_list[selected_frame.index[0]]

        datetime_conversion = [dt.datetime.strptime(x.replace('JLY', 'JUL'), '%b%y') for x in contact_month_strings]

        settle_frame['ticker_year'] = [x.year for x in datetime_conversion]
        settle_frame['ticker_month'] = [x.month for x in datetime_conversion]

        settle_frame['settle'] = settle_list[selected_frame.index[0]]

        settle_frame['open'] = open_list[selected_frame.index[0]]
        settle_frame['high'] = high_list[selected_frame.index[0]]
        settle_frame['high'] = settle_frame['high'].str.replace('B', '')
        settle_frame['low'] = low_list[selected_frame.index[0]]
        settle_frame['low'] = settle_frame['low'].str.replace('A', '')

        settle_frame['volume'] = volume_filtered_list[selected_frame.index[0]]
        settle_frame['interest'] = interest_filtered_list[selected_frame.index[0]]

    elif file_type == 'csv':
        data_read_out = rcf.read_cme_future_settle_csv_files(file_name=file_name, report_date=report_date)

        selected_frame = data_read_out[data_read_out['ticker_head'] == ticker_head]

        settle_frame['ticker_head'] = selected_frame['ticker_head']
        settle_frame['ticker_year'] = selected_frame['CONTRACT YEAR']
        settle_frame['ticker_month'] = selected_frame['CONTRACT MONTH'].astype('int')

        settle_frame['settle'] = selected_frame['SETTLE']
        settle_frame['open'] = selected_frame['OPEN']
        settle_frame['high'] = selected_frame['HIGH']
        settle_frame['low'] = selected_frame['LOW']
        settle_frame['volume'] = selected_frame['EST. VOL']
        settle_frame['interest'] = selected_frame['PRIOR INT']

        settle_frame['volume'] = settle_frame['volume'].replace('', 0)
        settle_frame['interest'] = settle_frame['interest'].replace('', 0)

    settle_frame['ticker'] = [ticker_head +
                            cmi.full_letter_month_list[settle_frame.loc[x, 'ticker_month']-1] +
                            str(settle_frame.loc[x, 'ticker_year']) for x in settle_frame.index]

    settle_frame['volume'] = settle_frame['volume'].replace('',0)
    settle_frame['volume'] = settle_frame['volume'].astype('int')

    settle_frame['interest'] = settle_frame['interest'].replace('',0)
    settle_frame['interest'] = settle_frame['interest'].astype('int')

    if ticker_head in ['ED', 'SM', 'BO', 'LC', 'LN', 'FC', 'ES', 'NQ', 'AD', 'CD', 'EC', 'JY', 'BP']:
        settle_frame['settle'] = settle_frame['settle'].astype('float64')
        settle_frame['open'] = settle_frame['open'].replace('----', np.NaN)
        settle_frame['high'] = settle_frame['high'].replace('----', np.NaN)
        settle_frame['low'] = settle_frame['low'].replace('----', np.NaN)
        settle_frame['open'] = settle_frame['open'].astype('float64')
        settle_frame['high'] = settle_frame['high'].astype('float64')
        settle_frame['low'] = settle_frame['low'].astype('float64')
    elif ticker_head in ['C', 'S', 'W', 'KW']:
        splited_strings = [x.split("'") for x in settle_frame['settle']]
        settle_frame['settle'] = [int(x[0])+int(x[1])*0.125 if len(x) == 2 else np.NaN for x in splited_strings]
        splited_strings = [x.split("'") for x in settle_frame['open']]
        settle_frame['open'] = [int(x[0])+int(x[1])*0.125 if len(x) == 2 else np.NaN for x in splited_strings]
        splited_strings = [x.split("'") for x in settle_frame['high']]
        settle_frame['high'] = [int(x[0])+int(x[1])*0.125 if len(x) == 2 else np.NaN for x in splited_strings]
        splited_strings = [x.split("'") for x in settle_frame['low']]
        settle_frame['low'] = [int(x[0])+int(x[1])*0.125 if len(x) == 2 else np.NaN for x in splited_strings]
    elif ticker_head in ['FV', 'TU', 'TY', 'US']:
        settle_frame['settle'] = [convert_treasury_settles(ticker_head,x) for x in settle_frame['settle']]
        settle_frame['open'] = [convert_treasury_settles(ticker_head,x) for x in settle_frame['open']]
        settle_frame['high'] = [convert_treasury_settles(ticker_head,x) for x in settle_frame['high']]
        settle_frame['low'] = [convert_treasury_settles(ticker_head,x) for x in settle_frame['low']]
    elif ticker_head in ['GC', 'SI', 'CL', 'NG', 'HO', 'RB']:
        settle_frame['settle'] = settle_frame['settle'].astype('float64')
        settle_frame['open'] = settle_frame['open'].replace('', np.NaN)
        settle_frame['high'] = settle_frame['high'].replace('', np.NaN)
        settle_frame['low'] = settle_frame['low'].replace('', np.NaN)
        settle_frame['open'] = settle_frame['open'].astype('float64')
        settle_frame['high'] = settle_frame['high'].astype('float64')
        settle_frame['low'] = settle_frame['low'].astype('float64')


    return {'settle_frame': settle_frame}

def convert_treasury_settles(ticker_head,cme_string):

    splitted_strings = cme_string.split("'")

    if len(splitted_strings) < 2:
        converted_value = np.NaN
    elif len(splitted_strings) == 2:

        if ticker_head in ['FV', 'TU', 'TY']:
            if splitted_strings[1][2] == '2':
                decimal_value = 0.25
            elif splitted_strings[1][2] == '5':
                decimal_value = 0.5
            elif splitted_strings[1][2] == '7':
                decimal_value = 0.75
            elif splitted_strings[1][2] == '0':
                decimal_value = 0

            converted_value = int(splitted_strings[0])+\
                              ((int(splitted_strings[1][:-1])+decimal_value)/32)

        elif ticker_head == 'US':

            converted_value = int(splitted_strings[0])+\
                              ((int(splitted_strings[1]))/32)


    return converted_value
