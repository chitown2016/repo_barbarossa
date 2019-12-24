
import contract_utilities.contract_meta_info as cmi
import read_exchange_files.read_cme_files as rcf
import read_exchange_files.cme_utilities as cmeu
import datetime as dt
import pandas as pd
import numpy as np
pd.options.mode.chained_assignment = None  # default='warn'


def process_cme_options_4ticker(**kwargs):

    ticker = kwargs['ticker']
    report_date = kwargs['report_date']

    contract_specs_output = cmi.get_contract_specs(ticker)
    ticker_class = contract_specs_output['ticker_class']
    ticker_head = contract_specs_output['ticker_head']

    name_type_output = cmeu.get_file_name_type_from_tickerclass(ticker_class, 'options')
    file_name = name_type_output['file_name']
    file_type = name_type_output['file_type']

    if file_type == 'txt':

        if 'data_read_out' in kwargs.keys():
            data_read_out = kwargs['data_read_out'][file_name]
        else:
            data_read_out = rcf.read_cme_settle_txt_files(file_name=file_name, report_date=report_date)

        title_frame = data_read_out['title_frame']
        settle_list = data_read_out['settle_list']
        month_strike_list = data_read_out['month_strike_list']
        volume_filtered_list = data_read_out['volume_filtered_list']
        interest_filtered_list = data_read_out['interest_filtered_list']

        selected_frame = title_frame[(title_frame['asset_type'] == 'options') & (title_frame['ticker_head'] == ticker_head)]

        if selected_frame.empty:
            return {'success': False, 'settle_frame': pd.DataFrame()}

        datetime_conversion = [dt.datetime.strptime(x.replace('JLY', 'JUL'), '%b%y') for x in selected_frame['maturity_string']]
        selected_frame['ticker_year'] = [x.year for x in datetime_conversion]
        selected_frame['ticker_month'] = [x.month for x in datetime_conversion]

        selected_frame['ticker'] = [ticker_head +
                            cmi.full_letter_month_list[selected_frame.loc[x, 'ticker_month']-1] +
                            str(selected_frame.loc[x, 'ticker_year']) for x in selected_frame.index]

        selected_frame_call = selected_frame[(selected_frame['ticker'] == ticker)&(selected_frame['option_type'] == 'C')]
        selected_frame_put = selected_frame[(selected_frame['ticker'] == ticker)&(selected_frame['option_type'] == 'P')]

        if selected_frame_call.empty or selected_frame_put.empty:
            return {'success': False, 'settle_frame': pd.DataFrame()}

        selected_call_indx = selected_frame_call.index[0]
        selected_put_indx = selected_frame_put.index[0]

        call_dataframe = pd.DataFrame.from_dict({'strike': month_strike_list[selected_call_indx],
                                                  'settle': settle_list[selected_call_indx],
                                                  'volume': volume_filtered_list[selected_call_indx],
                                                  'interest': interest_filtered_list[selected_call_indx]})

        put_dataframe = pd.DataFrame.from_dict({'strike': month_strike_list[selected_put_indx],
                                                  'settle': settle_list[selected_put_indx],
                                                  'volume': volume_filtered_list[selected_put_indx],
                                                  'interest': interest_filtered_list[selected_put_indx]})

        call_dataframe['option_type'] = 'C'
        put_dataframe['option_type'] = 'P'

        settle_frame = pd.concat([call_dataframe, put_dataframe])

    elif file_type == 'csv':

        data_read_out = rcf.read_cme_option_settle_csv_files(file_name=file_name, report_date=report_date)

        settle_frame = pd.DataFrame()
        selected_frame = data_read_out[data_read_out['ticker_head'] == ticker_head]

        if selected_frame.empty:
            return {'success': False, 'settle_frame': pd.DataFrame()}

        selected_frame['ticker_month'] = selected_frame['CONTRACT MONTH'].astype('int')

        selected_frame['ticker'] = [ticker_head +
                            cmi.full_letter_month_list[selected_frame.loc[x, 'ticker_month']-1] +
                            str(selected_frame.loc[x, 'CONTRACT YEAR']) for x in selected_frame.index]

        selected_frame = selected_frame[selected_frame['ticker'] == ticker]

        if selected_frame.empty:
            return {'success': False, 'settle_frame': pd.DataFrame()}

        settle_frame['ticker'] = selected_frame['ticker']
        settle_frame['option_type'] = selected_frame['PUT/CALL']
        settle_frame['strike'] = selected_frame['STRIKE']
        settle_frame['settle'] = selected_frame['SETTLE']
        settle_frame['volume'] = selected_frame['EST. VOL']
        settle_frame['interest'] = selected_frame['PRIOR INT']
        settle_frame.reset_index(inplace=True, drop=True)

    settle_frame = settle_frame[~settle_frame['settle'].str.contains('-')]

    if settle_frame.empty:
            return {'success': False, 'settle_frame': pd.DataFrame()}

    settle_frame['strike'] = settle_frame['strike'].astype('float64')

    if ticker_head in ['C', 'S', 'W', 'KW']:
        splited_strings = [x.split("'") for x in settle_frame['settle']]
        settle_frame['settle'] = [(0 if x[0] == '' else int(x[0])) + int(x[1])*0.125 if len(x) == 2 else np.NaN for x in splited_strings]
        settle_frame['strike'] = settle_frame['strike']/10
    elif ticker_head in ['ED', 'E0', 'E2', 'E3', 'E4', 'E5']:
        settle_frame['settle'] = settle_frame['settle'].replace('CAB', cmi.option_cabinet_values[ticker_head])
        settle_frame['settle'] = settle_frame['settle'].astype('float64')
        half_quarter_indx = settle_frame['strike'] % 25 == 12
        settle_frame['strike'] = settle_frame['strike']/100
        settle_frame['strike'][half_quarter_indx] = settle_frame['strike'][half_quarter_indx]+0.005
    elif ticker_head in ['SM']:
        settle_frame['settle'] = settle_frame['settle'].astype('float64')
        settle_frame['strike'] = settle_frame['strike']/100
    elif ticker_head in ['BO']:
        settle_frame['settle'] = settle_frame['settle'].astype('float64')
        settle_frame['strike'] = settle_frame['strike']/1000
    elif ticker_head in ['LC', 'LN', 'ES', 'NQ']:
        settle_frame.reset_index(inplace=True, drop=True)
        settle_frame['settle'] = settle_frame['settle'].replace('CAB', cmi.option_cabinet_values[ticker_head])
        settle_frame['settle'] = settle_frame['settle'].astype('float64')
    elif ticker_head in ['FC']:
        settle_frame['settle'] = settle_frame['settle'].replace('CAB', cmi.option_cabinet_values[ticker_head])
        settle_frame['settle'] = settle_frame['settle'].astype('float64')
        settle_frame['strike'] = settle_frame['strike']/100
    elif ticker_head in ['AD', 'CD']:
        settle_frame['settle'] = settle_frame['settle'].replace('CAB', 100*cmi.option_cabinet_values[ticker_head])
        settle_frame['settle'] = settle_frame['settle'].astype('float64')/100
        settle_frame['strike'] = settle_frame['strike']/10000
    elif ticker_head in ['EC']:
        settle_frame['settle'] = settle_frame['settle'].replace('CAB', cmi.option_cabinet_values[ticker_head])
        settle_frame['settle'] = settle_frame['settle'].astype('float64')
        settle_frame['strike'] = settle_frame['strike']/10000
    elif ticker_head in ['JY']:
        settle_frame['settle'] = settle_frame['settle'].replace('CAB', cmi.option_cabinet_values[ticker_head]/1000)
        settle_frame['settle'] = settle_frame['settle'].astype('float64')*1000
        settle_frame['strike'] = settle_frame['strike']*10
    elif ticker_head in ['BP']:
        settle_frame['settle'] = settle_frame['settle'].replace('CAB', 100*cmi.option_cabinet_values[ticker_head])
        settle_frame['settle'] = settle_frame['settle'].astype('float64')/100
        settle_frame['strike'] = settle_frame['strike']/1000
    elif ticker_head in ['TU', 'FV']:
        half_quarter_indx = settle_frame['strike'] % 25 == 12
        settle_frame['strike'] = settle_frame['strike']/100
        settle_frame['strike'][half_quarter_indx] = settle_frame['strike'][half_quarter_indx]+0.005

        splited_strings = [x.split("'") for x in settle_frame['settle']]

        settle_frame['settle'] = [(0 if x[0] == '' else int(x[0])) + int(x[1])/640 if len(x) == 2 else
                                      (cmi.option_cabinet_values[ticker_head] if x[0] == 'CAB' else np.NaN) for x in splited_strings]
    elif ticker_head in ['TY','US']:
        settle_frame['strike'] = settle_frame['strike']/100

        splited_strings = [x.split("'") for x in settle_frame['settle']]

        settle_frame['settle'] = [(0 if x[0] == '' else int(x[0])) + int(x[1])/64 if len(x) == 2 else
                                      (cmi.option_cabinet_values[ticker_head] if x[0] == 'CAB' else np.NaN) for x in splited_strings]

    elif ticker_head in ['GC', 'SI', 'CL', 'NG', 'RB', 'HO']:
        settle_frame['settle'] = settle_frame['settle'].replace([''], [None])
        settle_frame = settle_frame[~pd.isnull(settle_frame['settle'])]
        settle_frame['settle'] = settle_frame['settle'].astype('float64')

    settle_frame['volume'] = settle_frame['volume'].replace('', 0)
    settle_frame['volume'] = settle_frame['volume'].astype('int')

    settle_frame['interest'] = settle_frame['interest'].replace('', 0)
    settle_frame['interest'] = settle_frame['interest'].astype('int')

    return {'success': True, 'settle_frame': settle_frame}


def convert_treasury_settles(ticker_head, cme_string):

    splitted_strings = cme_string.split("'")

    if len(splitted_strings) < 2:
        converted_value = np.NaN
    elif len(splitted_strings) == 2:

        if ticker_head in ['TU']:
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
