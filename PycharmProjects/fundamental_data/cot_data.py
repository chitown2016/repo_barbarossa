
import quandl_data.get_data_quandl as gdq
import shared.directory_names_aux as dna
import contract_utilities.contract_meta_info as cmi
import shared.calendar_utilities as cu
import datetime as dt
import os.path
import pandas as pd

presaved_cot_data_folder = dna.get_directory_name(ext='commitments_of_traders_data')

db_2_quandl_dictionary = {'GC': '088691',
                          'SI': '084691',
                          'EC': '099741',
                          'BP': '096742',
                          'JY': '097741',
                          'AD': '232741',
                          'CD': '090741',
                          'TU': '042601',
                          'FV': '044601',
                          'TY': '043602',
                          'US': '020601',
                          'ED': '132741',
                          'ES': '13874A',
                          'SP': '138741',
                          'NQ': '209742',
                          'CL': '067651',
                          'HO': '022651',
                          'RB': '111659',
                          'NG': '023651',
                          'C': '002602',
                          'W': '001602',
                          'KW': '001612',
                          'S': '005602',
                          'SM': '026603',
                          'BO': '007601',
                          'LN': '054642',
                          'LC': '057642',
                          'FC': '061641',
                          'KC': '083731',
                          'CT': '033661',
                          'SB': '080732',
                          'CC': '073732',
                          'OJ': '040701'}

def presave_cot_data_4ticker_head(**kwargs):

    ticker_head = kwargs['ticker_head']

    file_name = presaved_cot_data_folder + '/' + ticker_head + '.pkl'

    if os.path.isfile(file_name):
        old_data = pd.read_pickle(file_name)
        kwargs['date_from'] = int(1e4*old_data['settle_date'].iloc[-1].year+1e2*old_data['settle_date'].iloc[-1].month+old_data['settle_date'].iloc[-1].day)

    quandl_ticker = 'CFTC/' + db_2_quandl_dictionary[ticker_head] + '_FO_L_ALL'

    kwargs['quandl_ticker'] = quandl_ticker
    quandl_out = gdq.get_data(**kwargs)

    if quandl_out['success']:
        new_data = quandl_out['data_out']
        new_data['settle_date'] = new_data.index

        if os.path.isfile(file_name):

            new_data['frame_indx'] = 1
            old_data['frame_indx'] = 0

            merged_data = pd.concat([old_data, new_data],ignore_index=True)
            merged_data.sort_values(['settle_date', 'frame_indx'], ascending=[True, False], inplace=True)
            merged_data.drop_duplicates(subset='settle_date',keep='first', inplace=True)
            merged_data.drop('frame_indx', 1, inplace=True)
            data_out = merged_data
        else:
            data_out = new_data

        data_out.reset_index(drop=True, inplace=True)
        data_out.to_pickle(presaved_cot_data_folder + '/' + ticker_head + '.pkl')


def presave_cot_data():

    tickerhead_list = list(set(cmi.futures_butterfly_strategy_tickerhead_list + cmi.cme_futures_tickerhead_list))
    tickerhead_list.remove('B')
    tickerhead_list.append('SP')
    [presave_cot_data_4ticker_head(ticker_head=x) for x in tickerhead_list]


def get_cot_data(**kwargs):

    ticker_head = kwargs['ticker_head']

    if os.path.isfile(presaved_cot_data_folder + '/' + ticker_head + '.pkl'):
        data_out = pd.read_pickle(presaved_cot_data_folder + '/' + ticker_head + '.pkl')
        if 'date_from' in kwargs.keys():
            data_out = data_out[data_out['settle_date'] >= cu.convert_doubledate_2datetime(kwargs['date_from'])]
        if 'date_to' in kwargs.keys():
            data_out = data_out[data_out['settle_date'] <= cu.convert_doubledate_2datetime(kwargs['date_to'])-dt.timedelta(days=3)]
    else:
        data_out = pd.DataFrame()
    return data_out

def get_cot_signals(**kwargs):

    ticker_head = kwargs['ticker_head']
    kwargs2 = {'date_to': kwargs['date_to']}

    if ticker_head=='ES':
        kwargs2['ticker_head'] = 'ES'
        es_data = get_cot_data(**kwargs2)
        kwargs2['ticker_head'] = 'SP'
        sp_data = get_cot_data(**kwargs2)
        final_data = pd.merge(es_data, sp_data, left_on='settle_date', right_on='settle_date', how='inner')
        final_data['Noncommercial Long'] = final_data['Noncommercial Long_x'] + 5*final_data['Noncommercial Long_y']
        final_data['Noncommercial Short'] = final_data['Noncommercial Short_x'] + 5*final_data['Noncommercial Short_y']
        final_data['Commercial Long'] = final_data['Commercial Long_x'] + 5*final_data['Commercial Long_y']
        final_data['Commercial Short'] = final_data['Commercial Short_x'] + 5*final_data['Commercial Short_y']
        final_data['Nonreportable Positions Long'] = final_data['Nonreportable Positions Long_x'] + 5 * final_data['Nonreportable Positions Long_y']
        final_data['Nonreportable Positions Short'] = final_data['Nonreportable Positions Short_x'] + 5 * final_data['Nonreportable Positions Short_y']
        final_data['Open Interest'] = final_data['Open Interest_x'] + 5 * final_data['Open Interest_y']
    else:
        final_data = get_cot_data(**kwargs)

    final_data['comm_net'] = final_data['Commercial Long'] - final_data['Commercial Short']
    final_data['spec_net'] = final_data['Noncommercial Long'] - final_data['Noncommercial Short']
    final_data['small_net'] = final_data['Nonreportable Positions Long'] - final_data['Nonreportable Positions Short']
    final_data['oi'] = final_data['Open Interest']

    final_data['comm_net_oi'] = final_data['comm_net']/final_data['oi']
    final_data['h_comm_net_oi'] = final_data['comm_net_oi'].rolling(26, min_periods=20).max()
    final_data['l_comm_net_oi'] = final_data['comm_net_oi'].rolling(26, min_periods=20).min()
    final_data['willco'] = 100 * (final_data['comm_net_oi'] - final_data['l_comm_net_oi']) / (final_data['h_comm_net_oi'] - final_data['l_comm_net_oi'])



    final_data['h_oi'] = final_data['oi'].rolling(52, min_periods=40).max()
    final_data['l_oi'] = final_data['oi'].rolling(52, min_periods=40).min()
    final_data['k_oi'] = 100 * (final_data['oi'] - final_data['l_oi']) / (final_data['h_oi'] - final_data['l_oi'])
    final_data['s_oi'] = final_data['k_oi'].rolling(3, min_periods=3).mean()

    final_data['comm_min_156'] = final_data['comm_net'].rolling(156,min_periods=130).min()
    final_data['comm_max_156'] = final_data['comm_net'].rolling(156, min_periods=130).max()
    final_data['comm_indx_156'] = 100*(final_data['comm_net']-final_data['comm_min_156'])/(final_data['comm_max_156']-final_data['comm_min_156'])



    final_data['spec_min_156'] = final_data['spec_net'].rolling(156, min_periods=130).min()
    final_data['spec_max_156'] = final_data['spec_net'].rolling(156, min_periods=130).max()
    final_data['spec_indx_156'] = 100 * (final_data['spec_net'] - final_data['spec_min_156']) / (
                final_data['spec_max_156'] - final_data['spec_min_156'])


    final_data['small_min_156'] = final_data['small_net'].rolling(156, min_periods=130).min()
    final_data['small_max_156'] = final_data['small_net'].rolling(156, min_periods=130).max()
    final_data['small_indx_156'] = 100 * (final_data['small_net'] - final_data['small_min_156']) / (
                final_data['small_max_156'] - final_data['small_min_156'])

    if 'date_from' in kwargs.keys():
        final_data = final_data[final_data['settle_date'] >= cu.convert_doubledate_2datetime(kwargs['date_from'])]

    return final_data






