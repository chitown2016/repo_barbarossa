

import pandas as pd
import os as os
import math as m
import shared.directory_names_aux as dna
import contract_utilities.contract_meta_info as cmi
pd.options.mode.chained_assignment = None  # default='warn'
import ta.portfolio_manager as tpm
import shared.calendar_utilities as cu

conversion_from_man_ticker_head = {'06': 'SM',
                                   '6': 'SM',
                                   '07': 'BO',
                                   '7': 'BO',
                                   'C-': 'C',
                                   'KW': 'KW',
                                   'S-': 'S',
                                   'W-': 'W',
                                   '48': 'LC',
                                   '62': 'FC',
                                   'LN': 'LN',
                                   '39': 'SI',
                                   '37': 'GC',
                                   '27': 'SB',
                                   '21': 'TY',
                                   '43': 'KC',
                                   '96': 'OJ',
                                   'CY': 'CC',
                                   'CU': 'CL',
                                   'HO': 'HO',
                                   'NG': 'NG',
                                   'RB': 'RB',
                                   '28': 'CT',
                                   'EC': 'EC',
                                   'BP': 'BP',
                                   'C1': 'CD',
                                   'ED': 'ED',
                                   'ES': 'ES',
                                   'EU': 'EC',
                                   'GU': 'BP',
                                   'BC': 'B'}

man_strike_multiplier = {'C': 100, 'S': 100, 'W': 100, 'BP': 0.01, 'SI': 0.01}


def load_and_convert_man_position_file(**kwargs):

    positions_directory = dna.get_directory_name(ext='man_positions')

    file_list = os.listdir(positions_directory)
    num_files = len(file_list)

    time_list = []

    for i in range(num_files):
        time_list.append(os.path.getmtime(positions_directory + '/' + file_list[i]))

    loc_latest_file = time_list.index(max(time_list))

    man_frame = pd.read_csv(positions_directory + '/' + file_list[loc_latest_file])
    man_frame.rename(columns={'Strike': 'strike_price', 'Option Type': 'option_type',
                              'Net Qty': 'qty','Futures Code': 'Instrument',
                              'Contract Maturity': 'Prompt'}, inplace=True)

    man_frame['ticker_head'] = [conversion_from_man_ticker_head[x] for x in man_frame['Instrument']]

    man_frame['strike_multiplier'] = [man_strike_multiplier.get(x, 1) for x in man_frame['ticker_head']]

    man_frame['ticker'] = [man_frame['ticker_head'].iloc[x] +
                           cmi.full_letter_month_list[int(man_frame['Prompt'].iloc[x]%100)-1] +
                           str(m.floor(man_frame['Prompt'].iloc[x]/100))
                               for x in range(len(man_frame.index))]

    man_frame.rename(columns={'Strike': 'strike_price', 'OptionType': 'option_type', 'NetQty': 'qty'}, inplace=True)
    man_frame['strike_price'] = round(man_frame['strike_multiplier']*man_frame['strike_price'],4)

    man_frame['instrumet'] = 'F'
    option_indx = (man_frame['option_type'] == 'C')|(man_frame['option_type'] == 'P')
    man_frame['instrumet'][option_indx] = 'O'

    man_frame['generalized_ticker'] = man_frame['ticker']
    man_frame['generalized_ticker'][option_indx] = man_frame['ticker'][option_indx] + '-' + \
                                                         man_frame['option_type'][option_indx] + '-' + \
                                                         man_frame['strike_price'][option_indx].astype(str)

    man_frame = man_frame[man_frame['qty'] != 0]
    man_frame['generalized_ticker'] = [x.rstrip('0').rstrip('.') for x in man_frame['generalized_ticker']]

    return man_frame[['generalized_ticker', 'qty']]


def reconcile_position(**kwargs):

    abn_position = load_and_convert_man_position_file(**kwargs)

    db_position = tpm.get_position_4portfolio(trade_date_to=cu.get_doubledate())

    db_position['generalized_ticker'] = [x.rstrip('0').rstrip('.') for x in db_position['generalized_ticker']]

    merged_data = pd.merge(abn_position,db_position,how='outer',on='generalized_ticker')
    merged_data['qty_diff'] = merged_data['qty_x'].astype('float64')-merged_data['qty_y'].astype('float64')
    return merged_data[merged_data['qty_diff']!=0]

