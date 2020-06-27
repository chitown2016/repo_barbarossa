
import pandas as pd
import numpy as np
import shared.calendar_utilities as cu
import shared.directory_names_aux as dna
import contract_utilities.contract_meta_info as cmi
import ta.portfolio_manager as tpm
pd.options.mode.chained_assignment = None  # default='warn'
import math as m

conversion_from_man_ticker_head = {'6': 'SM',
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

wh_strike_multiplier = {'C': 100, 'S': 100, 'BP': 0.01, 'SI': 0.01, 'LC': 0.1,'LN': 0.01,'NG':0.01,'ES': 0.1}

def load_and_convert_wh_position_file(**kwargs):

    positions_directory = dna.get_directory_name(ext='wh_positions')
    file_name = 'open pos .tul01 5-1.xlsx'

    double_date = cu.get_doubledate()
    century_mark = m.floor(double_date / 1e6) * 100

    wh_frame = pd.read_excel(positions_directory + '/' + file_name, header=None, names=['raw_symbol', 'qty'])

    raw_symbol_list = []
    qty_list = []
    instrument_list = []
    current_instrument = ''
    direction_list = []
    current_direction = ''

    strike_price_list = []
    option_type_list = []
    ticker_list = []
    ticker_head_list = []

    for i in range(len(wh_frame.index) - 1):
        if len(str(wh_frame['raw_symbol'].iloc[i])) > 4 and (str(wh_frame['qty'].iloc[i]).isnumeric()):
            raw_symbol_list.append(wh_frame['raw_symbol'].iloc[i])
            instrument_list.append(current_instrument)

            split_out = wh_frame['raw_symbol'].iloc[i].split(' ')
            if split_out[0] in ['CALL', 'PUT']:
                strike_price_list.append(float(split_out[-1]))
                option_type_list.append(split_out[0][0])
                if split_out[0] == 'CALL':
                    month_indx = 1
                    year_indx = 2
                else:
                    month_indx = 2
                    year_indx = 3
            else:
                strike_price_list.append(np.nan)
                option_type_list.append(None)
                month_indx = 0
                year_indx = 1

            ticker_head = conversion_from_man_ticker_head[current_instrument]
            ticker_head_list.append(ticker_head)
            ticker_list.append(ticker_head +
                               cmi.full_letter_month_list[cu.three_letter_month_dictionary[split_out[month_indx]] - 1] +
                               str(century_mark + int(split_out[year_indx])))

            if current_direction == 'B':
                qty_list.append(wh_frame['qty'].iloc[i])
            elif current_direction == 'S':
                qty_list.append(-wh_frame['qty'].iloc[i])
            else:
                print(current_direction)
        elif wh_frame['raw_symbol'].iloc[i] in ['B', 'S']:
            current_direction = wh_frame['raw_symbol'].iloc[i]
        else:
            current_instrument = str(wh_frame['raw_symbol'].iloc[i])

    wh_frame = pd.DataFrame()
    wh_frame['Instrument'] = instrument_list
    wh_frame['raw_symbol'] = raw_symbol_list
    wh_frame['strike_price'] = strike_price_list
    wh_frame['option_type'] = option_type_list
    wh_frame['qty'] = qty_list
    wh_frame['ticker_head'] = ticker_head_list
    wh_frame['ticker'] = ticker_list

    wh_frame['strike_multiplier'] = [wh_strike_multiplier.get(x, 1) for x in wh_frame['ticker_head']]
    wh_frame['strike_price'] = round(wh_frame['strike_multiplier']* wh_frame['strike_price'], 4)

    wh_frame['instrumet'] = 'F'
    option_indx = (wh_frame['option_type'] == 'C') | (wh_frame['option_type'] == 'P')
    wh_frame['instrumet'][option_indx] = 'O'

    wh_frame['generalized_ticker'] = wh_frame['ticker']
    wh_frame['generalized_ticker'][option_indx] = wh_frame['ticker'][option_indx] + '-' + \
                                                   wh_frame['option_type'][option_indx] + '-' + \
                                                   wh_frame['strike_price'][option_indx].astype(str)

    wh_frame['generalized_ticker'] = [x.rstrip('0').rstrip('.') for x in wh_frame['generalized_ticker']]

    return wh_frame[['generalized_ticker', 'qty']]

def reconcile_position(**kwargs):

    wh_position = load_and_convert_wh_position_file(**kwargs)

    db_position = tpm.get_position_4portfolio(trade_date_to=cu.get_doubledate())

    db_position['generalized_ticker'] = [x.rstrip('0').rstrip('.') for x in db_position['generalized_ticker']]

    merged_data = pd.merge(wh_position,db_position,how='outer',on='generalized_ticker')
    merged_data['qty_diff'] = merged_data['qty_x'].astype('float64')-merged_data['qty_y'].astype('float64')
    return merged_data[merged_data['qty_diff']!=0]