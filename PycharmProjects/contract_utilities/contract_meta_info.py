__author__ = 'kocat_000'

import math as m
import numpy as np
import pytz as pytz
import datetime as dt

full_letter_month_list = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']
quarterly_month_list = ['H', 'M', 'U', 'Z']
letter_month_string = 'FGHJKMNQUVXZ'
central_zone = pytz.timezone('US/Central')

futures_contract_months = {'LN': ['G', 'J', 'K', 'M', 'N', 'Q', 'V', 'Z'],
                           'LC': ['G', 'J', 'M', 'Q', 'V', 'Z'],
                           'FC': ['F', 'H', 'J', 'K', 'Q', 'U', 'V', 'X'],
                           'C': ['H', 'K', 'N', 'U', 'Z'],
                           'S': ['F', 'H', 'K', 'N', 'Q', 'U', 'X'],
                           'SM': ['F', 'H', 'K', 'N', 'Q', 'U', 'V', 'Z'],
                           'BO': ['F', 'H', 'K', 'N', 'Q', 'U', 'V', 'Z'],
                           'W': ['H', 'K', 'N', 'U', 'Z'],
                           'KW': ['H', 'K', 'N', 'U', 'Z'],
                           'SB': ['H', 'K', 'N', 'V'],
                           'KC': ['H', 'K', 'N', 'U', 'Z'],
                           'CC': ['H', 'K', 'N', 'U', 'Z'],
                           'CT': ['H', 'K', 'N', 'V', 'Z'],
                           'OJ': ['F', 'H', 'K', 'N', 'U', 'X'],
                           'ED': full_letter_month_list,
                           'CL': full_letter_month_list,
                           'B' : full_letter_month_list,
                           'HO': full_letter_month_list,
                           'RB': full_letter_month_list,
                           'NG': full_letter_month_list,
                           'ES': quarterly_month_list,
                           'NQ': quarterly_month_list,
                           'EC': quarterly_month_list,
                           'JY': quarterly_month_list,
                           'AD': quarterly_month_list,
                           'CD': quarterly_month_list,
                           'BP': quarterly_month_list,
                           'TY': quarterly_month_list,
                           'US': quarterly_month_list,
                           'FV': quarterly_month_list,
                           'TU': quarterly_month_list,
                           'GC': full_letter_month_list,
                           'SI': full_letter_month_list}

option_cabinet_values = {'ED': 0.0025,
                         'E0': 0.0025,
                         'E2': 0.0025,
                         'E3': 0.0025,
                         'E4': 0.0025,
                         'E5': 0.0025,
                         'LC': 0.0125,
                         'LN': 0.0125,
                         'FC': 0.0125,
                         'ES': 0.05,
                         'NQ': 0.05,
                         'TU': 1/128,
                         'FV': 1/128,
                         'TY': 1/64,
                         'US': 1/64,
                         'EC': 0.00005,
                         'JY': 5,
                         'AD': 0.00005,
                         'CD': 0.00005,
                         'BP': 0.0001}


def get_option_exercise_type(**kwargs):

    ticker_head = kwargs['ticker_head']

    if ticker_head in ['LN', 'LC', 'ES', 'GC', 'SI',
                       'TY', 'US', 'FV', 'TU', 'C', 'S', 'SM', 'BO', 'W', 'CL', 'NG',
                       'ED', 'E0', 'E2', 'E3', 'E4', 'E5']:
        exercise_type = 'A'
    elif ticker_head in ['EC', 'JY', 'AD', 'CD', 'BP']:
        exercise_type = 'E'
    else:
        exercise_type = None

    return exercise_type


def get_option_contract_months(**kwargs):

    ticker_head = kwargs['ticker_head']
    contract_months = []

    if ticker_head == 'LN':
        contract_months = ['G', 'J', 'K', 'M', 'N', 'Q', 'V', 'Z']
    elif ticker_head in option_tickerhead_list:
        contract_months = full_letter_month_list

    return contract_months

futures_butterfly_strategy_tickerhead_list = ['LN', 'LC', 'FC',
                                              'C', 'S', 'SM', 'BO', 'W', 'KW',
                                              'SB', 'KC', 'CC', 'CT', 'OJ',
                                              'CL', 'B', 'HO', 'RB', 'NG', 'ED']

cme_futures_tickerhead_list = ['LN', 'LC', 'FC',
                               'C', 'S', 'SM', 'BO', 'W', 'KW',
                               'CL', 'HO', 'RB', 'NG', 'ED',
                               'ES', 'NQ', 'EC', 'JY', 'AD', 'CD', 'BP',
                               'TY', 'US', 'FV', 'TU', 'GC', 'SI']

cme_option_tickerhead_list = ['LN', 'LC', 'ES', 'EC', 'JY', 'AD', 'CD', 'BP', 'GC', 'SI',
                              'TY', 'US', 'FV', 'TU', 'C', 'S', 'SM', 'BO', 'W', 'CL', 'NG',
                              'ED', 'E0', 'E2', 'E3', 'E4', 'E5']

option_tickerhead_list = cme_option_tickerhead_list


def get_tickerhead_list(list_name):

    if list_name == 'butterfly':
        tickerhead_list = futures_butterfly_strategy_tickerhead_list
    elif list_name == 'cme_futures':
        tickerhead_list = cme_futures_tickerhead_list
    elif list_name == 'cme_options':
        tickerhead_list = cme_option_tickerhead_list

    return tickerhead_list


def get_exchange_traded(ticker_head):

    if (ticker_head in cme_futures_tickerhead_list) or (ticker_head in cme_option_tickerhead_list):
        exchange = 'CME'
    elif ticker_head in futures_butterfly_strategy_tickerhead_list:
        exchange = 'ICE'
    else:
        exchange = null

    return exchange

contract_name = {'LN': 'Lean Hog',
                 'LC': 'Live Cattle',
                 'FC': 'Feeder Cattle',
                 'C': 'Corn',
                 'S': 'Soybean',
                 'SM': 'Soybean Meal',
                 'BO': 'Soybean Oil',
                 'W': 'Chicago SRW Wheat',
                 'KW': 'Kansas HRW Wheat',
                 'SB': 'Sugar 11',
                 'KC': 'Coffee C',
                 'CC': 'Cocoa',
                 'CT': 'Cotton No. 2',
                 'OJ': 'FCOJ-A',
                 'CL': 'Crude Oil',
                 'B' : 'Brent Crude',
                 'HO': 'NY Harbor ULSD',
                 'RB': 'RBOB Gasoline',
                 'NG': 'Henry Hub Natural Gas',
                 'ED': 'Eurodollar',
                 'E0': 'Eurodollar 1 Year Mid-Curve Options',
                 'E2': 'Eurodollar 2 Year Mid-Curve Options',
                 'E3': 'Eurodollar 3 Year Mid-Curve Options',
                 'E4': 'Eurodollar 4 Year Mid-Curve Options',
                 'E5': 'Eurodollar 5 Year Mid-Curve Options',
                 'ES': 'E-mini S&P 500',
                 'NQ': 'E-mini Nasdaq 100',
                 'EC': 'Euro FX',
                 'JY': 'Japanese Yen',
                 'AD': 'Australian Dollar',
                 'CD': 'Canadian Dollar',
                 'BP': 'British Pound',
                 'TY': '10-Year T-Note',
                 'US': 'US Treasury Bond',
                 'FV': '5-Year T-Note',
                 'TU': '2-Year T-Note',
                 'GC': 'Gold',
                 'SI': 'Silver'}

relevant_max_cal_dte = {'LN': 720,
                        'LC': 720,
                        'FC': 720,
                        'C': 720,
                        'S': 720,
                        'SM': 720,
                        'BO': 720,
                        'W': 720,
                        'KW': 720,
                        'SB': 720,
                        'KC': 720,
                        'CC': 720,
                        'CT': 720,
                        'OJ': 720,
                        'CL': 720,
                        'B' : 720,
                        'HO': 720,
                        'RB': 720,
                        'NG': 720,
                        'ED': 1440}

last_trade_hour_minute = {'LN': dt.time(13, 3, 0, 0),
                          'LC': dt.time(13, 3, 0, 0),
                          'FC': dt.time(13, 3, 0, 0),
                          'C': dt.time(13, 18, 0, 0),
                          'S': dt.time(13, 18, 0, 0),
                          'SM': dt.time(13, 18, 0, 0),
                          'BO': dt.time(13, 18, 0, 0),
                          'W': dt.time(13, 18, 0, 0),
                          'KW': dt.time(13, 18, 0, 0),
                          'SB': dt.time(11, 58, 0, 0),
                          'KC': dt.time(12, 28, 0, 0),
                          'CC': dt.time(12, 28, 0, 0),
                          'CT': dt.time(12, 28, 0, 0),
                          'OJ': dt.time(12, 58, 0, 0),
                          'CL': dt.time(14, 28, 0, 0),
                          'B': dt.time(14, 28, 0, 0),
                          'HO': dt.time(14, 28, 0, 0),
                          'RB': dt.time(14, 28, 0, 0),
                          'NG': dt.time(14, 28, 0, 0),
                          'ED': dt.time(14, 28, 0, 0),
                          'ES': dt.time(14, 28, 0, 0),
                          'NQ': dt.time(14, 28, 0, 0),
                          'EC': dt.time(14, 28, 0, 0),
                          'JY': dt.time(14, 28, 0, 0),
                          'AD': dt.time(14, 28, 0, 0),
                          'CD': dt.time(14, 28, 0, 0),
                          'BP': dt.time(14, 28, 0, 0),
                          'TY': dt.time(14, 28, 0, 0),
                          'US': dt.time(14, 28, 0, 0),
                          'FV': dt.time(14, 28, 0, 0),
                          'TU': dt.time(14, 28, 0, 0),
                          'GC': dt.time(14, 28, 0, 0),
                          'SI': dt.time(14, 28, 0, 0)}


first_trade_hour_minute = {'LN': dt.time(8, 35, 0, 0),
                           'LC': dt.time(8, 35, 0, 0),
                           'FC': dt.time(8, 35, 0, 0),
                           'C': dt.time(8, 35, 0, 0),
                           'S': dt.time(8, 35, 0, 0),
                           'SM': dt.time(8, 35, 0, 0),
                           'BO': dt.time(8, 35, 0, 0),
                           'W': dt.time(8, 35, 0, 0),
                           'KW': dt.time(8, 35, 0, 0),
                           'SB': dt.time(3, 0, 0, 0),
                           'KC': dt.time(4, 0, 0, 0),
                           'CC': dt.time(4, 0, 0, 0),
                           'CT': dt.time(0, 45, 0, 0),
                           'OJ': dt.time(8, 0, 0, 0),
                           'CL': dt.time(0, 45, 0, 0),
                           'B': dt.time(0, 45, 0, 0),
                           'HO': dt.time(0, 45, 0, 0),
                           'RB': dt.time(0, 45, 0, 0),
                           'NG': dt.time(0, 45, 0, 0),
                           'ED': dt.time(0, 45, 0, 0),
                           'ES': dt.time(0, 45, 0, 0),
                           'NQ': dt.time(0, 45, 0, 0),
                           'EC': dt.time(0, 45, 0, 0),
                           'JY': dt.time(0, 45, 0, 0),
                           'AD': dt.time(0, 45, 0, 0),
                           'CD': dt.time(0, 45, 0, 0),
                           'BP': dt.time(0, 45, 0, 0),
                           'TY': dt.time(0, 45, 0, 0),
                           'US': dt.time(0, 45, 0, 0),
                           'FV': dt.time(0, 45, 0, 0),
                           'TU': dt.time(0, 45, 0, 0),
                           'GC': dt.time(0, 45, 0, 0),
                           'SI': dt.time(0, 45, 0, 0)}


def get_max_cal_dte(**kwargs):

    ticker_head = kwargs['ticker_head']
    ticker_month = kwargs['ticker_month']

    if ticker_head in ['LN', 'LC', 'FC', 'C', 'S', 'SM', 'BO', 'W', 'KW', 'SB', 'KC', 'CC', 'CT', 'OJ', 'HO', 'RB', 'NG']:
        max_cal_dte = 720
    elif ticker_head in ['ED', 'E0','E2', 'E3', 'E4', 'E5']:
        max_cal_dte = 1440
    elif ticker_head == 'CL':
        if ticker_month in [6, 12]:
            max_cal_dte = 1080
        else:
            max_cal_dte = 720
    elif ticker_head == 'B':
        if ticker_month in [6, 12]:
            max_cal_dte = 1440
        else:
            max_cal_dte = 720
    elif ticker_head in ['ES', 'NQ', 'TY', 'US', 'FV', 'TU', 'GC', 'SI']:
        max_cal_dte = 540
    elif ticker_head in ['EC', 'JY', 'AD', 'CD', 'BP']:
        max_cal_dte = 720

    return max_cal_dte

ticker_class = {'LN': 'Livestock',
                'LC': 'Livestock',
                'FC': 'Livestock',
                'ES': 'Index',
                'NQ': 'Index',
                'EC': 'FX',
                'JY': 'FX',
                'AD': 'FX',
                'CD': 'FX',
                'BP': 'FX',
                'GC': 'Metal',
                'SI': 'Metal',
                'TY': 'Treasury',
                'US': 'Treasury',
                'FV': 'Treasury',
                'TU': 'Treasury',
                'C': 'Ag',
                'S': 'Ag',
                'SM': 'Ag',
                'BO': 'Ag',
                'W': 'Ag',
                'KW': 'Ag',
                'SB': 'Soft',
                'KC': 'Soft',
                'CC': 'Soft',
                'CT': 'Soft',
                'OJ': 'Soft',
                'CL': 'Energy',
                'B' : 'Energy',
                'HO': 'Energy',
                'RB': 'Energy',
                'NG': 'Energy',
                'ED': 'STIR',
                'E0': 'STIR',
                'E2': 'STIR',
                'E3': 'STIR',
                'E4': 'STIR',
                'E5': 'STIR'}

aligned_data_tickerhead = {'LN': 'LNH',
                           'LC': 'LVC',
                           'ES': 'SP',
                           'EC': 'EC',
                           'JY': 'JY100',
                           'AD': 'AD',
                           'CD': 'C1',
                           'BP': 'BP1',
                           'GC': 'GC',
                           'SI': 'SI',
                           'TY': 'TY',
                           'US': 'US',
                           'FV': 'FV',
                           'TU': 'TU',
                           'C': 'CB',
                           'S': 'SB',
                           'SM': 'SM',
                           'BO': 'BO',
                           'W': 'WW',
                           'CL': 'CL',
                           'NG': 'NG',
                           'ED': 'ED',
                           'E0': 'E1',
                           'E2': 'E2',
                           'E3': 'E3',
                           'E4': 'E4',
                           'E5': 'E5'}

def get_ib_exchange_name(ticker_head):
    if ticker_head in ['CL', 'HO', 'RB', 'NG','GC','SI']:
        exchange = 'NYMEX'
    elif ticker_head in ['C', 'W', 'KW', 'S', 'SM', 'BO' ,'TU', 'FV', 'TY', 'US']:
        exchange = 'ECBOT'
    elif ticker_head in ['LC', 'LN', 'FC', 'EC', 'BP', 'JY', 'AD', 'CD', 'ES', 'NQ', 'ED']:
        exchange = 'GLOBEX'
    else:
        exchange = ''

    return exchange


aligned_data_tr_dte_list = [10, 15, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100,
                           120, 140, 160, 180, 200, 220, 240, 260, 280, 300, 330, 360, 390]

contract_multiplier = {'LN': 400,
                       'LC': 400,
                       'FC': 500,
                       'C': 50,
                       'S': 50,
                       'SM': 100,
                       'BO': 600,
                       'W': 50,
                       'KW': 50,
                       'SB': 1120,
                       'KC': 375,
                       'CC': 10,
                       'CT': 500,
                       'OJ': 150,
                       'CL': 1000,
                       'B': 1000,
                       'HO': 42000,
                       'RB': 42000,
                       'NG': 10000,
                       'ED': 2500,
                       'E0': 2500,
                       'E2': 2500,
                       'E3': 2500,
                       'E4': 2500,
                       'E5': 2500,
                       'ES': 50,
                       'NQ': 20,
                       'AD': 100000,
                       'CD': 100000,
                       'EC': 125000,
                       'JY': 1.25,
                       'BP': 62500,
                       'MEC': 12500,
                       'FV': 1000,
                       'TU': 2000,
                       'TY': 1000,
                       'US': 1000,
                       'GC': 100,
                       'SI': 5000}

tick_size = {'C': 0.25,
             'W': 0.25,
             'KW': 0.25,
             'S': 0.25,
             'SM': 0.1,
             'BO': 0.01,
             'LC': 0.025,
             'LN': 0.025,
             'FC': 0.025,
             'EC': 0.00005,
             'BP': 0.0001,
             'AD': 0.0001,
             'CD': 0.00005,
             'JY': 5,
             'FV': 0.0078125,
             'TU': 0.0078125,
             'TY': 0.015625,
             'US': 0.03125,
             'GC': 0.1,
             'SI': 0.005,
             'ES': 0.25,
             'NQ': 0.25,
             'ED': 0.005,
             'CL': 0.01,
             'HO': 0.0001,
             'RB': 0.0001,
             'NG': 0.001,
             'SB': 0.01,
             'KC': 0.05,
             'CC': 1,
             'CT': 0.01,
             'OJ': 0.05,
             'B': 0.01}

option_tick_size = {'LC': 0.025,
                    'LN': 0.025,
                    'C': 0.125,
                    'W': 0.125,
                    'S': 0.125,
                    'BO': 0.005,
                    'SM': 0.05,
                    'ES': 0.25}

t_cost = {'CL': 0.65,
          'NG': 0.65,
          'B': 0.92,
          'HO': 0.65,
          'LC': 0.61,
          'LN': 0.61,
          'FC': 0.61,
          'RB': 0.65,
          'C': 0.61,
          'W': 0.61,
          'S': 0.61,
          'SM': 0.61,
          'BO': 0.61,
          'KW': 0.61,
          'ED': 0.29,
          'JY': 0.42,
          'EC': 0.42,
          'BP': 0.42,
          'AD': 0.42,
          'CD': 0.42,
          'US': 0.31,
          'TY': 0.31,
          'FV': 0.31,
          'TU': 0.31,
          'ES': 0.45,
          'NQ': 0.45,
          'SI': 0.65,
          'GC': 0.65,
          'KC': 2.22,
          'CC': 2.22,
          'SB': 2.22,
          'OJ': 2.22,
          'CT': 2.22}

t_cost_ib = {'S': 2.81,
             'C': 2.81,
             'KW': 2.81,
             'W': 2.81,
             'SM': 2.81,
             'BO': 2.81,
             'FC': 2.89,
             'LN': 2.89,
             'LC': 2.89,
             'CL': 2.36,
             'HO': 2.36,
             'NG': 2.36,
             'MEC': 0.33,
             'NQ': 2.05}

mini_contract_dictionary = {'MEC': 'EC'}

def get_t_cost(**kwargs):

    broker = kwargs['broker']
    ticker_head = kwargs['ticker_head']
    t_cost_out = np.nan

    if broker=='abn':
        t_cost_out = t_cost[ticker_head]
    elif broker=='ib':
        t_cost_out = t_cost_ib[ticker_head]

    return t_cost_out

def is_stockQ(ticker):

    is_stock_Q = False

    try:
        int(ticker[-4:])
    except:
        is_stock_Q = True

    return is_stock_Q

def get_contract_specs(ticker):

    is_stock_Q = is_stockQ(ticker)

    if is_stock_Q:
        output_dictionary = {'ticker_head': '',
            'ticker_year': np.nan,
            'ticker_month_str': '',
            'ticker_month_num': np.nan,
            'ticker_class': 'STOCK',
            'cont_indx': np.nan}
    else:
        ticker_head = ticker[:-5]
        output_dictionary = {'ticker_head': ticker_head,
         'ticker_year': int(ticker[-4:]),
         'ticker_month_str': ticker[-5],
         'ticker_month_num': letter_month_string.find(ticker[-5]) + 1,
         'ticker_class': ticker_class[mini_contract_dictionary.get(ticker_head, ticker_head)],
         'cont_indx': 100 * int(ticker[-4:]) + letter_month_string.find(ticker[-5]) + 1}

    return output_dictionary


def get_month_seperation_from_cont_indx(cont_indx1, cont_indx2):

    contract_month1 = cont_indx1%100
    contract_month2 = cont_indx2%100
    contract_year1 = m.floor(cont_indx1/100)
    contract_year2 = m.floor(cont_indx2/100)

    return 12*(contract_year1-contract_year2)+(contract_month1-contract_month2)


def get_cont_indx_from_month_seperation(cont_indx1,month_seperation):

    contract_month1 = cont_indx1 % 100
    contract_year1 = round(cont_indx1/100)

    total_months1 = 12*contract_year1 + contract_month1

    total_months2 = total_months1+month_seperation

    contract_month2 = total_months2 % 12

    if contract_month2 == 0:
        contract_month2 = 12
        contract_year2 = round(total_months2/12)-1
    else:
        contract_year2 = m.floor(total_months2/12)

    return 100*contract_year2+contract_month2










