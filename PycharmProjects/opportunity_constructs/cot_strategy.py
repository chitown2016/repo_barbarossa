
import contract_utilities.contract_lists as cl
import signals.futures_directional_signals as fds
import os.path
import ta.strategy as ts
import pandas as pd

def get_cot_sheet_4date(**kwargs):

    date_to = kwargs['date_to']
    output_dir = ts.create_strategy_output_dir(strategy_class='cot', report_date=date_to)

    if os.path.isfile(output_dir + '/summary.pkl'):
        cot_sheet = pd.read_pickle(output_dir + '/summary.pkl')
        return {'cot_sheet': cot_sheet,'success': True}

    ticker_head_list = ['LN', 'LC', 'FC', 'C', 'S', 'SM', 'BO', 'W', 'KW', 'SB', 'KC', 'CC', 'CT', 'OJ',
                       'CL', 'HO', 'RB', 'NG', 'ED', 'ES', 'NQ', 'EC', 'JY', 'AD', 'CD', 'BP',
                       'TY', 'US', 'FV', 'TU', 'GC', 'SI']

    signals_output = [fds.get_cot_strategy_signals(ticker_head=x, date_to=date_to) for x in ticker_head_list]

    cot_sheet = pd.DataFrame()
    cot_sheet['ticker_head'] = [x['ticker_head'] for x in signals_output if x['success']]
    cot_sheet['cot_index_slow'] = [x['cot_index_slow'] for x in signals_output if x['success']]

    cot_sheet.to_pickle(output_dir + '/summary.pkl')

    return {'cot_sheet': cot_sheet,'success': True}

