

import contract_utilities.contract_meta_info as cmi
import signals.futures_directional_signals as fds
import ta.strategy as ts
import pandas as pd
import os.path


def generate_fm_sheet_4date(**kwargs):

    date_to = kwargs['date_to']

    output_dir = ts.create_strategy_output_dir(strategy_class='fm', report_date=date_to)

    if os.path.isfile(output_dir + '/summary.pkl'):
        futures = pd.read_pickle(output_dir + '/summary.pkl')
        return {'futures': futures,'success': True}

    ticker_head_list = list(set(cmi.cme_futures_tickerhead_list)|set(cmi.futures_butterfly_strategy_tickerhead_list))
    ticker_head_list.remove('B')

    signals_list = [fds.get_fm_signals(ticker_head=x, date_to=date_to) for x in ticker_head_list]

    futures = pd.DataFrame()

    futures['ticker'] = [x['ticker'] for x in signals_list]
    futures['tickerHead'] = [x['tickerHead'] for x in signals_list]
    futures['comm_indx_156'] = [x['comm_indx_156'] for x in signals_list]
    futures['spec_indx_156'] = [x['spec_indx_156'] for x in signals_list]
    futures['small_indx_156'] = [x['small_indx_156'] for x in signals_list]
    futures['willco'] = [x['willco'] for x in signals_list]
    futures['s_oi'] = [x['s_oi'] for x in signals_list]
    futures['trend_direction'] = [x['trend_direction'] for x in signals_list]
    futures['curve_slope'] = [x['curve_slope'] for x in signals_list]
    futures['notes'] = [x['notes'] for x in signals_list]
    futures['technicalNotes'] = [x['technicalNotes'] for x in signals_list]




    writer = pd.ExcelWriter(output_dir + '/summary.xlsx', engine='xlsxwriter')
    futures.to_excel(writer, sheet_name='all')
    writer.save()

    futures.to_pickle(output_dir + '/summary.pkl')

    return {'futures': futures,'success': True}

