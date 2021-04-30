

import get_price.get_futures_price as gfp
import futures_charts.generic_charts as gc
import contract_utilities.contract_meta_info as cmi
import fundamental_data.cot_data as cot
import datetime as dt
import pandas as pd


def get_outright_chart(**kwargs):

    date_to = kwargs['date_to']
    date_from = kwargs['date_from']


    ticker_head = kwargs['ticker_head']
    data_out = gfp.get_futures_price_preloaded(ticker_head=ticker_head, settle_date_to=date_to,
                                                   settle_date_from=date_from)

    if ticker_head=='ED':
        data_out = data_out[data_out['tr_dte'] >= 500]
    else:
        data_out = data_out[data_out['tr_dte'] >= 20]


    data_out.sort_values(['settle_date', 'tr_dte'], ascending=[True, True], inplace=True)
    data_out.drop_duplicates(subset=['settle_date'], keep='first', inplace=True)

    data_out.set_index('settle_date', drop=True, inplace=True)
    weekly_data_obj = data_out.resample('W-FRI', axis=0)

    weekly_data = pd.DataFrame()
    weekly_data['open'] = weekly_data_obj['open_price'].first()
    weekly_data['close'] = weekly_data_obj['close_price'].last()
    weekly_data['high'] = weekly_data_obj['high_price'].max()
    weekly_data['low'] = weekly_data_obj['low_price'].min()

    final_data = weekly_data
    final_data['ma52'] = final_data['close'].rolling(52, min_periods=40).mean()

    if 'signal_list' in kwargs.keys():
        signal_list = kwargs['signal_list']
        if signal_list[0] in ['comm_net', 'comm_indx_156','small_net','small_indx_156','spec_net', 'spec_indx_156','oi','s_oi','comm_short_oi','willco']:
            cot_output = cot.get_cot_signals(ticker_head=ticker_head, date_to=date_to)
            cot_output['settle_date'] = [x + dt.timedelta(days=3) for x in cot_output['settle_date']]
            final_data = pd.merge(weekly_data, cot_output, left_index=True, right_on='settle_date', how='inner')
            final_data.set_index('settle_date',drop=True,inplace=True)
            if len(signal_list)>1:
                gc.get_candlestick_chart(data2plot=final_data, indicator_list=[signal_list[0]], main_panel_indicator_list=[signal_list[1]], num_panels=2)
            else:
                gc.get_candlestick_chart(data2plot=final_data, indicator_list=[signal_list[0]],num_panels=2)

    else:
        gc.get_candlestick_chart(data2plot=final_data)

    return final_data

