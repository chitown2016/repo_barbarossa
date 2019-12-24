
import contract_utilities.contract_lists as cl
import signals.option_signals as ops
import pandas as pd
import contract_utilities.contract_meta_info as cmi
import shared.calendar_utilities as cu
import my_sql_routines.my_sql_utilities as msu
import contract_utilities.expiration as exp
import get_price.get_futures_price as gfp
import datetime as dt
import numpy as np


def load_ticker_signals_4settle_date(**kwargs):

    if 'con' not in kwargs.keys():
        close_connection_before_exit = True
        con = msu.get_my_sql_connection(**kwargs)
        kwargs['con'] = con
    else:
        close_connection_before_exit = False
        con = kwargs['con']

    if not exp.is_business_day(double_date=kwargs['settle_date']):
        if close_connection_before_exit:
            con.close()
        return

    options_frame = cl.generate_liquid_options_list_dataframe(**kwargs)

    futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in cmi.get_tickerhead_list('cme_futures')}

    real_vol_output = [ops.calc_realized_vol_4options_ticker(ticker=x,
                                                             futures_data_dictionary=futures_data_dictionary,
                                                             settle_date=kwargs['settle_date'])
                       for x in options_frame['ticker']]

    delta_vol_output = [ops.calc_delta_vol_4ticker(ticker=x,
                                                   settle_date=kwargs['settle_date'],
                                                   delta_target=0.5) for x in options_frame['ticker']]

    volume_interest_output = [ops.calc_volume_interest_4ticker(ticker=x,settle_date=kwargs['settle_date']) for x in options_frame['ticker']]

    atm_vol_frame = pd.concat([options_frame, pd.DataFrame(delta_vol_output), pd.DataFrame(volume_interest_output)], axis=1)
    atm_vol_frame['real_vol'] = real_vol_output

    contract_specs_output = [cmi.get_contract_specs(x) for x in atm_vol_frame['ticker']]

    atm_vol_frame['ticker_head'] = [x['ticker_head'] for x in contract_specs_output]
    atm_vol_frame['ticker_month'] = [x['ticker_month_num'] for x in contract_specs_output]
    atm_vol_frame['ticker_year'] = [x['ticker_year'] for x in contract_specs_output]

    column_names = atm_vol_frame.columns.tolist()

    ticker_indx = column_names.index('ticker')
    cal_dte_indx = column_names.index('cal_dte')
    tr_dte_indx = column_names.index('tr_dte')
    ticker_head_indx = column_names.index('ticker_head')
    ticker_month_indx = column_names.index('ticker_month')
    ticker_year_indx = column_names.index('ticker_year')
    imp_vol_indx = column_names.index('delta_vol')
    strike_indx = column_names.index('strike')
    theta_indx = column_names.index('theta')
    open_interest_indx = column_names.index('open_interest')
    volume_indx = column_names.index('volume')
    real_vol_indx = column_names.index('real_vol')

    now = dt.datetime.now()
    settle_datetime = cu.convert_doubledate_2datetime(kwargs['settle_date'])

    tuples = [tuple([x[ticker_indx], x[ticker_head_indx], x[ticker_month_indx] , x[ticker_year_indx],
                     None if np.isnan(x[cal_dte_indx]) else x[cal_dte_indx],
                     None if np.isnan(x[tr_dte_indx]) else x[tr_dte_indx],
                     None if np.isnan(x[imp_vol_indx]) else 100*x[imp_vol_indx], 0.5,
                     None if np.isnan(x[theta_indx]) else x[theta_indx],
                     None if np.isnan(x[strike_indx]) else x[strike_indx],
                     None if np.isnan(x[real_vol_indx]) else x[real_vol_indx],
                     None if np.isnan(x[open_interest_indx]) else x[open_interest_indx],
                     None if np.isnan(x[volume_indx]) else x[volume_indx],
                     settle_datetime.date(), now, now]) for x in atm_vol_frame.values]

    column_str = "ticker, ticker_head, ticker_month, ticker_year, " \
                 " cal_dte, tr_dte, imp_vol, delta, theta, strike, " \
                 "close2close_vol20, open_interest, volume, price_date, created_date, last_updated_date"

    insert_str = ("%s, " * len(column_str.split(',')))[:-2]
    final_str = "REPLACE INTO option_ticker_indicators (%s) VALUES (%s)" % (column_str, insert_str)

    msu.sql_execute_many_wrapper(final_str=final_str, tuples=tuples, con=con)

    load_pnls_4settle_date(**kwargs)

    if close_connection_before_exit:
            con.close()


def load_pnls_4settle_date(**kwargs):

    if 'con' not in kwargs.keys():
        close_connection_before_exit = True
        con = msu.get_my_sql_connection(**kwargs)
        kwargs['con'] = con
    else:
        close_connection_before_exit = False
        con = kwargs['con']

    if not exp.is_business_day(double_date=kwargs['settle_date']):
        if close_connection_before_exit:
            con.close()
        return

    settle_date_ex = exp.doubledate_shift_bus_days(double_date=kwargs['settle_date'],shift_in_days=7)

    indicator_frame = ops.get_option_ticker_indicators(settle_date=settle_date_ex,delta=0.5,
                                                      column_names=['id','ticker','price_date','tr_dte','ticker_head'])

    if indicator_frame.empty:
        if close_connection_before_exit:
            con.close()
        return

    indicator_frame.reset_index(drop=True,inplace=True)

    delta_vol_output = [ops.calc_delta_vol_4ticker(ticker=x,
                                                   settle_date=settle_date_ex,
                                                   delta_target=0.5) for x in indicator_frame['ticker']]

    delta_vol_output = pd.DataFrame(delta_vol_output)

    indicator_frame['option_pnl5'] = delta_vol_output['option_pnl5']
    indicator_frame['delta_pnl5'] = delta_vol_output['delta_pnl5']

    indicator_frame.dropna(inplace=True)

    column_names = indicator_frame.columns.tolist()

    #indicator_frame['option_pnl5'] = indicator_frame['option_pnl5'].astype(object)
    #indicator_frame['delta_pnl5'] = indicator_frame['delta_pnl5'].astype(object)

    id_indx = column_names.index('id')
    option_pnl_indx = column_names.index('option_pnl5')
    delta_pnl_indx = column_names.index('delta_pnl5')

    tuples = [tuple([x[option_pnl_indx],
                    x[delta_pnl_indx],
                    x[id_indx]]) for x in indicator_frame.values]

    final_str = "UPDATE option_ticker_indicators SET option_pnl5 = %s,  delta_pnl5 = %s WHERE id=%s"

    msu.sql_execute_many_wrapper(final_str=final_str, tuples=tuples, con=con)

    if close_connection_before_exit:
            con.close()




