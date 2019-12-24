
import my_sql_routines.my_sql_utilities as msu
import option_models.skew_calc as osc
import pandas as pd
import datetime as dt
import contract_utilities.contract_meta_info as cmi
import contract_utilities.contract_lists as cl
import shared.calendar_utilities as cu
import numpy as np


def update_options_greek_database_4ticker(**kwargs):

    if 'con' not in kwargs.keys():
        con = msu.get_my_sql_connection(**kwargs)
        kwargs['con'] = con
        close_connection_before_exit = True
    else:
        close_connection_before_exit = False
        con = kwargs['con']

    ticker = kwargs['ticker']
    #print(ticker)

    option_greeks = osc.cal_greeks_4option_maturity(**kwargs)

    #print(option_greeks)

    if option_greeks.empty:
        return

    column_names = option_greeks.columns.tolist()

    option_type_indx = column_names.index('option_type')
    strike_indx = column_names.index('strike')
    settle_indx = column_names.index('close_price')
    volume_indx = column_names.index('volume')
    interest_indx = column_names.index('open_interest')

    delta_indx = column_names.index('delta')
    gamma_indx = column_names.index('gamma')
    implied_vol_indx = column_names.index('implied_vol')
    theta_indx = column_names.index('theta')
    vega_indx = column_names.index('vega')

    cal_dte = int(option_greeks['cal_dte'].iloc[0])
    tr_dte = int(option_greeks['tr_dte'].iloc[0])

    data_vendor_id = 2
    now = dt.datetime.now()
    settle_datetime = cu.convert_doubledate_2datetime(kwargs['settle_date'])

    contract_specs_output = cmi.get_contract_specs(ticker)

    x = option_greeks.values[0]

    option_greeks['close_price'] = option_greeks['close_price'].astype('float64')
    option_greeks['volume'] = option_greeks['volume'].astype('int_')
    option_greeks['open_interest'] = option_greeks['open_interest'].astype('int_')

    tuples = [tuple([data_vendor_id,
                     contract_specs_output['ticker_head'],
                     contract_specs_output['ticker_month_num'],
                     contract_specs_output['ticker_year'],
                     ticker, x[option_type_indx], x[strike_indx],settle_datetime.date(),
                     cal_dte, tr_dte, now, now,
                     None if np.isnan(x[settle_indx]) else x[settle_indx],
                     None if np.isnan(x[implied_vol_indx]) else x[implied_vol_indx],
                     None if np.isnan(x[delta_indx]) else x[delta_indx],
                     None if np.isnan(x[gamma_indx]) else x[gamma_indx],
                     None if np.isnan(x[theta_indx]) else x[theta_indx],
                     None if np.isnan(x[vega_indx]) else x[vega_indx],
                     None if np.isnan(x[volume_indx]) else int(x[volume_indx]),
                     None if np.isnan(x[interest_indx]) else int(x[interest_indx])]) for x in option_greeks.values]

    column_str = "data_vendor_id, ticker_head, ticker_month, ticker_year, ticker, " \
                 "option_type, strike, price_date, cal_dte, tr_dte, " \
                 "created_date,last_updated_date, close_price, imp_vol, delta, gamma, theta, vega, " \
                 "volume, open_interest"

    insert_str = ("%s, " * len(column_str.split(',')))[:-2]
    final_str = "REPLACE INTO daily_option_price (%s) VALUES (%s)" % (column_str, insert_str)

    msu.sql_execute_many_wrapper(final_str=final_str, tuples=tuples, con=con)

    if close_connection_before_exit:
        con.close()


def update_options_greeks_4date(**kwargs):

    options_frame = cl.generate_liquid_options_list_dataframe(**kwargs)
    [update_options_greek_database_4ticker(ticker=x, **kwargs) for x in options_frame['ticker']]




