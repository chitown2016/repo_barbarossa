
import read_exchange_files.process_cme_options as pco
import read_exchange_files.read_cme_files as rcf
import contract_utilities.expiration as exp
import my_sql_routines.my_sql_utilities as msu
import contract_utilities.contract_meta_info as cmi
import my_sql_routines.my_sql_utilities as msu
import shared.calendar_utilities as cu
import contract_utilities.contract_lists as cl
from pandas.tseries.offsets import CustomBusinessDay
import pandas as pd
import numpy as np
import datetime as dt
import time
import shared.directory_names as dn


def update_options_price_database_from_cme_files_4ticker(**kwargs):

    ticker = kwargs['ticker']
    print(ticker)
    contract_specs_output = cmi.get_contract_specs(ticker)
    ticker_head = contract_specs_output['ticker_head']
    ticker_month_num = contract_specs_output['ticker_month_num']
    ticker_year = contract_specs_output['ticker_year']

    if 'settle_date' in kwargs.keys():
        settle_date = kwargs['settle_date']
        kwargs['report_date'] = settle_date
    else:
        settle_date = int(time.strftime('%Y%m%d'))
        kwargs['settle_date'] = settle_date
        kwargs['report_date'] = settle_date

    if not exp.is_business_day(double_date=settle_date, reference_tickerhead=ticker_head):
        return

    if 'expiration_date' in kwargs.keys():
        expiration_date = kwargs['expiration_date']
    else:
        expiration_date = exp.get_options_expiration(ticker)
        expiration_date = expiration_date.date()

    settle_datetime = cu.convert_doubledate_2datetime(settle_date)

    if 'cal_dte' in kwargs.keys():
        cal_dte = kwargs['cal_dte']
    else:
        cal_dte = (expiration_date-settle_datetime.date()).days

    if 'tr_dte' in kwargs.keys():
        tr_dte = kwargs['tr_dte']
    else:
        bday_us = CustomBusinessDay(calendar=exp.get_calendar_4ticker_head(ticker_head))
        dts = pd.date_range(start=settle_datetime, end=expiration_date, freq=bday_us)
        tr_dte = len([x for x in dts if x.to_pydatetime().date() < expiration_date])

    data_vendor_id = 2
    now = dt.datetime.now()
    con = msu.get_my_sql_connection(**kwargs)

    process_output = pco.process_cme_options_4ticker(**kwargs)

    if process_output['success']:
        settle_frame = process_output['settle_frame']
    else:
        if 'con' not in kwargs.keys():
            con.close()
        return

    column_names = settle_frame.columns.tolist()

    option_type_indx = column_names.index('option_type')
    strike_indx = column_names.index('strike')
    settle_indx = column_names.index('settle')
    volume_indx = column_names.index('volume')
    interest_indx = column_names.index('interest')

    tuples = [tuple([data_vendor_id, ticker_head, ticker_month_num, ticker_year,
                     ticker, x[option_type_indx],x[strike_indx],settle_datetime.date(),
                     cal_dte, tr_dte, now, now,
                    None if np.isnan(x[settle_indx]) else x[settle_indx],
                    None if np.isnan(x[volume_indx]) else x[volume_indx],
                    None if np.isnan(x[interest_indx]) else x[interest_indx]]) for x in settle_frame.values]

    column_str = "data_vendor_id, ticker_head, ticker_month, ticker_year, ticker, " \
                 " option_type, strike, price_date, cal_dte, tr_dte, " \
                 " created_date,last_updated_date, close_price, volume, open_interest"

    insert_str = ("%s, " * len(column_str.split(',')))[:-2]
    final_str = "REPLACE INTO daily_option_price (%s) VALUES (%s)" % (column_str, insert_str)
    msu.sql_execute_many_wrapper(final_str=final_str, tuples=tuples, con=con)

    if 'con' not in kwargs.keys():
        con.close()


def update_options_price_database_from_cme_files(**kwargs):

    if 'settle_date' in kwargs.keys():
        settle_date = kwargs['settle_date']
    else:
        settle_date = int(time.strftime('%Y%m%d'))
        kwargs['settle_date'] = settle_date

    if 'con' not in kwargs.keys():
        close_connection_before_exit = True
        con = msu.get_my_sql_connection(**kwargs)
        kwargs['con'] = con
    else:
        close_connection_before_exit = False

    if not exp.is_business_day(double_date=settle_date):
        if close_connection_before_exit:
            con.close()
        return

    data_read_out = {}
    data_read_out['commodity'] = rcf.read_cme_settle_txt_files(file_name='commodity', report_date=settle_date)
    data_read_out['equity'] = rcf.read_cme_settle_txt_files(file_name='equity', report_date=settle_date)
    data_read_out['fx'] = rcf.read_cme_settle_txt_files(file_name='fx', report_date=settle_date)
    data_read_out['interest_rate'] = rcf.read_cme_settle_txt_files(file_name='interest_rate', report_date=settle_date)

    options_frame = cl.generate_liquid_options_list_dataframe(**kwargs)

    for i in range(len(options_frame.index)):

        ticker = options_frame['ticker'].iloc[i]
        expiration_date = options_frame['expiration_date'].iloc[i]

        update_options_price_database_from_cme_files_4ticker(ticker=ticker,
                                                             expiration_date=expiration_date,
                                                             data_read_out=data_read_out,
                                                             **kwargs)

    if close_connection_before_exit:
        con.close()
