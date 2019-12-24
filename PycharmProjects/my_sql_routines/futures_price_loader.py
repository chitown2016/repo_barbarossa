__author__ = 'kocat_000'

import sys
import quandl_data.get_data_quandl as gdq
import my_sql_routines.my_sql_utilities as msu
import contract_utilities.contract_meta_info as cmi
import get_price.get_futures_price as gfp
import contract_utilities.contract_lists as cl
import shared.calendar_utilities as cu
import contract_utilities.expiration as exp
import read_exchange_files.process_cme_futures as pcf
import pandas as pd
import numpy as np
import datetime
from pandas.tseries.offsets import CustomBusinessDay


def load_price_data_4ticker(load_price_data_input):

    ticker = load_price_data_input['ticker']
    expiration_date = load_price_data_input['expiration_date']
    data_vendor_id = load_price_data_input['data_vendor_id']
    symbol_id = load_price_data_input['symbol_id']

    quandl_input = {'ticker': ticker}

    if 'date_to' in load_price_data_input.keys():
        quandl_input['date_to'] = load_price_data_input['date_to']

    if 'date_from' in load_price_data_input.keys() and load_price_data_input['date_from'] is not None:
        quandl_input['date_from'] = load_price_data_input['date_from']

    quandl_out = gdq.get_daily_historic_data_quandl(**quandl_input)

    if not quandl_out['success']:
        return

    price_data = quandl_out['data_out']

    if price_data.empty:
        print('Empty Results For ' + ticker)
        return

    contract_specs_output = cmi.get_contract_specs(ticker)

    if contract_specs_output['ticker_head'] == 'JY':
        price_multiplier = 10
    else:
        price_multiplier = 1

    bday_us = CustomBusinessDay(calendar=exp.get_calendar_4ticker_head(contract_specs_output['ticker_head']))
    dts = pd.date_range(start=price_data.index[0], end=expiration_date, freq=bday_us)
    dts = [x.date() for x in dts]

    now = datetime.datetime.now()

    price_data['price_date'] = pd.Series(price_data.index, index=price_data.index)

    column_names = price_data.columns.tolist()

    open_indx = column_names.index('Open')
    high_indx = column_names.index('High')
    low_indx = column_names.index('Low')
    settle_indx = column_names.index('Settle')
    volume_indx = column_names.index('Volume')

    if 'Previous Day Open Interest' in column_names:
        interest_indx = column_names.index('Previous Day Open Interest')
    else:
        interest_indx = column_names.index('Open Interest')

    date_indx = column_names.index('price_date')

    tuples = [tuple([data_vendor_id, symbol_id,
                     contract_specs_output['ticker_head'],
                     contract_specs_output['ticker_month_num'],
                     x[date_indx].to_pydatetime().date(),
                     (expiration_date-x[date_indx].to_pydatetime().date()).days,
                     len([y for y in dts if y > x[date_indx].to_pydatetime().date()]),
                     now, now,
                     None if pd.isnull(x[open_indx]) else price_multiplier*x[open_indx],
                     None if pd.isnull(x[high_indx]) else price_multiplier*x[high_indx],
                     None if pd.isnull(x[low_indx]) else price_multiplier*x[low_indx],
                     None if pd.isnull(x[settle_indx]) else price_multiplier*x[settle_indx],
                     None if pd.isnull(x[volume_indx]) else x[volume_indx],
                     None if pd.isnull(x[interest_indx]) else x[interest_indx]]) for x in price_data.values]

    column_str = "data_vendor_id, symbol_id, ticker_head, ticker_month, price_date,cal_dte, tr_dte, created_date,last_updated_date, open_price, high_price, low_price, close_price, volume, open_interest"
    insert_str = ("%s, " * 15)[:-2]
    final_str = "REPLACE INTO daily_price (%s) VALUES (%s)" % (column_str, insert_str)

    con = msu.get_my_sql_connection(**load_price_data_input)

    msu.sql_execute_many_wrapper(final_str=final_str, tuples=tuples, con=con)

    if 'con' not in load_price_data_input.keys():
        con.close()


def load_entire_history(**kwargs):

    contract_list = cl.get_db_contract_list_filtered(instrument='futures',
                                                     ticker_year_from=1980, ticker_year_to=2022, **kwargs)

    # length of contract_list is 6594

    if 'start_indx' in kwargs.keys():
        start_indx = kwargs['start_indx']
    else:
        start_indx = 0

    if 'end_indx' in kwargs.keys():
        end_indx = kwargs['end_indx']
    else:
        end_indx = len(contract_list)

    con = msu.get_my_sql_connection(**kwargs)
    load_price_data_input = dict()
    load_price_data_input['con'] = con

    if 'date_to' in kwargs.keys():
        load_price_data_input['date_to'] = kwargs['date_to']

    for i in range(start_indx, end_indx):
        load_price_data_input['symbol_id'] = contract_list[i][0]
        load_price_data_input['data_vendor_id'] = 1
        load_price_data_input['ticker'] = contract_list[i][1]
        load_price_data_input['expiration_date'] = contract_list[i][2]

        load_price_data_4ticker(load_price_data_input)
        print('No : ' + str(i) + ', ' + contract_list[i][1] + ' loaded')

    if 'con' not in kwargs.keys():
        con.close()


def update_futures_price_database(**kwargs):

    import time
    run_date = int(time.strftime('%Y%m%d'))
    early_start_date = cu.doubledate_shift(run_date, 15)   #45
    con = msu.get_my_sql_connection(**kwargs)

    contract_list = []

    for ticker_head in cmi.futures_butterfly_strategy_tickerhead_list:
        for ticker_month in cmi.futures_contract_months[ticker_head]:
            ticker_month_num = cmi.letter_month_string.find(ticker_month)+1
            max_cal_dte = cmi.get_max_cal_dte(ticker_head=ticker_head, ticker_month=ticker_month_num)
            contract_list.extend(cl.get_db_contract_list_filtered(expiration_date_from=early_start_date,
                                                            expiration_date_to=cu.doubledate_shift(run_date, -max_cal_dte),
                                                            ticker_head=ticker_head, ticker_month=ticker_month_num, con=con,
                                                                  instrument='futures'))

    date_from_list = [gfp.get_futures_last_price_date_4ticker(ticker=x[1], con=con) for x in contract_list]

    load_price_data_input = dict()
    load_price_data_input['con'] = con

    for i in range(len(contract_list)):
        #print(contract_list[i][1])
        load_price_data_input['symbol_id'] = contract_list[i][0]
        load_price_data_input['data_vendor_id'] = 1
        load_price_data_input['ticker'] = contract_list[i][1]
        load_price_data_input['expiration_date'] = contract_list[i][2]
        load_price_data_input['date_from'] = date_from_list[i]

        load_price_data_4ticker(load_price_data_input)
        print('No : ' + str(i) + ', ' + contract_list[i][1] + ' loaded')

    if 'con' not in kwargs.keys():
        con.close()


def update_futures_price_database_from_cme_file(**kwargs):

    ticker_head_list = cmi.cme_futures_tickerhead_list

    import time
    con = msu.get_my_sql_connection(**kwargs)

    if 'settle_date' in kwargs.keys():
        run_date = kwargs['settle_date']
    else:
        run_date = int(time.strftime('%Y%m%d'))

    #run_date = 20160225
    data_vendor_id = 2
    now = datetime.datetime.now()
    run_datetime = cu.convert_doubledate_2datetime(run_date)

    for ticker_head in ticker_head_list:
        #print(ticker_head)

        contract_list = []

        bday_us = CustomBusinessDay(calendar=exp.get_calendar_4ticker_head(ticker_head))

        if not exp.is_business_day(double_date=run_date):
            continue

        cme_output = pcf.process_cme_futures_4tickerhead(ticker_head=ticker_head, report_date=run_date)
        settle_frame = cme_output['settle_frame']

        if settle_frame.empty:
            continue

        for ticker_month in cmi.futures_contract_months[ticker_head]:
            ticker_month_num = cmi.letter_month_string.find(ticker_month)+1
            max_cal_dte = cmi.get_max_cal_dte(ticker_head=ticker_head, ticker_month=ticker_month_num)

            contract_list.extend(cl.get_db_contract_list_filtered(expiration_date_from=run_date,
                                                            expiration_date_to=cu.doubledate_shift(run_date, -max_cal_dte),
                                                            ticker_head=ticker_head, ticker_month=ticker_month_num, con=con,
                                                                  instrument='futures'))

        contract_frame = pd.DataFrame(contract_list, columns=['symbol_id', 'ticker', 'expiration_date'])
        merged_frame = pd.merge(contract_frame,settle_frame, how='inner', on='ticker')
        merged_frame.sort_values('expiration_date', ascending=True, inplace=True)

        column_names = merged_frame.columns.tolist()

        symbol_id_indx = column_names.index('symbol_id')
        ticker_month_indx = column_names.index('ticker_month')
        open_indx = column_names.index('open')
        high_indx = column_names.index('high')
        low_indx = column_names.index('low')
        settle_indx = column_names.index('settle')
        volume_indx = column_names.index('volume')
        interest_indx = column_names.index('interest')
        expiration_indx = column_names.index('expiration_date')

        dts = pd.date_range(start=run_datetime, end=merged_frame['expiration_date'].iloc[-1], freq=bday_us)

        tuples = [tuple([data_vendor_id, x[symbol_id_indx],
                     ticker_head,
                     x[ticker_month_indx],
                     run_datetime.date(),
                    (x[expiration_indx]-run_datetime.date()).days,
                     len([y for y in dts if y.to_pydatetime().date() < x[expiration_indx]]),
                     now, now,
                     None if np.isnan(x[open_indx]) else x[open_indx],
                     None if np.isnan(x[high_indx]) else x[high_indx],
                     None if np.isnan(x[low_indx]) else x[low_indx],
                     None if np.isnan(x[settle_indx]) else x[settle_indx],
                     None if np.isnan(x[volume_indx]) else x[volume_indx],
                     None if np.isnan(x[interest_indx]) else x[interest_indx]]) for x in merged_frame.values]

        column_str = "data_vendor_id, symbol_id, ticker_head, ticker_month, price_date,cal_dte, tr_dte, created_date,last_updated_date, open_price, high_price, low_price, close_price, volume, open_interest"
        insert_str = ("%s, " * 15)[:-2]
        final_str = "REPLACE INTO daily_price (%s) VALUES (%s)" % (column_str, insert_str)
        msu.sql_execute_many_wrapper(final_str=final_str, tuples=tuples, con=con)

    if 'con' not in kwargs.keys():
        con.close()




