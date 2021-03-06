__author__ = 'kocat_000'

import sys
sys.path.append(r'C:\Users\kocat_000\quantFinance\PycharmProjects')
import contract_utilities.contract_meta_info as cmf
import shared.calendar_utilities as cu
import my_sql_routines.my_sql_utilities as msu
import pandas as pd
from pandas.tseries.holiday import get_calendar, HolidayCalendarFactory,\
    AbstractHolidayCalendar, USMartinLutherKingJr, USPresidentsDay, GoodFriday, USMemorialDay, USLaborDay, \
    USThanksgivingDay, Holiday, nearest_workday, weekend_to_monday
from pandas.tseries.offsets import CustomBusinessDay
from dateutil.relativedelta import MO
import shared.constants as const
import time as tm
import datetime as dt
import math as m

def get_options_expiration(ticker):

    contract_specs = cmf.get_contract_specs(ticker)
    ticker_head = contract_specs['ticker_head']
    ticker_class = cmf.ticker_class[ticker_head]
    ticker_year = contract_specs['ticker_year']
    ticker_month_num = contract_specs['ticker_month_num']

    bday_us = CustomBusinessDay(calendar=get_calendar_4ticker_head(ticker_head))

    if ticker_class == 'Ag' or ticker_class == 'Treasury':

        prev_output = get_prev_ticker_year_month(ticker_year, ticker_month_num)
        ticker_year_prev = prev_output['ticker_year_prev']
        ticker_month_num_prev = prev_output['ticker_month_num_prev']

        dts = pd.date_range(pd.datetime(ticker_year_prev, ticker_month_num_prev, 1), periods=32)
        dts = dts[dts.month == ticker_month_num_prev]

        bu_dts = pd.date_range(pd.datetime(ticker_year_prev, ticker_month_num_prev, 1), periods=32, freq=bday_us)
        bu_dts = bu_dts[bu_dts.month == ticker_month_num_prev]

        fridays = dts[dts.dayofweek == 4]

        selected_fridays = fridays[fridays <= bu_dts[-3]]

        if selected_fridays[-1] in bu_dts:
            dts = selected_fridays
        else:
            dts = bu_dts[bu_dts < selected_fridays[-1]]
        exp_indx = -1

    elif ticker_head == 'LC':

        bu_dts = pd.date_range(pd.datetime(ticker_year, ticker_month_num, 1), periods=32, freq=bday_us)
        fridays = bu_dts[bu_dts.dayofweek == 4]
        dts = fridays
        exp_indx = 0
    elif ticker_head == 'LN':
        dts = pd.date_range(pd.datetime(ticker_year,ticker_month_num, 1), periods=15, freq=bday_us)
        exp_indx = 9
    elif ticker_head == 'CL':
        prev_output = get_prev_ticker_year_month(ticker_year, ticker_month_num)
        ticker_year_prev = prev_output['ticker_year_prev']
        ticker_month_num_prev = prev_output['ticker_month_num_prev']
        dts = pd.date_range(start=pd.datetime(ticker_year_prev,ticker_month_num_prev,1), end=pd.datetime(ticker_year_prev, ticker_month_num_prev, 25), freq=bday_us)
        exp_indx = -7
    elif ticker_head == 'NG':
        prev_output = get_prev_ticker_year_month(ticker_year,ticker_month_num)
        ticker_year_prev = prev_output['ticker_year_prev']
        ticker_month_num_prev = prev_output['ticker_month_num_prev']
        dts = pd.date_range(start=pd.datetime(ticker_year_prev, ticker_month_num_prev, 1), end=pd.datetime(ticker_year, ticker_month_num, 1), freq=bday_us)
        if pd.datetime(ticker_year,ticker_month_num,1) in dts:
            exp_indx = -5
        else:
            exp_indx = -4
    elif ticker_head in ['ES', 'NQ']:
        dts = pd.date_range(pd.datetime(ticker_year, ticker_month_num, 1), periods=32)
        dts = dts[dts.dayofweek == 4]
        exp_indx = 2
    elif ticker_class == 'FX':
        dts = pd.date_range(pd.datetime(ticker_year, ticker_month_num, 1), periods=32)
        fridays = dts[dts.dayofweek == 4]
        wednesdays = dts[dts.dayofweek == 2]
        dts = fridays[fridays < wednesdays[2]]
        exp_indx = -2
    elif ticker_class == 'STIR':

        if (ticker_month_num % 3) or ticker_head in ['E0', 'E2', 'E3', 'E4', 'E5']:
            dts = pd.date_range(pd.datetime(ticker_year, ticker_month_num, 1), periods=32)
            fridays = dts[dts.dayofweek == 4]
            wednesdays = dts[dts.dayofweek == 2]
            dts = fridays[fridays < wednesdays[2]]
            exp_indx = -1
        else:
            dts = pd.date_range(pd.datetime(ticker_year, ticker_month_num, 1), periods=32)
            dts = dts[dts.month == ticker_month_num]
            wednesday_list = dts[dts.dayofweek == 2]
            dts = pd.date_range(start=pd.datetime(ticker_year, ticker_month_num, 1), end=wednesday_list[2], freq=bday_us)
            exp_indx = -3
    elif ticker_class == 'Metal':

        prev_output = get_prev_ticker_year_month(ticker_year, ticker_month_num)
        ticker_year_prev = prev_output['ticker_year_prev']
        ticker_month_num_prev = prev_output['ticker_month_num_prev']

        dts = pd.date_range(pd.datetime(ticker_year_prev, ticker_month_num_prev, 1), periods=32)
        dts = dts[dts.month == ticker_month_num_prev]

        bu_dts = pd.date_range(pd.datetime(ticker_year_prev, ticker_month_num_prev, 1), periods=32, freq=bday_us)
        bu_dts = bu_dts[bu_dts.month == ticker_month_num_prev]

        cal_days_after = dts[bu_dts[-4] < dts]

        #return {'dts': dts, 'bu_dts': bu_dts}

        if cal_days_after[0] in bu_dts:
            dts = bu_dts
            exp_indx = -4
        else:
            cal_days_after = dts[bu_dts[-5] < dts]

            if cal_days_after[0] in bu_dts:
                dts = bu_dts
                exp_indx = -5
            else:
                dts = bu_dts
                exp_indx = -6

    return dts[exp_indx].to_pydatetime()


def get_expiration_from_db(**kwargs):

    # instrument=options or futures

    con = msu.get_my_sql_connection(**kwargs)

    sql_query = 'SELECT expiration_date FROM futures_master.symbol WHERE instrument=\'' + kwargs['instrument'] + '\' and ticker=\'' + kwargs['ticker'] + '\''

    cur = con.cursor()
    cur.execute(sql_query)
    data = cur.fetchall()

    if 'con' not in kwargs.keys():
        con.close()

    return data[0][0]

def get_futures_expiration(ticker):
    contract_specs = cmf.get_contract_specs(ticker)
    ticker_head = contract_specs['ticker_head']
    ticker_class = cmf.ticker_class[ticker_head]
    ticker_year = contract_specs['ticker_year']
    ticker_month_num = contract_specs['ticker_month_num']

    bday_us = CustomBusinessDay(calendar=get_calendar_4ticker_head(ticker_head))

    if ticker_class == 'Ag':
        dts = pd.date_range(start=pd.datetime(ticker_year,ticker_month_num,10), end=pd.datetime(ticker_year, ticker_month_num, 15), freq=bday_us)
        if pd.datetime(ticker_year,ticker_month_num,15) in dts:
            exp_indx = -2
        else:
            exp_indx = -1
    elif ticker_head == 'LC':
        dts = pd.date_range(pd.datetime(ticker_year,ticker_month_num, 1), periods=32, freq=bday_us)
        dts = dts[dts.month==ticker_month_num]
        exp_indx = -1
    elif ticker_head == 'LN':
        dts = pd.date_range(pd.datetime(ticker_year,ticker_month_num, 1), periods=15, freq=bday_us)
        exp_indx = 9
    elif ticker_head == 'FC':
        prev_output = get_prev_ticker_year_month(ticker_year,ticker_month_num)
        ticker_year_prev = prev_output['ticker_year_prev']
        ticker_month_num_prev = prev_output['ticker_month_num_prev']

        dts = pd.date_range(pd.datetime(ticker_year_prev,ticker_month_num_prev, 1), periods=62, freq=bday_us)
        dts = dts[dts.month <= ticker_month_num]

        thursday_list = []
        thursday_no = []
        num_thursdays = 0
        holiday_requirement_dummy = []

        for i in range(len(dts)):

            if dts.dayofweek[i]==3:
                thursday_list.append(dts[i])
                if dts.month[i]==ticker_month_num:
                    num_thursdays += 1
                    thursday_no.append(num_thursdays)
                else:
                    thursday_no.append(-1)


                if(dts.dayofweek[i-1]==2)and(dts.dayofweek[i-2]==1)and(dts.dayofweek[i-3]==0)and(dts.dayofweek[i-4]==4):
                    holiday_requirement_dummy.append(True)
                else:
                    holiday_requirement_dummy.append(False)

        if ticker_month_num is not 11:
            dts = [thursday_list[i] for i in range(len(thursday_list)) if holiday_requirement_dummy[i]]
        else:
            dts = [thursday_list[i] for i in range(len(thursday_list)) if holiday_requirement_dummy[i] and thursday_no[i] < 4]
        exp_indx = -1
    elif ticker_head == 'CL':
        prev_output = get_prev_ticker_year_month(ticker_year,ticker_month_num)
        ticker_year_prev = prev_output['ticker_year_prev']
        ticker_month_num_prev = prev_output['ticker_month_num_prev']
        dts = pd.date_range(start=pd.datetime(ticker_year_prev,ticker_month_num_prev,1), end=pd.datetime(ticker_year_prev, ticker_month_num_prev, 25), freq=bday_us)
        exp_indx = -4
    elif ticker_head == 'HO' or ticker_head == 'RB':
        prev_output = get_prev_ticker_year_month(ticker_year,ticker_month_num)
        ticker_year_prev = prev_output['ticker_year_prev']
        ticker_month_num_prev = prev_output['ticker_month_num_prev']
        dts = pd.date_range(pd.datetime(ticker_year_prev,ticker_month_num_prev, 1), periods=32, freq=bday_us)
        dts = dts[dts.month == ticker_month_num_prev]
        exp_indx = -1
    elif ticker_head == 'NG':
        prev_output = get_prev_ticker_year_month(ticker_year,ticker_month_num)
        ticker_year_prev = prev_output['ticker_year_prev']
        ticker_month_num_prev = prev_output['ticker_month_num_prev']
        dts = pd.date_range(start=pd.datetime(ticker_year_prev, ticker_month_num_prev, 1), end=pd.datetime(ticker_year, ticker_month_num, 1), freq=bday_us)
        if pd.datetime(ticker_year,ticker_month_num,1) in dts:
            exp_indx = -4
        else:
            exp_indx = -3
    elif ticker_head in ['ES', 'NQ']:
        dts = pd.date_range(pd.datetime(ticker_year, ticker_month_num, 1), periods=32)
        dts = dts[dts.dayofweek == 4]
        exp_indx = 2
    elif ticker_class == 'FX':
        dts = pd.date_range(pd.datetime(ticker_year, ticker_month_num, 1), periods=32)
        wednesdays = dts[dts.dayofweek == 2]
        dts = pd.date_range(start=pd.datetime(ticker_year, ticker_month_num, 1), end=wednesdays[2], freq=bday_us)
        exp_indx = -3
    elif ticker_head in ['TY', 'US']:
        dts = pd.date_range(pd.datetime(ticker_year, ticker_month_num, 1), periods=32, freq=bday_us)
        dts = dts[dts.month == ticker_month_num]
        exp_indx = -8
    elif ticker_head in ['FV', 'TU']:
        dts = pd.date_range(pd.datetime(ticker_year, ticker_month_num, 1), periods=32, freq=bday_us)
        dts = dts[dts.month == ticker_month_num]
        exp_indx = -1
    elif ticker_head == 'ED':
        dts = pd.date_range(pd.datetime(ticker_year,ticker_month_num, 1), periods=32)
        dts = dts[dts.month == ticker_month_num]
        wednesday_list = dts[dts.dayofweek == 2]
        dts = pd.date_range(start=pd.datetime(ticker_year,ticker_month_num, 1), end=wednesday_list[2], freq=bday_us)
        exp_indx = -3
    elif ticker_class == 'Metal':
        dts = pd.date_range(pd.datetime(ticker_year, ticker_month_num, 1), periods=32, freq=bday_us)
        dts = dts[dts.month == ticker_month_num]
        exp_indx = -3
    elif ticker_head == 'SB':
        prev_output = get_prev_ticker_year_month(ticker_year,ticker_month_num)
        ticker_year_prev = prev_output['ticker_year_prev']
        ticker_month_num_prev = prev_output['ticker_month_num_prev']
        dts = pd.date_range(pd.datetime(ticker_year_prev, ticker_month_num_prev, 1), periods=32, freq=bday_us)
        dts = dts[dts.month == ticker_month_num_prev]
        exp_indx = -1
    elif ticker_head == 'B':
        if 100*ticker_year+ticker_month_num <= 201602:
            prev_output = get_prev_ticker_year_month(ticker_year,ticker_month_num)
            ticker_year_prev = prev_output['ticker_year_prev']
            ticker_month_num_prev = prev_output['ticker_month_num_prev']
            dts = pd.date_range(start=pd.datetime(ticker_year_prev, ticker_month_num_prev, 1), end=pd.datetime(ticker_year, ticker_month_num, 1))
            dts = pd.date_range(start=pd.datetime(ticker_year_prev, ticker_month_num_prev, 1), end=dts[-16], freq=bday_us)
            exp_indx = -2
        else:
            prev_output = get_prev_ticker_year_month(ticker_year,ticker_month_num)
            prev_output2 = get_prev_ticker_year_month(prev_output['ticker_year_prev'], prev_output['ticker_month_num_prev'])
            ticker_year_prev2 = prev_output2['ticker_year_prev']
            ticker_month_num_prev2 = prev_output2['ticker_month_num_prev']
            dts = pd.date_range(pd.datetime(ticker_year_prev2, ticker_month_num_prev2, 1), periods=32, freq=bday_us)

            if ticker_month_num_prev2 == 12:
                dts_week = pd.bdate_range(pd.datetime(ticker_year_prev2, ticker_month_num_prev2, 1), periods=32)
                special_days = set(dts_week).difference(set(dts))
                bus_days_before_special_days = [dts_week[x] for x in range(len(dts_week)-1) if dts_week[x+1] in special_days]
                dts = pd.DatetimeIndex([dts[x] for x in range(len(dts)) if dts[x] not in bus_days_before_special_days])
            dts = dts[dts.month==ticker_month_num_prev2]
            exp_indx = -1
    elif ticker_head == 'KC':
        dts = pd.date_range(pd.datetime(ticker_year,ticker_month_num, 1), periods=32, freq=bday_us)
        dts = dts[dts.month==ticker_month_num]
        exp_indx = -9
    elif ticker_head == 'CC':
        dts = pd.date_range(pd.datetime(ticker_year,ticker_month_num, 1), periods=32, freq=bday_us)
        dts = dts[dts.month==ticker_month_num]
        exp_indx = -12
    elif ticker_head == 'CT':
        dts = pd.date_range(pd.datetime(ticker_year,ticker_month_num, 1), periods=32, freq=bday_us)
        dts = dts[dts.month==ticker_month_num]
        exp_indx = -17
    elif ticker_head == 'OJ':
        dts = pd.date_range(pd.datetime(ticker_year,ticker_month_num, 1), periods=32, freq=bday_us)
        dts = dts[dts.month == ticker_month_num]
        exp_indx = -15


    return dts[exp_indx].to_pydatetime()


def get_futures_days2_expiration(expiration_input):

    ticker = expiration_input['ticker']
    date_to = expiration_input['date_to']

    contract_specs_output = cmf.get_contract_specs(ticker)
    bday_us = CustomBusinessDay(calendar=get_calendar_4ticker_head(contract_specs_output['ticker_head']))
    expiration_date = get_futures_expiration(ticker)
    dts = pd.date_range(start=cu.convert_doubledate_2datetime(date_to), end=expiration_date, freq=bday_us)

    return len(dts)-1


def get_days2_expiration(**kwargs):

    date_to = kwargs['date_to']

    datetime_to = cu.convert_doubledate_2datetime(date_to)
    expiration_datetime = get_expiration_from_db(**kwargs)

    contract_specs_output = cmf.get_contract_specs(kwargs['ticker'])
    bday_us = CustomBusinessDay(calendar=get_calendar_4ticker_head(contract_specs_output['ticker_head']))
    dts = pd.date_range(start=datetime_to, end=expiration_datetime, freq=bday_us)

    return {'expiration_datetime': expiration_datetime,
            'cal_dte': (expiration_datetime-datetime_to.date()).days,
            'tr_dte': len(dts)-1}


def get_days2_roll(**kwargs):

    ticker = kwargs['ticker']
    date_to = kwargs['date_to']
    contract_specs = cmf.get_contract_specs(ticker)
    ticker_head = contract_specs['ticker_head']
    ticker_class = cmf.ticker_class[ticker_head]
    ticker_year = contract_specs['ticker_year']
    ticker_month_num = contract_specs['ticker_month_num']

    datetime_to = cu.convert_doubledate_2datetime(date_to)
    bday_us = CustomBusinessDay(calendar=get_calendar_4ticker_head(ticker_head))

    if ticker_class == 'Metal':
        if ticker_month_num == 1:
            ticker_year_roll = ticker_year-1
            ticker_month_roll = 12
        else:
            ticker_year_roll = ticker_year
            ticker_month_roll = ticker_month_num-1
        roll_datetime = dt.date(ticker_year_roll,ticker_month_roll,25)
    else:
        expiration_datetime = get_expiration_from_db(**kwargs)
        roll_datetime = expiration_datetime-dt.timedelta(days=5)

    if roll_datetime > datetime_to.date():
        dts = pd.date_range(start=datetime_to, end=roll_datetime, freq=bday_us)
        tr_days_2roll = len(dts)-1
    else:
        dts = pd.date_range(start=roll_datetime, end=datetime_to, freq=bday_us)
        tr_days_2roll = -(len(dts)-1)

    return {'roll_datetime': roll_datetime,
            'cal_days_2roll': (roll_datetime-datetime_to.date()).days,
            'tr_days_2roll': tr_days_2roll}


def get_prev_ticker_year_month(ticker_year,ticker_month_num):
    if ticker_month_num == 1:
        ticker_month_num_prev = 12
        ticker_year_prev = ticker_year-1
    else:
        ticker_month_num_prev = ticker_month_num-1
        ticker_year_prev = ticker_year
    return {'ticker_year_prev': ticker_year_prev, 'ticker_month_num_prev': ticker_month_num_prev}


def get_calendar_4ticker_head(ticker_head):

    if ticker_head == 'ED' or ticker_head == 'B':

        class trading_calendar(AbstractHolidayCalendar):
            rules = [
            Holiday('New Years Day', month=1,  day=1,  observance=weekend_to_monday),
            GoodFriday,
            Holiday('MemorialDay', month=5, day=25, offset=pd.DateOffset(weekday=MO(1))),
            Holiday('July 4th', month=7,  day=4,  observance=nearest_workday),
            Holiday('Summer Bank Holiday', month=8, day=25, offset=pd.DateOffset(weekday=MO(1))),
            USThanksgivingDay,
            Holiday('Christmas', month=12, day=25, observance=nearest_workday)]

    else:

        class trading_calendar(AbstractHolidayCalendar):
            rules = [
            Holiday('New Years Day', month=1,  day=1,  observance=nearest_workday),
            USMartinLutherKingJr,
            USPresidentsDay,
            GoodFriday,
            Holiday('MemorialDay', month=5, day=25, offset=pd.DateOffset(weekday=MO(1))),
            Holiday('July 4th', month=7,  day=4,  observance=nearest_workday),
            USLaborDay,
            USThanksgivingDay,
            Holiday('Christmas', month=12, day=25, observance=nearest_workday)]

    return trading_calendar()


def doubledate_shift_bus_days(**kwargs):

    if 'double_date' in kwargs.keys():
        double_date = kwargs['double_date']
    else:
        double_date = int(tm.strftime('%Y%m%d'))

    if 'shift_in_days' in kwargs.keys():
        shift_in_days = kwargs['shift_in_days']
    else:
        shift_in_days = 1

    if 'reference_tickerhead' in kwargs.keys():
        reference_tickerhead = kwargs['reference_tickerhead']
    else:
        reference_tickerhead = const.reference_tickerhead_4business_calendar

    bday_us = CustomBusinessDay(calendar=get_calendar_4ticker_head(reference_tickerhead))
    double_date_datetime = cu.convert_doubledate_2datetime(double_date)

    if shift_in_days < 0:
        dts_aux = pd.date_range(double_date_datetime, periods=-shift_in_days+1, freq=bday_us)
        dts = [x for x in dts_aux if x.to_pydatetime() != double_date_datetime]
        shifted_datetime = dts[-shift_in_days-1]
    elif shift_in_days > 0:
        dts_aux = pd.date_range(start=double_date_datetime-dt.timedelta(max(m.ceil(shift_in_days*7/4), shift_in_days+8)),
                            end=double_date_datetime, freq=bday_us)
        dts = [x for x in dts_aux if x.to_pydatetime() != double_date_datetime]
        shifted_datetime = dts[-shift_in_days]

    return int(shifted_datetime.strftime('%Y%m%d'))


def is_business_day(**kwargs):

    double_date = kwargs['double_date']

    if 'reference_tickerhead' in kwargs.keys():
        reference_tickerhead = kwargs['reference_tickerhead']
    else:
        reference_tickerhead = const.reference_tickerhead_4business_calendar

    bday_us = CustomBusinessDay(calendar=get_calendar_4ticker_head(reference_tickerhead))
    double_date_datetime = cu.convert_doubledate_2datetime(double_date)

    dts_aux = pd.date_range(double_date_datetime, periods=1, freq=bday_us)

    return double_date_datetime in dts_aux


def get_bus_day_list(**kwargs):

    if 'reference_tickerhead' in kwargs.keys():
        reference_tickerhead = kwargs['reference_tickerhead']
    else:
        reference_tickerhead =const.reference_tickerhead_4business_calendar

    if 'date_from' in kwargs.keys():
        datetime_from = cu.convert_doubledate_2datetime(kwargs['date_from'])
    if 'datetime_from' in kwargs.keys():
        datetime_from = kwargs['datetime_from']

    if 'date_to' in kwargs.keys():
        datetime_to = cu.convert_doubledate_2datetime(kwargs['date_to'])
    if 'datetime_to' in kwargs.keys():
        datetime_to = kwargs['datetime_to']

    bday_us = CustomBusinessDay(calendar=get_calendar_4ticker_head(reference_tickerhead))

    date_index = pd.date_range(start=datetime_from, end=datetime_to, freq=bday_us)

    return [int(x.to_pydatetime().strftime('%Y%m%d')) for x in date_index]







