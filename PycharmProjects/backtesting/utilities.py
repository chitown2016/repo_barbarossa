__author__ = 'kocat_000'

import sys
sys.path.append(r'C:\Users\kocat_000\quantFinance\PycharmProjects')

import pandas as pd
import shared.calendar_utilities as cu
import contract_utilities.expiration as exp
from pandas.tseries.offsets import CustomBusinessDay
import math as m
import numpy as np

def get_backtesting_dates(**kwargs):
    date_to = kwargs['date_to']
    years_back = kwargs['years_back']

    if 'day_of_week' in kwargs.keys():
        day_of_week = kwargs['day_of_week']
    else:
        day_of_week = 2

    date_from = cu.doubledate_shift(date_to, years_back*365)

    trading_calendar = exp.get_calendar_4ticker_head('CL')
    bday_us = CustomBusinessDay(calendar=trading_calendar)

    dts_all = pd.date_range(start=cu.convert_doubledate_2datetime(date_from),
                    end=cu.convert_doubledate_2datetime(date_to), freq=bday_us)

    dts = dts_all[dts_all.dayofweek==day_of_week]
    double_dts_all = [int(x.strftime('%Y%m%d')) for x in dts_all]

    double_dts = [double_dts_all[x] for x in range(len(double_dts_all)) if dts_all[x].dayofweek==day_of_week]

    return {'date_time_dates': dts,
            'double_dates': double_dts,
            'double_dates_all': double_dts_all}

def find_exit_point(**kwargs):

    time_series = kwargs['time_series']
    trigger_value = kwargs['trigger_value']
    trigger_direction = kwargs['trigger_direction']

    if 'max_exit_point' in kwargs.keys():
        max_exit_point = kwargs['max_exit_point']
    else:
        max_exit_point = len(time_series)-1

    if trigger_direction == 'going_down':
        exit_vector = time_series<trigger_value
    elif trigger_direction == 'going_up':
        exit_vector = time_series>trigger_value

    obs_indx = [x for x in range(1,len(time_series)) if exit_vector[x-1] and exit_vector[x]]

    if not obs_indx:
        exit_point = max_exit_point
    else:
        exit_point = min(obs_indx[0],max_exit_point)

    return exit_point









