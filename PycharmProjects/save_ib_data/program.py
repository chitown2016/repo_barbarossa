
import ib_api_utils.subscription as subs
import my_sql_routines.my_sql_utilities as msu
import copy as cpy
import ta.trade_fill_loader as tfl
import ta.position_manager as pm
import contract_utilities.contract_meta_info as cmi
import contract_utilities.expiration as exp
import get_price.get_futures_price as gfp
import api_utils.portfolio as aup
import shared.utils as su
import shared.calendar_utilities as cu
import shared.directory_names_aux as dna
import numpy as np
import pandas as pd
import math as mth
import threading as thr
import datetime as dt
from ibapi.contract import *
from ibapi.common import *
from ibapi.ticktype import *
from ibapi.order_condition import *
import save_ib_data.save_data_algo as algo

import shared.log as lg

def save_ib_data(**kwargs):

    if 'duration_str' in kwargs.keys():
        duration_str = kwargs['duration_str']
    else:
        duration_str = '2 M'

    app = algo.Algo()
    con = msu.get_my_sql_connection()
    date_now = cu.get_doubledate()
    datetime_now = dt.datetime.now()
    report_date = exp.doubledate_shift_bus_days()


    ticker_head_list = cmi.cme_futures_tickerhead_list

    data_list = [gfp.get_futures_price_preloaded(ticker_head=x, settle_date=report_date) for x in ticker_head_list]
    ticker_frame = pd.concat(data_list)
    ticker_frame = ticker_frame[~((ticker_frame['ticker_head']=='ED')&(ticker_frame['tr_dte']<250))]
    ticker_frame = ticker_frame[~((ticker_frame['ticker_head']=='GC')|(ticker_frame['ticker_head']=='SI'))]

    ticker_frame.sort_values(['ticker_head', 'volume'], ascending=[True, False], inplace=True)
    ticker_frame.drop_duplicates(subset=['ticker_head'], keep='first', inplace=True)

    app.ticker_list = list(ticker_frame['ticker'])
    app.output_dir = dna.get_directory_name(ext='ib_data')
    app.durationStr = duration_str
    app.con = con

    app.connect(client_id=5)

    app.run()

    #try:
    #    app.run()
    #except:
    #    pass



