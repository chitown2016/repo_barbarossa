
import datetime as dt
import pandas as pd
import shared.calendar_utilities as cu
import binance_trading.get_data as bgd


def get_symbol_list(**kwargs):

    quote_type = kwargs['quote_type']
    datetime_from = cu.convert_doubledate_2datetime(20190317)

    datetime_to = dt.datetime.combine(dt.date.today(), dt.datetime.min.time())

    delta = datetime_to - datetime_from  # as timedelta

    ticker_list = []

    for i in range(delta.days):
        day = datetime_from + dt.timedelta(days=i)
        ticker_frame_out = bgd.get_ticker_frame(date=int(day.strftime('%Y%m%d')))
        ticker_list.extend(list(ticker_frame_out['symbol']))

    ticker_list = list(set(ticker_list))

    if quote_type == 'BTC':
        select_indx = -3
    elif quote_type == 'USDT':
        select_indx = -4

    res = [i for i in ticker_list if i[select_indx:] == quote_type]

    return res
