

import shared.directory_names_aux as dna
import shared.calendar_utilities as cu
import shared.directory_names as dn
import pandas as pd


def get_ticker_frame(**kwargs):

    date = kwargs['date']

    folder_name = dn.get_dated_directory_extension(folder_date=date, ext='binance')

    try:
        frame_out = pd.read_pickle(folder_name + '/ticker_frame.pkl')
    except:
        frame_out = pd.DataFrame(columns=['symbol'])

    return frame_out


def get_daily_price_data4ticker(**kwargs):

    date_to = kwargs['date_to']
    datetime_to = cu.convert_doubledate_2datetime(date_to)

    try:
        data_out = pd.read_pickle(dna.get_directory_name(ext='binance') + '/daily/' + kwargs['ticker'] + '.pkl')
    except:
        return pd.DataFrame()

    #data_out['pydate'] = [x.to_pydatetime().date() for x in data_out['openDatetime']]
    return data_out[data_out['openDate'] <= datetime_to.date()]


def get_weekly_price_data4ticker(**kwargs):
    daily_data = get_daily_price_data4ticker(**kwargs)
    if len(daily_data.index) == 0:
        return daily_data

    daily_data.index = daily_data['openDatetime']

    weekly_data = pd.DataFrame()
    weekly_data['high'] = daily_data.high.resample('W-Sun').max()
    weekly_data['low'] = daily_data.low.resample('W-Sun').min()
    weekly_data['open'] = daily_data.open.resample('W-Sun').first()
    weekly_data['close'] = daily_data.close.resample('W-Sun').last()
    weekly_data['volume'] = daily_data.volume.resample('W-Sun').sum()

    return weekly_data




