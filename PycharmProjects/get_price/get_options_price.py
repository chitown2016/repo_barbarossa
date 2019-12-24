__author__ = 'kocat_000'

import warnings
with warnings.catch_warnings():
    warnings.filterwarnings("ignore",category=FutureWarning)
    import h5py

import shared.directory_names_aux as dna
import shared.directory_names as dn
import my_sql_routines.my_sql_utilities as msu
import scipy.io
import pandas as pd
import numpy as np
import os
import h5py

def get_column_names_4option_data():

    fwd_looking_window_list = [5, 10, 20, 40, 60]

    optional_cc_skew_columns = ['vegaPnL', 'pastAtmVol1',
            'atmThetaBs','atmVegaBs','atmVegaOu','atmGammaBs',
            'Kvol','LogPrice','SquaredPrice', 'KnsImpBsSkew', 'KnsImpOuSkew',
            'MFIVolPath', 'BkmImpSkewness', 'BkmImpKurtosis',
            'ImpSkewStickinessRatio','SkewStickinessRatio',
            'dImpVolDk', 'BSHistKnsRzVar', 'OUHistKnsRzVar',
            'KnsRzBsSkew', 'DRzOuSkewness', 'DRzSkewness','DRzKurtosis', 'BiPowerVol',
            'BSHistBkmImpBipowerRzVolPremAnn','BSHistSkewPremiumAnn','BSHistScaledKurtPremiumAnn',
            'HistKnsImpVar','HistKnsImpSkew','HistKnsImpKurt' , 'contINDX', 'optBpvs',
            'yzVol20', 'timeValue', 'pastAtmVol20', 'pastAtmVolChange20', 'pastAtmVolChange1', 'expectedDailyMove',
             'optionPnL', 'deltaPnL', 'gammaPnL', 'thetaPnL', 'dSpot', 'Theta30days']

    default_aligned_data_column_names = ['settleDates', 'TickerYear', 'TickerMonth', 'trDTE', 'calDTE',
        'underlying', 'atmVol', 'strike', 'theoValue', 'theoPremium',
        'impVol', 'delta', 'gamma', 'vega', 'theta', 'rate2OptExp',
        'dollarVega', 'dollarTheta', 'normDollarTheta', 'dollarGamma', 'dailyPnL', 'dailyUnhedgedPnL',
        'close2CloseVol10', 'close2CloseVol20', 'close2CloseVol60',
        'dailySdMove', 'success']

    window_fields = sum([['profit'+str(x), 'unhedgedProfit'+str(x), 'onceHedgedProfit'+str(x),
                         'underlyingProfit'+str(x), 'vegaProfit'+str(x), 'gammaProfit'+str(x),
                         'thetaProfit'+str(x), 'impVolFuture'+str(x), 'atmVolFuture'+str(x),
                         'futureAtmVolChange'+str(x), 'spotFuture'+str(x)] for x in fwd_looking_window_list],[])

    return default_aligned_data_column_names + window_fields + optional_cc_skew_columns


def load_aligend_options_data_file(**kwargs):

    ticker_head = kwargs['ticker_head']
    tr_dte_center = kwargs['tr_dte_center']

    option_data_dir = dna.get_directory_name(ext='aligned_time_series_output')

    if 'delta_center' in kwargs.keys():
        delta_center = kwargs['delta_center']
    else:
        delta_center = 0.5

    if 'model' in kwargs.keys():
        model = kwargs['model']
    else:
        model = 'BS'

    if 'contract_month_letter' in kwargs.keys():
        contract_month_str = '_' + kwargs['contract_month_letter']
    else:
        contract_month_str = ''

    if 'column_names' in kwargs.keys():
        column_names = kwargs['column_names']
    else:
        column_names = get_column_names_4option_data()

    file_dir = ticker_head + '_' + str(delta_center) + '_' + model + '_20_510204060_' + str(tr_dte_center) + contract_month_str + '.mat'

    if os.path.isfile(option_data_dir+'/'+file_dir):
        try:
            mat_output = scipy.io.loadmat(option_data_dir+'/'+file_dir)
            data_frame_out = pd.DataFrame(mat_output['alignedDataMatrix'], columns=column_names)
        except Exception:
            mat_output = h5py.File(option_data_dir+'/'+file_dir)
            data_frame_out = pd.DataFrame(mat_output['alignedDataMatrix'].value.transpose(), columns=column_names)
    else:
        data_frame_out = pd.DataFrame(columns=column_names)

    return data_frame_out


def load_options_backtesting_data(**kwargs):

    ticker = kwargs['ticker']
    settle_date = kwargs['settle_date']
    delta = kwargs['delta']

    if 'model' in kwargs.keys():
        model = kwargs['model']
    else:
        model = 'BS'

    option_data_dir = dn.get_dated_directory_extension(folder_date=settle_date, ext='options_backtesting_data')
    file_dir = ticker + '_' + model + '_' + str(delta) + '.mat'

    output_dictionary = {'final_pnl5': np.NaN, 'final_pnl10': np.NaN, 'final_pnl20': np.NaN,
                             'accumulated_theta_5': np.NaN, 'accumulated_theta_10': np.NaN, 'accumulated_theta_20': np.NaN}

    if os.path.isfile(option_data_dir+'/'+file_dir):

        try:
            mat_output = scipy.io.loadmat(option_data_dir+'/'+file_dir)
            paper_trader_output = mat_output['paperTraderOutput']

            output_dictionary = {'final_pnl5': paper_trader_output['finalPnl5'][0][0][0][0],
                             'final_pnl10': paper_trader_output['finalPnl10'][0][0][0][0],
                             'final_pnl20': paper_trader_output['finalPnl20'][0][0][0][0],
                             'accumulated_theta_5': paper_trader_output['accumulatedTheta5'][0][0][0][0],
                             'accumulated_theta_10': paper_trader_output['accumulatedTheta10'][0][0][0][0],
                             'accumulated_theta_20': paper_trader_output['accumulatedTheta20'][0][0][0][0]}
        except Exception:
            print('Cannot load ' + file_dir + ' !')

    return output_dictionary


def get_options_price_from_db(**kwargs):

    con = msu.get_my_sql_connection(**kwargs)

    if 'ticker' in kwargs.keys():
        filter_string = 'WHERE ticker=\'' + kwargs['ticker'] + '\''
    elif 'ticker_head' in kwargs.keys():
        filter_string = 'WHERE ticker_head=\'' + kwargs['ticker_head'] + '\''
    else:
        filter_string = ''

    if 'settle_date' in kwargs.keys():
        filter_string = filter_string + ' and price_date=' + str(kwargs['settle_date'])

    if 'delta_target' in kwargs.keys():

        filter_string = filter_string + ' and abs(' + str(kwargs['delta_target']) + '-delta) = ' +\
        '(SELECT min(abs('+ str(kwargs['delta_target']) + '-delta)) FROM daily_option_price ' + filter_string + ')'

    if 'strike' in kwargs.keys():
        filter_string = filter_string + ' and strike=' + str(kwargs['strike'])

    if 'option_type' in kwargs.keys():
        filter_string = filter_string + ' and option_type=\'' + kwargs['option_type'] + '\''

    if 'column_names' in kwargs.keys():
        column_names = kwargs['column_names']
    else:
        column_names = ['id', 'option_type', 'strike', 'cal_dte', 'tr_dte', 'close_price', 'volume', 'open_interest']

    sql_query = 'SELECT ' + ",".join(column_names) + ' FROM daily_option_price ' + filter_string

    cur = con.cursor()
    cur.execute(sql_query)
    data = cur.fetchall()

    if 'con' not in kwargs.keys():
        con.close()

    if (not data) and ('strike' in kwargs.keys()) and ('option_type' in kwargs.keys()):
        data_frame_out = pd.DataFrame([(np.nan, kwargs['option_type'], kwargs['strike'], np.nan, np.nan, np.nan, np.nan, np.nan)], columns=column_names)
    else:
        data_frame_out = pd.DataFrame(data, columns=column_names)

    for x in ['close_price', 'delta', 'imp_vol', 'strike', 'theta', 'vega', 'option_pnl5', 'delta_pnl5']:

        if x in column_names:
            data_frame_out[x] = data_frame_out[x].astype('float64')

    return data_frame_out



