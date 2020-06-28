__author__ = 'kocat_000'

import numpy as np
import pandas as pd
import shared.statistics as stats
pd.options.mode.chained_assignment = None  # default='warn'

import warnings
warnings.filterwarnings("ignore", message="invalid value encountered in sign")


def get_adx(**kwargs):

    data_frame_input = kwargs['data_frame_input']
    period = kwargs['period']
    smoothing_lookback = min(150, round(len(data_frame_input.index) / 2))
    smoothing_lookback = max(smoothing_lookback, len(data_frame_input.index) - 2 * period)

    data_frame_input['high_1'] = data_frame_input['high'].shift(1)
    data_frame_input['low_1'] = data_frame_input['low'].shift(1)

    positive_index = data_frame_input['high'] - data_frame_input['high_1'] > data_frame_input['low_1'] - data_frame_input['low']
    data_frame_input['DMP'] = 0
    data_frame_input['DMP'].loc[positive_index] = data_frame_input['high'].loc[positive_index] - data_frame_input['high_1'].loc[positive_index]
    data_frame_input['DMP'][data_frame_input['DMP'] < 0] = 0

    negative_index = data_frame_input['high'] - data_frame_input['high_1'] < data_frame_input['low_1'] - data_frame_input['low']
    data_frame_input['DMN'] = 0
    data_frame_input['DMN'].loc[negative_index] = data_frame_input['low_1'].loc[negative_index] - data_frame_input['low'].loc[negative_index]
    data_frame_input['DMN'][data_frame_input['DMN'] < 0] = 0

    data_frame_input['DMPS'] = np.nan
    data_frame_input['DMNS'] = np.nan

    for i in range(smoothing_lookback):

        if i == 0:
            data_frame_input['DMPS'].iloc[-smoothing_lookback + i] = np.mean(data_frame_input['DMP'].iloc[(-smoothing_lookback-period+1):(-smoothing_lookback + 1 + i)])
            data_frame_input['DMNS'].iloc[-smoothing_lookback + i] = np.mean(data_frame_input['DMN'].iloc[(-smoothing_lookback-period+1):(-smoothing_lookback + 1 + i)])
        else:
            data_frame_input['DMPS'].iloc[-smoothing_lookback + i] = ((period-1) * data_frame_input['DMPS'].iloc[-smoothing_lookback - 1 + i] +
                                                      data_frame_input['DMP'].iloc[-smoothing_lookback + i]) / period

            data_frame_input['DMNS'].iloc[-smoothing_lookback + i] = ((period - 1) * data_frame_input['DMNS'].iloc[-smoothing_lookback - 1 + i] +
                                                       data_frame_input['DMN'].iloc[-smoothing_lookback + i]) / period

    data_frame_input = get_atr(data_frame_input=data_frame_input,period=period)

    data_frame_input['DIP'] = 100*data_frame_input['DMPS']/data_frame_input['atr_' + str(period)]
    data_frame_input['DIN'] = 100*data_frame_input['DMNS']/data_frame_input['atr_' + str(period)]

    data_frame_input['DX'] = 100*abs(data_frame_input['DIP']-data_frame_input['DIN'])/(data_frame_input['DIP']+data_frame_input['DIN'])

    smoothing_lookback = smoothing_lookback -30
    data_frame_input['adx_' + str(period)] = np.nan

    for i in range(smoothing_lookback):

        if i == 0:
            data_frame_input['adx_' + str(period)].iloc[-smoothing_lookback + i] = np.mean(data_frame_input['DX'].iloc[(-smoothing_lookback-period+1):-smoothing_lookback+1 + i])
        else:
            data_frame_input['adx_' + str(period)].iloc[-smoothing_lookback + i] = ((period-1) * data_frame_input['adx_' + str(period)].iloc[-smoothing_lookback-1 + i] +
                                                      data_frame_input['DX'].iloc[-smoothing_lookback + i]) / period

    return data_frame_input.drop(['high_1', 'low_1', 'DMP', 'DMN', 'DMPS', 'DMNS', 'DIP', 'DIN', "DX"], 1, inplace=False)

def get_atr(**kwargs):

    data_frame_input = kwargs['data_frame_input']
    period = kwargs['period']

    if 'percent_q' in kwargs.keys():
        percent_q = kwargs['percent_q']
    else:
        percent_q = False

    data_frame_input['close_1'] = data_frame_input['close'].shift(1)

    data_frame_input['TR1'] = data_frame_input['high'] - data_frame_input['low']
    data_frame_input['TR2'] = abs(data_frame_input['high'] - data_frame_input['close_1'])
    data_frame_input['TR3'] = abs(data_frame_input['low'] - data_frame_input['close_1'])

    data_frame_input['TR'] = data_frame_input[['TR1', 'TR2', 'TR3']].max(axis=1)

    if percent_q:
        data_frame_input['denominator'] = np.nan
        first_selection = data_frame_input['TR']==data_frame_input['TR2']
        data_frame_input['denominator'].loc[first_selection] = data_frame_input['close_1'].loc[first_selection]+(data_frame_input['TR'].loc[first_selection]/2)
        second_selection = (data_frame_input['TR']==data_frame_input['TR1'])|(data_frame_input['TR']==data_frame_input['TR3'])
        data_frame_input['denominator'].loc[second_selection] = data_frame_input['low'].loc[second_selection] +(data_frame_input['TR'].loc[second_selection]/2)
        data_frame_input['TR'] = 100*data_frame_input['TR']/data_frame_input['denominator']

    data_frame_input['atr_' + str(period)] = np.nan

    smoothing_lookback = min(150, round(len(data_frame_input.index) / 2))
    smoothing_lookback = max(smoothing_lookback, len(data_frame_input.index) - 2 * period)

    for i in range(smoothing_lookback):

        if i == 0:
            data_frame_input['atr_' + str(period)].iloc[-smoothing_lookback + i] = np.mean(data_frame_input['TR'].iloc[(-smoothing_lookback-period+1):(-smoothing_lookback + 1 + i)])
        elif ~np.isnan(data_frame_input['TR'].iloc[-smoothing_lookback + i]):
            data_frame_input['atr_' + str(period)].iloc[-smoothing_lookback + i] = ((period-1) * data_frame_input['atr_' + str(period)].iloc[-smoothing_lookback-1 + i] +
                                                      data_frame_input['TR'].iloc[-smoothing_lookback + i]) / period
        else:
            data_frame_input['atr_' + str(period)].iloc[-smoothing_lookback + i] = data_frame_input['atr_' + str(period)].iloc[-smoothing_lookback-1 + i]

    return data_frame_input.drop(['close_1', 'TR1', 'TR2', 'TR3', 'TR'], 1, inplace=False)

def get_cci(**kwargs):

    data_frame_input = kwargs['data_frame_input']
    period = kwargs['period']

    data_frame_input['TP'] = (data_frame_input['high'] + data_frame_input['low'] + data_frame_input['close']) / 3
    data_frame_input['TPA'] = data_frame_input['TP'].rolling(window=period, center=False).mean()
    data_frame_input['TPD'] = data_frame_input['TP'].rolling(window=period, center=False).std()
    data_frame_input['cci_'  + str(period)] = (data_frame_input['TP']-data_frame_input['TPA']) / (0.015 * data_frame_input['TPD'])

    return data_frame_input.drop(['TP', 'TPA', 'TPD'], 1, inplace=False)

def get_macd(**kwargs):

    data_frame_input = kwargs['data_frame_input']
    period1 = kwargs['period1']
    period2 = kwargs['period2']
    period3 = kwargs['period3']

    data_frame_input['aux1'] = data_frame_input['close'].ewm(span=period1, min_periods=period1, adjust=True, ignore_na=False).mean()
    data_frame_input['aux2'] = data_frame_input['close'].ewm(span=period2, min_periods=period2, adjust=True, ignore_na=False).mean()
    data_frame_input['macd_' + str(period1) + '_' + str(period2)] = data_frame_input['aux1'] - data_frame_input['aux2']

    data_frame_input['macd_signal_' + str(period1) + '_' + str(period2) + '_' + str(period3)] = data_frame_input['macd_' + str(period1) + '_' + str(period2)].ewm(span=period3, min_periods=period3, adjust=True, ignore_na=False).mean()

    data_frame_input['macd_hist_' + str(period1) + '_' + str(period2) + '_' + str(period3)] = data_frame_input['macd_' + str(period1) + '_' + str(period2)]-\
                                                                                              data_frame_input['macd_signal_' + str(period1) + '_' + str(period2) + '_' + str(period3)]
    return data_frame_input.drop(['aux1', 'aux2'], 1, inplace=False)


def get_money_flow_index(**kwargs):

    data_frame_input = kwargs['data_frame_input']
    period = kwargs['period']

    data_frame_input['TP'] = (data_frame_input['high'] + data_frame_input['low'] + data_frame_input['close']) / 3
    data_frame_input['TPD'] = data_frame_input['TP'].diff()

    data_frame_input['gains'] = data_frame_input['TP']*data_frame_input['volume']
    data_frame_input['losses'] = data_frame_input['TP']*data_frame_input['volume']

    data_frame_input['gains'][data_frame_input['TPD'] < 0] = 0
    data_frame_input['losses'][data_frame_input['TPD'] > 0] = 0

    data_frame_input['mfi_' + str(period)] = 100-100/(1+(data_frame_input['gains'].rolling(window=period, center=False).sum()/data_frame_input['losses'].rolling(window=period, center=False).sum()))
    return data_frame_input.drop(['TP', 'TPD', 'gains', 'losses'], 1, inplace=False)


def get_obv(**kwargs):

    data_frame_input = kwargs['data_frame_input']
    period = kwargs['period']

    data_frame_input['signed_volume'] = np.sign(data_frame_input['change_1']) * data_frame_input['volume']
    data_frame_input['obv'] = data_frame_input['signed_volume'].rolling(window=period, center=False).sum()
    return data_frame_input.drop(['signed_volume'], 1, inplace=False)

def get_rocr(**kwargs):

    data_frame_input = kwargs['data_frame_input']
    period = kwargs['period']

    data_frame_input['rocr_' + str(period)] = 100*data_frame_input['close']/data_frame_input['close'].shift(period)
    return data_frame_input


def rsi(**kwargs):

    data_frame_input = kwargs['data_frame_input']
    change_field = kwargs['change_field']
    period = kwargs['period']

    if len(data_frame_input.index)<period:
        data_frame_input['rsi_' + str(period)] = np.nan
        return data_frame_input

    data_frame_input['gains'] = data_frame_input[change_field]
    data_frame_input['gains'][data_frame_input['gains']<0] = 0

    data_frame_input['losses'] = data_frame_input[change_field]
    data_frame_input['losses'][data_frame_input['losses']>0] = 0

    data_frame_input['losses'] *= -1

    data_frame_input['smg'] = np.nan
    data_frame_input['sml'] = np.nan

    smoothing_lookback = min(150, round(len(data_frame_input.index)/2))
    smoothing_lookback = max(smoothing_lookback, len(data_frame_input.index) - 2 * period)

    for i in range(smoothing_lookback):
        if i == 0:
            data_frame_input['smg'].iloc[-smoothing_lookback + i] = np.mean(data_frame_input['gains'].iloc[(-smoothing_lookback - period + 1):-smoothing_lookback + 1 + i])
            data_frame_input['sml'].iloc[-smoothing_lookback + i] = np.mean(data_frame_input['losses'].iloc[(-smoothing_lookback - period + 1):-smoothing_lookback + 1 + i])

        else:
            data_frame_input['smg'].iloc[-smoothing_lookback + i] = ((period - 1) * data_frame_input['smg'].iloc[-smoothing_lookback - 1 + i] +data_frame_input['gains'].iloc[-smoothing_lookback + i]) / period
            data_frame_input['sml'].iloc[-smoothing_lookback + i] = ((period - 1) * data_frame_input['sml'].iloc[-smoothing_lookback - 1 + i] + data_frame_input['losses'].iloc[-smoothing_lookback + i]) / period

    data_frame_input['rs'] = data_frame_input['smg']/data_frame_input['sml']
    data_frame_input['rsi_' + str(period)] = 100-(100/(1+data_frame_input['rs']))

    data_frame_input.drop(['gains','losses','smg','sml','rs'], 1, inplace=True)

    return data_frame_input


def stochastic(**kwargs):

    data_frame_input = kwargs['data_frame_input']
    p1 = kwargs['p1']
    p2 = kwargs['p2']
    p3 = kwargs['p3']

    data_frame_input['hh'] = pd.rolling_max(data_frame_input['high'], window=p1, min_periods=p1)
    data_frame_input['ll'] = pd.rolling_min(data_frame_input['low'], window=p1, min_periods=p1)
    data_frame_input['K'] = 100*(data_frame_input['close']-data_frame_input['ll'])/(data_frame_input['hh']- data_frame_input['ll'])
    data_frame_input['D1'] = pd.rolling_mean(data_frame_input['K'], window=p2)
    data_frame_input['D2'] = pd.rolling_mean(data_frame_input['D1'], window=p3)

    return data_frame_input.drop(['hh','ll'], 1, inplace=False)


def time_series_regression(**kwargs):

    data_frame_input = kwargs['data_frame_input']
    num_obs = kwargs['num_obs']
    data_frame_input = data_frame_input.iloc[-num_obs:]

    return stats.get_regression_results({'x': range(num_obs),'y':data_frame_input[kwargs['y_var_name']],'clean_num_obs': num_obs})


def get_time_series_based_return_forecast(**kwargs):

    data_frame_input = kwargs['data_frame_input']
    regression_window = kwargs['regression_window']
    forecast_window1 = kwargs['forecast_window1']
    forecast_window2 = kwargs['forecast_window2']

    regress_output_list = [stats.get_regression_results({'x': range(regression_window),
                                                   'y': data_frame_input['close'].iloc[(i-regression_window+1):(i+1)].values,
                                                   'clean_num_obs': regression_window})
                           for i in range(regression_window-1, len(data_frame_input.index))]

    data_frame_input['alpha'] = np.nan
    data_frame_input['beta'] = np.nan

    data_frame_input['alpha'].iloc[(regression_window-1):] = [x['alpha'] for x in regress_output_list]
    data_frame_input['beta'].iloc[(regression_window-1):] = [x['beta'] for x in regress_output_list]

    data_frame_input['tsf_' + str(regression_window) + '_' + str(forecast_window1)] = \
        (100*(data_frame_input['alpha'] + data_frame_input['beta'] * (regression_window + forecast_window1))/(data_frame_input['close']))-100

    data_frame_input['tsf_' + str(regression_window) + '_' + str(forecast_window2)] = \
        (100*(data_frame_input['alpha'] + data_frame_input['beta'] * (regression_window + forecast_window2))/(data_frame_input['close']))-100

    return data_frame_input.drop(['alpha', 'beta'], 1, inplace=False)


def get_trix(**kwargs):

    data_frame_input = kwargs['data_frame_input']
    period = kwargs['period']

    data_frame_input['aux1'] = data_frame_input['close'].ewm(span=period, min_periods=period, adjust=True, ignore_na=False).mean()
    data_frame_input['aux2'] = data_frame_input['aux1'].ewm(span=period, min_periods=period, adjust=True, ignore_na=False).mean()
    data_frame_input['aux3'] = data_frame_input['aux2'].ewm(span=period, min_periods=period, adjust=True, ignore_na=False).mean()

    data_frame_input['trix_' + str(period)] = data_frame_input['aux3']/data_frame_input['aux3'].shift(1)

    return data_frame_input.drop(['aux1', 'aux2', 'aux3'], 1, inplace=False)


def get_williams_r(**kwargs):

    data_frame_input = kwargs['data_frame_input']
    period = kwargs['period']

    data_frame_input['min'] = data_frame_input['low'].rolling(window=period, center=False).min()
    data_frame_input['max'] = data_frame_input['high'].rolling(window=period, center=False).max()

    data_frame_input['williams_r_' + str(period)] = -100 * (data_frame_input['max'] - data_frame_input['close']) / (
        data_frame_input['max'] - data_frame_input['min'])

    return data_frame_input.drop(['min', 'max'], 1, inplace=False)


def get_bollinger_deviation(**kwargs):

    data_frame_input = kwargs['data_frame_input']
    period = kwargs['period']

    data_frame_input['bollinger_mean_' + str(period)] = data_frame_input['close'].rolling(window=period, center=False).mean()
    data_frame_input['bollinger_std_' + str(period)] = data_frame_input['close'].rolling(window=period,center=False).std()
    data_frame_input['bollinger_dev_' + str(period)] = (data_frame_input['close'] - data_frame_input['bollinger_mean_' + str(period)]) / \
                                                       data_frame_input['bollinger_std_' + str(period)]

    data_frame_input['bollinger_upper_' + str(period)] = data_frame_input['bollinger_mean_' + str(period)] + 2*data_frame_input['bollinger_std_' + str(period)]
    data_frame_input['bollinger_lower_' + str(period)] = data_frame_input['bollinger_mean_' + str(period)] - 2*data_frame_input['bollinger_std_' + str(period)]

    return data_frame_input


def get_ewma(**kwargs):

    data_frame_input = kwargs['data_frame_input']
    period = kwargs['period']
    field_name = kwargs['field_name']

    if len(data_frame_input)<period:
        data_frame_input['ewma_' + str(period)] = np.nan
        return data_frame_input

    data_frame_input['aux'] = data_frame_input[field_name].rolling(window=period, center=False).mean()
    data_frame_input.aux.iloc[period:] = data_frame_input[field_name].iloc[period:]
    data_frame_input['ewma_' + str(period)] = data_frame_input.aux.ewm(span=period, adjust=False).mean()
    data_frame_input.drop('aux', axis=1, inplace=True)

    return data_frame_input



























