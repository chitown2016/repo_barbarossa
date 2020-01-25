
import my_sql_routines.my_sql_utilities as msu
import get_price.get_options_price as gop
from scipy.interpolate import interp1d
import get_price.get_futures_price as gfp
import option_models.utils as omu
import shared.calendar_utilities as cu
import numpy as np
import pandas as pd
import shared.utils as sut
import opportunity_constructs.utilities as ocu
import contract_utilities.contract_meta_info as cmi
import copy as cpy
import shared.statistics as stats
import datetime as dt

import warnings
warnings.filterwarnings("ignore", message="invalid value encountered in sign")

def get_vcs_signals(**kwargs):

    aligned_indicators_output = get_aligned_option_indicators(**kwargs)

    if not aligned_indicators_output['success']:
        return {'hist': [], 'current': [],
                'atm_vol_ratio': np.NaN,
            'fwd_vol': np.NaN,
            'downside': np.NaN,
            'upside': np.NaN,
            'real_vol_ratio': np.NaN,
            'atm_real_vol_ratio': np.NaN,
            'theta': np.NaN,
            'q': np.NaN, 'q1': np.NaN, 'fwd_vol_q': np.NaN}

    hist = aligned_indicators_output['hist']
    current = aligned_indicators_output['current']

    settle_datetime = cu.convert_doubledate_2datetime(kwargs['settle_date'])
    settle_datetime_1year_back = settle_datetime-dt.timedelta(360)

    hist['atm_vol_ratio'] = hist['c1']['imp_vol']/hist['c2']['imp_vol']

    if 'atm_vol_ratio' in kwargs.keys():
        atm_vol_ratio = kwargs['atm_vol_ratio']
    else:
        atm_vol_ratio = current['imp_vol'][0]/current['imp_vol'][1]

    hist_1year = hist[hist.index >= settle_datetime_1year_back]

    q = stats.get_quantile_from_number({'x': atm_vol_ratio,
                                        'y': hist['atm_vol_ratio'].to_numpy(), 'clean_num_obs': max(100, round(3*len(hist.index)/4))})

    q1 = stats.get_quantile_from_number({'x': atm_vol_ratio,
                                        'y': hist_1year['atm_vol_ratio'].to_numpy(), 'clean_num_obs': max(50, round(3*len(hist_1year.index)/4))})

    fwd_var = hist['c2']['cal_dte']*(hist['c2']['imp_vol']**2)-hist['c1']['cal_dte']*(hist['c1']['imp_vol']**2)
    fwd_vol_sq = fwd_var/(hist['c2']['cal_dte']-hist['c1']['cal_dte'])
    fwd_vol_adj = np.sign(fwd_vol_sq)*((abs(fwd_vol_sq)).apply(np.sqrt))
    hist['fwd_vol_adj'] = fwd_vol_adj

    fwd_var = current['cal_dte'][1]*(current['imp_vol'][1]**2)-current['cal_dte'][0]*(current['imp_vol'][0]**2)
    fwd_vol_sq = fwd_var/(current['cal_dte'][1]-current['cal_dte'][0])
    fwd_vol_adj = np.sign(fwd_vol_sq)*(np.sqrt(abs(fwd_vol_sq)))

    fwd_vol_q = stats.get_quantile_from_number({'x': fwd_vol_adj,
                                        'y': hist['fwd_vol_adj'].to_numpy(), 'clean_num_obs': max(100, round(3*len(hist.index)/4))})

    clean_indx = hist['c1']['profit5'].notnull()
    clean_data = hist[clean_indx]

    if clean_data.empty:
        downside = np.NaN
        upside = np.NaN
    else:
        last_available_align_date = clean_data.index[-1]
        clean_data = clean_data[clean_data.index >= last_available_align_date-dt.timedelta(5*365)]
        profit5 = clean_data['c1']['profit5']-clean_data['c2']['profit5']

        percentile_vector = stats.get_number_from_quantile(y=profit5.to_numpy(),
                                                       quantile_list=[1, 15, 85, 99],
                                                       clean_num_obs=max(100, round(3*len(profit5.to_numpy())/4)))

        downside = (percentile_vector[0]+percentile_vector[1])/2
        upside = (percentile_vector[2]+percentile_vector[3])/2

    return {'hist': hist, 'current': current,
            'atm_vol_ratio': atm_vol_ratio,
            'fwd_vol': fwd_vol_adj,
            'downside': downside,
            'upside': upside,
            'real_vol_ratio': current['close2close_vol20'][0]/current['close2close_vol20'][1],
            'atm_real_vol_ratio': current['imp_vol'][0]/current['close2close_vol20'][0],
            'theta': current['theta'][1]-current['theta'][0],
            'q': q, 'q1': q1, 'fwd_vol_q': fwd_vol_q}


def get_vcs_signals_legacy(**kwargs):

    aligned_indicators_output = get_aligned_option_indicators_legacy(**kwargs)

    if not aligned_indicators_output['success']:
        return {'atm_vol_ratio': np.NaN, 'q': np.NaN, 'q2': np.NaN, 'q1': np.NaN, 'q5': np.NaN,
            'fwd_vol_q': np.NaN, 'fwd_vol_q2': np.NaN, 'fwd_vol_q1': np.NaN, 'fwd_vol_q5': np.NaN,
             'atm_real_vol_ratio': np.NaN, 'q_atm_real_vol_ratio': np.NaN,
             'atm_real_vol_ratio_ratio': np.NaN, 'q_atm_real_vol_ratio_ratio': np.NaN,
             'tr_dte_diff_percent': np.NaN,'downside': np.NaN, 'upside': np.NaN, 'theta1': np.NaN, 'theta2': np.NaN, 'hist': []}

    hist = aligned_indicators_output['hist']
    current = aligned_indicators_output['current']
    settle_datetime = cu.convert_doubledate_2datetime(kwargs['settle_date'])

    settle_datetime_1year_back = settle_datetime-dt.timedelta(360)
    settle_datetime_5year_back = settle_datetime-dt.timedelta(5*360)

    hist['atm_vol_ratio'] = hist['c1']['imp_vol']/hist['c2']['imp_vol']

    fwd_var = hist['c2']['cal_dte']*(hist['c2']['imp_vol']**2)-hist['c1']['cal_dte']*(hist['c1']['imp_vol']**2)
    fwd_vol_sq = fwd_var/(hist['c2']['cal_dte']-hist['c1']['cal_dte'])
    fwd_vol_adj = np.sign(fwd_vol_sq)*((abs(fwd_vol_sq)).apply(np.sqrt))
    hist['fwd_vol_adj'] = fwd_vol_adj

    fwd_var = current['cal_dte'][1]*(current['imp_vol'][1]**2)-current['cal_dte'][0]*(current['imp_vol'][0]**2)
    fwd_vol_sq = fwd_var/(current['cal_dte'][1]-current['cal_dte'][0])
    fwd_vol_adj = np.sign(fwd_vol_sq)*(np.sqrt(abs(fwd_vol_sq)))

    atm_vol_ratio = current['imp_vol'][0]/current['imp_vol'][1]

    hist['atm_real_vol_ratio'] = hist['c1']['imp_vol']/hist['c1']['close2close_vol20']
    atm_real_vol_ratio = current['imp_vol'][0]/current['close2close_vol20'][0]

    hist['atm_real_vol_ratio_ratio'] = (hist['c1']['imp_vol']/hist['c1']['close2close_vol20'])/(hist['c2']['imp_vol']/hist['c2']['close2close_vol20'])
    atm_real_vol_ratio_ratio = (current['imp_vol'][0]/current['close2close_vol20'][0])/(current['imp_vol'][0]/current['close2close_vol20'][0])

    hist_1year = hist[hist.index >= settle_datetime_1year_back]
    hist_5year = hist[hist.index >= settle_datetime_5year_back]

    q = stats.get_quantile_from_number({'x': atm_vol_ratio,
                                        'y': hist['atm_vol_ratio'].values, 'clean_num_obs': max(100, round(3*len(hist.index)/4))})

    q2 = stats.get_quantile_from_number({'x': atm_vol_ratio, 'y': hist['atm_vol_ratio'].values[-40:], 'clean_num_obs': 30})

    q1 = stats.get_quantile_from_number({'x': atm_vol_ratio,
                                        'y': hist_1year['atm_vol_ratio'].values, 'clean_num_obs': max(50, round(3*len(hist_1year.index)/4))})

    q5 = stats.get_quantile_from_number({'x': atm_vol_ratio,
                                        'y': hist_5year['atm_vol_ratio'].values, 'clean_num_obs': max(100, round(3*len(hist_5year.index)/4))})

    fwd_vol_q = stats.get_quantile_from_number({'x': fwd_vol_adj,
                                                'y': hist['fwd_vol_adj'].values, 'clean_num_obs': max(100, round(3*len(hist.index)/4))})

    fwd_vol_q2 = stats.get_quantile_from_number({'x': fwd_vol_adj,
                                                 'y': hist['fwd_vol_adj'].values[-40:], 'clean_num_obs': 30})

    fwd_vol_q1 = stats.get_quantile_from_number({'x': fwd_vol_adj,
                                                 'y': hist_1year['fwd_vol_adj'].values, 'clean_num_obs': max(50, round(3*len(hist_1year.index)/4))})

    fwd_vol_q5 = stats.get_quantile_from_number({'x': fwd_vol_adj,
                                                 'y': hist_5year['fwd_vol_adj'].values, 'clean_num_obs': max(100, round(3*len(hist_5year.index)/4))})

    q_atm_real_vol_ratio = stats.get_quantile_from_number({'x': atm_real_vol_ratio,
                                                           'y': hist['atm_real_vol_ratio'].values, 'clean_num_obs': max(100, round(3*len(hist.index)/4))})

    q_atm_real_vol_ratio_ratio = stats.get_quantile_from_number({'x': atm_real_vol_ratio_ratio,
                                                                 'y': hist['atm_real_vol_ratio_ratio'].values, 'clean_num_obs': max(100, round(3*len(hist.index)/4))})

    tr_dte_diff_percent = round(100*(current['tr_dte'][1]-current['tr_dte'][0])/current['tr_dte'][0])

    profit5 = hist['c1']['profit5']-hist['c2']['profit5']

    clean_indx = profit5.notnull()
    clean_data = hist[clean_indx]

    if clean_data.empty:
        downside = np.NaN
        upside = np.NaN
    else:
        last_available_align_date = clean_data.index[-1]
        clean_data = clean_data[clean_data.index >= last_available_align_date-dt.timedelta(5*365)]
        profit5 = clean_data['c1']['profit5']-clean_data['c2']['profit5']

        percentile_vector = stats.get_number_from_quantile(y=profit5.values,
                                                       quantile_list=[1, 15, 85, 99],
                                                       clean_num_obs=max(100, round(3*len(profit5.values)/4)))

        downside = (percentile_vector[0]+percentile_vector[1])/2
        upside = (percentile_vector[2]+percentile_vector[3])/2

    return {'atm_vol_ratio': atm_vol_ratio, 'q': q, 'q2': q2, 'q1': q1, 'q5': q5,
            'fwd_vol_q': fwd_vol_q, 'fwd_vol_q2': fwd_vol_q2, 'fwd_vol_q1': fwd_vol_q1, 'fwd_vol_q5': fwd_vol_q5,
             'atm_real_vol_ratio': atm_real_vol_ratio, 'q_atm_real_vol_ratio': q_atm_real_vol_ratio,
             'atm_real_vol_ratio_ratio': atm_real_vol_ratio_ratio, 'q_atm_real_vol_ratio_ratio': q_atm_real_vol_ratio_ratio,
            'tr_dte_diff_percent': tr_dte_diff_percent, 'downside': downside, 'upside': upside, 'theta1': current['theta'][0], 'theta2': current['theta'][1], 'hist': hist}


def get_aligned_option_indicators_legacy(**kwargs):

    ticker_list = kwargs['ticker_list']
    tr_dte_list = kwargs['tr_dte_list']
    settle_datetime = cu.convert_doubledate_2datetime(kwargs['settle_date'])

    if 'num_cal_days_back' in kwargs.keys():
        num_cal_days_back = kwargs['num_cal_days_back']
    else:
        num_cal_days_back = 20*365

    settle_datetime_from = settle_datetime-dt.timedelta(num_cal_days_back)

    contract_specs_output_list = [cmi.get_contract_specs(x) for x in ticker_list]
    ticker_head_list = [x['ticker_head'] for x in contract_specs_output_list]

    cont_indx_list = [x['ticker_year']*100+x['ticker_month_num'] for x in contract_specs_output_list]
    month_seperation_list = [cmi.get_month_seperation_from_cont_indx(x,cont_indx_list[0]) for x in cont_indx_list]

    aggregation_method = max([ocu.get_aggregation_method_contracts_back({'ticker_class': x['ticker_class'],
                                                                         'ticker_head': x['ticker_head']})['aggregation_method'] for x in contract_specs_output_list])

    if (min(tr_dte_list) >= 80) and (aggregation_method == 1):
        aggregation_method = 3

    tr_days_half_band_width_selected = ocu.tr_days_half_band_with[aggregation_method]
    data_frame_list = []

    for x in range(len(ticker_list)):

        if ticker_head_list[x] in ['ED', 'E0', 'E2', 'E3', 'E4', 'E5']:
            model = 'OU'
        else:
            model = 'BS'

        tr_dte_upper_band = tr_dte_list[x]+tr_days_half_band_width_selected
        tr_dte_lower_band = tr_dte_list[x]-tr_days_half_band_width_selected

        ref_tr_dte_list = [y for y in cmi.aligned_data_tr_dte_list if y <= tr_dte_upper_band and y>=tr_dte_lower_band]

        if len(ref_tr_dte_list) == 0:
            return {'hist': [], 'current': [], 'success': False}

        if aggregation_method == 12:

            aligned_data = [gop.load_aligend_options_data_file(ticker_head=cmi.aligned_data_tickerhead[ticker_head_list[x]],
                                                    tr_dte_center=y,
                                                    contract_month_letter=contract_specs_output_list[x]['ticker_month_str'],
                                                    model=model) for y in ref_tr_dte_list]

        else:

            aligned_data = [gop.load_aligend_options_data_file(ticker_head=cmi.aligned_data_tickerhead[ticker_head_list[x]],
                                                    tr_dte_center=y,
                                                    model=model) for y in ref_tr_dte_list]

        aligned_data = [y[(y['trDTE'] >= tr_dte_lower_band)&(y['trDTE'] <= tr_dte_upper_band)] for y in aligned_data]

        aligned_data = pd.concat(aligned_data)
        aligned_data.drop('theta', axis=1, inplace=True)

        aligned_data['settle_date'] = pd.to_datetime(aligned_data['settleDates'].astype('str'), format='%Y%m%d')
        aligned_data = aligned_data[(aligned_data['settle_date'] <= settle_datetime)&(aligned_data['settle_date'] >= settle_datetime_from)]

        aligned_data.rename(columns={'TickerYear': 'ticker_year',
                                     'TickerMonth': 'ticker_month',
                                     'trDTE': 'tr_dte',
                                     'calDTE': 'cal_dte',
                                     'impVol': 'imp_vol',
                                     'close2CloseVol20': 'close2close_vol20',
                                     'dollarTheta': 'theta'}, inplace=True)

        aligned_data.sort(['settle_date', 'ticker_year', 'ticker_month'], ascending=[True,True,True],inplace=True)
        aligned_data.drop_duplicates(['settle_date','ticker_year','ticker_month'],inplace=True)

        aligned_data = aligned_data[['settle_date','ticker_month', 'ticker_year', 'cal_dte', 'tr_dte', 'imp_vol', 'theta', 'close2close_vol20', 'profit5']]

        aligned_data['cont_indx'] = 100*aligned_data['ticker_year']+aligned_data['ticker_month']
        aligned_data['cont_indx_adj'] = [cmi.get_cont_indx_from_month_seperation(y,-month_seperation_list[x]) for y in aligned_data['cont_indx']]

        data_frame_list.append(aligned_data)

    for x in range(len(ticker_list)):
        data_frame_list[x].set_index(['settle_date','cont_indx_adj'], inplace=True,drop=False)

    merged_dataframe = pd.concat(data_frame_list, axis=1, join='inner',keys=['c'+ str(x+1) for x in range(len(ticker_list))])
    merged_dataframe['abs_tr_dte_diff'] = abs(merged_dataframe['c1']['tr_dte']-tr_dte_list[0])
    merged_dataframe['settle_date'] = merged_dataframe['c1']['settle_date']
    merged_dataframe.sort(['settle_date', 'abs_tr_dte_diff'], ascending=[True,True], inplace=False)
    merged_dataframe.drop_duplicates('settle_date', inplace=True, take_last=False)

    merged_dataframe.index = merged_dataframe.index.droplevel(1)

    tr_dte_list = []
    cal_dte_list = []
    imp_vol_list = []
    theta_list = []
    close2close_vol20_list = []

    for x in range(len(ticker_list)):
        selected_data = merged_dataframe['c' + str(x+1)]

        if settle_datetime in selected_data.index:
            selected_data = selected_data.loc[settle_datetime]
        else:
            return {'hist': [], 'current': [], 'success': False}

        if selected_data['cont_indx'] != cont_indx_list[x]:
            return {'hist': [], 'current': [], 'success': False}

        tr_dte_list.append(selected_data['tr_dte'])
        cal_dte_list.append(selected_data['cal_dte'])
        imp_vol_list.append(selected_data['imp_vol'])
        theta_list.append(selected_data['theta'])
        close2close_vol20_list.append(selected_data['close2close_vol20'])

    current_data = pd.DataFrame.from_items([('ticker',ticker_list),
                             ('tr_dte', tr_dte_list),
                             ('cal_dte', cal_dte_list),
                             ('imp_vol', imp_vol_list),
                             ('theta', theta_list),
                             ('close2close_vol20', close2close_vol20_list)])

    current_data['settle_date'] = settle_datetime
    current_data.set_index('ticker', drop=True, inplace=True)

    return {'hist': merged_dataframe, 'current': current_data, 'success': True}


def get_aligned_option_indicators(**kwargs):

    ticker_list = kwargs['ticker_list']
    settle_datetime = cu.convert_doubledate_2datetime(kwargs['settle_date'])

    if 'num_cal_days_back' in kwargs.keys():
        num_cal_days_back = kwargs['num_cal_days_back']
    else:
        num_cal_days_back = 20*365

    settle_datetime_from = settle_datetime-dt.timedelta(num_cal_days_back)

    contract_specs_output_list = [cmi.get_contract_specs(x) for x in ticker_list]
    ticker_head_list = [x['ticker_head'] for x in contract_specs_output_list]
    contract_multiplier_list = [cmi.contract_multiplier[x['ticker_head']] for x in contract_specs_output_list]

    cont_indx_list = [x['ticker_year']*100+x['ticker_month_num'] for x in contract_specs_output_list]
    month_seperation_list = [cmi.get_month_seperation_from_cont_indx(x,cont_indx_list[0]) for x in cont_indx_list]

    if 'option_ticker_indicator_dictionary' in kwargs.keys():
        option_ticker_indicator_dictionary = kwargs['option_ticker_indicator_dictionary']
    else:
        con = msu.get_my_sql_connection(**kwargs)
        unique_ticker_heads = list(set(ticker_head_list))
        option_ticker_indicator_dictionary = {x: get_option_ticker_indicators(ticker_head=x,
                                                                              settle_date_to=kwargs['settle_date'],
                                                                              num_cal_days_back=num_cal_days_back,
                                                                              con=con) for x in unique_ticker_heads}
        if 'con' not in kwargs.keys():
            con.close()

    option_ticker_indicator_dictionary_final = {ticker_list[x]: option_ticker_indicator_dictionary[ticker_head_list[x]] for x in range(len(ticker_list))}

    max_available_settle_list = []
    tr_dte_list = []
    cal_dte_list = []
    imp_vol_list = []
    theta_list = []
    close2close_vol20_list = []
    volume_list = []
    open_interest_list = []

    for x in range(len(ticker_list)):
        ticker_data = option_ticker_indicator_dictionary_final[ticker_list[x]]
        ticker_data = ticker_data[ticker_data['settle_date'] <= settle_datetime]
        option_ticker_indicator_dictionary_final[ticker_list[x]] = ticker_data
        ticker_data = ticker_data[ticker_data['ticker'] == ticker_list[x]]
        max_available_settle_list.append(ticker_data['settle_date'].iloc[-1])

    last_available_settle = min(max_available_settle_list)

    for x in range(len(ticker_list)):
        ticker_data = option_ticker_indicator_dictionary_final[ticker_list[x]]
        ticker_data = ticker_data[(ticker_data['ticker'] == ticker_list[x]) & (ticker_data['settle_date'] == last_available_settle)]
        tr_dte_list.append(ticker_data['tr_dte'].iloc[0])
        cal_dte_list.append(ticker_data['cal_dte'].iloc[0])
        imp_vol_list.append(ticker_data['imp_vol'].iloc[0])
        theta_list.append(ticker_data['theta'].iloc[0]*contract_multiplier_list[x])
        close2close_vol20_list.append(ticker_data['close2close_vol20'].iloc[0])
        volume_list.append(ticker_data['volume'].iloc[0])
        open_interest_list.append(ticker_data['open_interest'].iloc[0])


    current_data = pd.DataFrame.from_dict({'ticker': ticker_list,
                             'tr_dte': tr_dte_list,
                             'cal_dte': cal_dte_list,
                             'imp_vol': imp_vol_list,
                             'theta': theta_list,
                             'close2close_vol20': close2close_vol20_list,
                             'volume': volume_list,
                             'open_interest': open_interest_list})

    current_data['settle_date'] = last_available_settle
    current_data.set_index('ticker', drop=True, inplace=True)

    current_data = current_data[['settle_date', 'tr_dte', 'cal_dte', 'imp_vol', 'close2close_vol20', 'theta', 'volume', 'open_interest']]

    aggregation_method = max([ocu.get_aggregation_method_contracts_back({'ticker_class': x['ticker_class'],
                                                                         'ticker_head': x['ticker_head']})['aggregation_method'] for x in contract_specs_output_list])

    if (current_data['tr_dte'].min() >= 80) and (aggregation_method == 1):
        aggregation_method = 3

    tr_days_half_band_width_selected = ocu.tr_days_half_band_with[aggregation_method]
    data_frame_list = []
    ref_tr_dte_list_list = []

    for x in range(len(ticker_list)):
        ticker_data = option_ticker_indicator_dictionary_final[ticker_list[x]]

        if ticker_head_list[x] in ['ED', 'E0', 'E2', 'E3', 'E4', 'E5']:
            model = 'OU'
        else:
            model = 'BS'

        tr_dte_upper_band = current_data['tr_dte'].loc[ticker_list[x]]+tr_days_half_band_width_selected
        tr_dte_lower_band = current_data['tr_dte'].loc[ticker_list[x]]-tr_days_half_band_width_selected

        ref_tr_dte_list = [y for y in cmi.aligned_data_tr_dte_list if y <= tr_dte_upper_band and y>=tr_dte_lower_band]

        if len(ref_tr_dte_list) == 0:
            return {'hist': [], 'current': [], 'success': False}

        if aggregation_method == 12:

            aligned_data = [gop.load_aligend_options_data_file(ticker_head=cmi.aligned_data_tickerhead[ticker_head_list[x]],
                                                    tr_dte_center=y,
                                                    contract_month_letter=contract_specs_output_list[x]['ticker_month_str'],
                                                    model=model) for y in ref_tr_dte_list]

            ticker_data = ticker_data[ticker_data['ticker_month'] == contract_specs_output_list[x]['ticker_month_num']]

        else:

            aligned_data = [gop.load_aligend_options_data_file(ticker_head=cmi.aligned_data_tickerhead[ticker_head_list[x]],
                                                    tr_dte_center=y,
                                                    model=model) for y in ref_tr_dte_list]

        aligned_data = [y[(y['trDTE'] >= current_data['tr_dte'].loc[ticker_list[x]]-tr_days_half_band_width_selected)&
              (y['trDTE'] <= current_data['tr_dte'].loc[ticker_list[x]]+tr_days_half_band_width_selected)] for y in aligned_data]

        aligned_data = pd.concat(aligned_data)

        aligned_data['settle_date'] = pd.to_datetime(aligned_data['settleDates'].astype('str'), format='%Y%m%d')

        aligned_data.rename(columns={'TickerYear': 'ticker_year',
                                     'TickerMonth': 'ticker_month',
                                     'trDTE': 'tr_dte',
                                     'calDTE': 'cal_dte',
                                     'impVol': 'imp_vol',
                                     'close2CloseVol20': 'close2close_vol20'}, inplace=True)

        aligned_data.sort_values(['settle_date', 'ticker_year', 'ticker_month'], ascending=[True,True,True],inplace=True)
        aligned_data.drop_duplicates(['settle_date','ticker_year','ticker_month'],inplace=True)
        aligned_data['old_aligned'] = True

        aligned_data = aligned_data[['settle_date','ticker_month', 'ticker_year', 'cal_dte', 'tr_dte', 'imp_vol', 'close2close_vol20', 'profit5', 'old_aligned']]

        tr_dte_selection = (ticker_data['tr_dte'] >= current_data['tr_dte'].loc[ticker_list[x]]-tr_days_half_band_width_selected)&\
                           (ticker_data['tr_dte'] <= current_data['tr_dte'].loc[ticker_list[x]]+tr_days_half_band_width_selected)

        ticker_data = ticker_data[tr_dte_selection]

        ticker_data['old_aligned'] = False
        ticker_data['profit5'] = (ticker_data['option_pnl5']-ticker_data['delta_pnl5']).astype(float)*cmi.contract_multiplier[ticker_head_list[x]]
        #ticker_data['profit5'] = np.nan
        ticker_data = pd.concat([aligned_data, ticker_data[['settle_date', 'ticker_month', 'ticker_year', 'cal_dte', 'tr_dte', 'imp_vol', 'close2close_vol20', 'profit5', 'old_aligned']]])

        ticker_data = ticker_data[(ticker_data['settle_date'] <= settle_datetime)&(ticker_data['settle_date'] >= settle_datetime_from)]

        ticker_data['cont_indx'] = 100*ticker_data['ticker_year']+ticker_data['ticker_month']
        ticker_data['cont_indx_adj'] = [cmi.get_cont_indx_from_month_seperation(y,-month_seperation_list[x]) for y in ticker_data['cont_indx']]

        data_frame_list.append(ticker_data)
        ref_tr_dte_list_list.append(ref_tr_dte_list)

    for x in range(len(ticker_list)):
        data_frame_list[x].set_index(['settle_date','cont_indx_adj'], inplace=True,drop=False)
        data_frame_list[x]['imp_vol'] = data_frame_list[x]['imp_vol'].astype('float64')

    merged_dataframe = pd.concat(data_frame_list, axis=1, join='inner',keys=['c'+ str(x+1) for x in range(len(ticker_list))])
    merged_dataframe['abs_tr_dte_diff'] = abs(merged_dataframe['c1']['tr_dte']-tr_dte_list[0])
    merged_dataframe['settle_date'] = merged_dataframe['c1']['settle_date']

    merged_dataframe.index.names = ['settle_date_index' if x is 'settle_date' else x for x in merged_dataframe.index.names]
    merged_dataframe.sort_values(['settle_date', 'abs_tr_dte_diff'], ascending=[True,True], inplace=False)

    unique_index = np.unique(merged_dataframe['settle_date'], return_index=True)[1]
    merged_dataframe = merged_dataframe.iloc[unique_index]
    merged_dataframe.index = merged_dataframe.index.droplevel(1)

    return {'hist': merged_dataframe, 'current': current_data, 'success': True}


def calc_delta_vol_4ticker(**kwargs):

    delta_target = kwargs['delta_target']
    del kwargs['delta_target']
    delta_max_deviation = 0.15

    skew_output = gop.get_options_price_from_db(column_names=['delta', 'imp_vol', 'strike', 'theta', 'cal_dte', 'tr_dte', 'option_pnl5', 'delta_pnl5'], **kwargs)

    if skew_output.empty:
        tr_dte = np.NaN
        cal_dte = np.NaN
    else:
        tr_dte = skew_output['tr_dte'][0]
        cal_dte = skew_output['cal_dte'][0]

    output_dict = {'delta_vol': np.NaN, 'strike': np.NaN, 'theta': np.NaN, 'cal_dte': cal_dte, 'tr_dte': tr_dte, 'option_pnl5': np.nan, 'delta_pnl5': np.nan}

    skew_output = skew_output[(skew_output['imp_vol'].notnull())]

    delta_band = [delta_target*x for x in [1-delta_max_deviation, 1+delta_max_deviation]]

    skew_output_select = skew_output[(skew_output['delta'] <= max(delta_band)) & (skew_output['delta'] >= min(delta_band))]

    if skew_output_select.empty:
        delta_target = -(1-delta_target)
        delta_band = [delta_target*x for x in [1-delta_max_deviation, 1+delta_max_deviation]]
        skew_output_select = skew_output[(skew_output['delta'] <= max(delta_band)) & (skew_output['delta'] >= min(delta_band))]
        if skew_output_select.empty:
            return output_dict

    skew_output_select['delta_diff'] = abs(skew_output_select['delta']-delta_target)
    skew_output_select.sort_values('delta_diff', ascending=True, inplace=True)

    output_dict['delta_vol'] = skew_output_select['imp_vol'].iloc[0]
    output_dict['strike'] = skew_output_select['strike'].iloc[0]
    output_dict['theta'] = skew_output_select['theta'].iloc[0]
    output_dict['option_pnl5'] = skew_output_select['option_pnl5'].iloc[0]
    output_dict['delta_pnl5'] = skew_output_select['delta_pnl5'].iloc[0]

    return output_dict


def calc_volume_interest_4ticker(**kwargs):

    con = msu.get_my_sql_connection(**kwargs)

    sql_query = 'SELECT sum(volume),sum(open_interest) FROM futures_master.daily_option_price WHERE ticker=\'' + kwargs['ticker'] + '\'' + \
                ' and price_date=' + str(kwargs['settle_date'])

    cur = con.cursor()
    cur.execute(sql_query)
    data = cur.fetchall()

    if 'con' not in kwargs.keys():
        con.close()

    return {'volume': int(data[0][0]) if data[0][0] is not None else np.NaN,
            'open_interest': int(data[0][1]) if data[0][1] is not None else np.NaN}


def calc_realized_vol_4options_ticker(**kwargs):

    ticker = kwargs['ticker']
    contract_specs_output = cmi.get_contract_specs(ticker)

    if contract_specs_output['ticker_class'] in ['Index', 'FX', 'Metal']:
        use_proxy_contract = True
    else:
        use_proxy_contract = False

    if use_proxy_contract:

        if 'futures_data_dictionary' in kwargs.keys():
            futures_data_input = {'ticker_head': contract_specs_output['ticker_head'],'settle_date': kwargs['settle_date']}
            futures_data_input['futures_data_dictionary'] = kwargs['futures_data_dictionary']
            data_out = gfp.get_futures_price_preloaded(**futures_data_input)
            data_out = data_out.reset_index()
            kwargs['ticker'] = data_out['ticker'].loc[data_out['volume'].idxmax()]
    else:
        kwargs['ticker'] = omu.get_option_underlying(**kwargs)

    return calc_realized_vol_4futures_ticker(**kwargs)


def calc_realized_vol_4futures_ticker(**kwargs):

    settle_date = kwargs.pop('settle_date')

    num_obs = kwargs.pop('num_obs', 20)

    futures_price_output = gfp.get_futures_price_preloaded(**kwargs)

    settle_datetime = cu.convert_doubledate_2datetime(settle_date)

    futures_price_selected = futures_price_output[futures_price_output['settle_date'] <= settle_datetime]

    logreturns = np.log(futures_price_selected['close_price'][-(num_obs+1):]/
                         futures_price_selected['close_price'][-(num_obs+1):].shift(1))

    return 100*np.sqrt(252*np.mean(np.square(logreturns)))


def get_option_ticker_indicators(**kwargs):

    con = msu.get_my_sql_connection(**kwargs)

    if 'ticker' in kwargs.keys():
        filter_string = 'WHERE ticker=\'' + kwargs['ticker'] + '\''
    elif 'ticker_head' in kwargs.keys():
        filter_string = 'WHERE ticker_head=\'' + kwargs['ticker_head'] + '\''
    else:
        filter_string = ''

    if 'settle_date' in kwargs.keys():
        if filter_string == '':
            filter_string = filter_string + ' WHERE price_date=' + str(kwargs['settle_date'])
        else:
            filter_string = filter_string + ' and price_date=' + str(kwargs['settle_date'])

    if 'settle_date_to' in kwargs.keys():
        if filter_string == '':
            filter_string = filter_string + ' WHERE price_date<=' + str(kwargs['settle_date_to'])
        else:
            filter_string = filter_string + ' and price_date<=' + str(kwargs['settle_date_to'])

    if 'delta' in kwargs.keys():
        if filter_string == '':
            filter_string = ' WHERE delta=' + str(kwargs['delta'])
        else:
            filter_string = filter_string + ' and delta=0.5'

    if 'num_cal_days_back' in kwargs.keys():
        date_from = cu.doubledate_shift(kwargs['settle_date_to'], kwargs['num_cal_days_back'])
        filter_string = filter_string + ' and price_date>=' + str(date_from)

    if 'column_names' in kwargs.keys():
        column_names = kwargs['column_names']
    else:
        column_names = ['ticker', 'price_date' , 'ticker_head', 'ticker_month', 'ticker_year', 'cal_dte', 'tr_dte', 'imp_vol', 'theta','close2close_vol20',
                        'option_pnl5', 'delta_pnl5','volume','open_interest']

    sql_query = 'SELECT ' + ",".join(column_names) + ' FROM option_ticker_indicators ' + filter_string

    cur = con.cursor()
    cur.execute(sql_query)
    data = cur.fetchall()

    if 'con' not in kwargs.keys():
        con.close()

    data_frame_output = pd.DataFrame(data, columns=['settle_date' if x == 'price_date' else x for x in column_names])

    for x in ['imp_vol', 'close2close_vol20', 'theta']:

        if x in column_names:
            data_frame_output[x] = data_frame_output[x].astype('float64')

    return data_frame_output.sort_values(['ticker_head','settle_date', 'tr_dte'], ascending=[True, True,True], inplace=False)






