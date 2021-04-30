import sys
sys.path.append(r'C:\Users\kocat_000\quantFinance\PycharmProjects')
import contract_utilities.expiration as exp
import opportunity_constructs.utilities as opUtil
import contract_utilities.contract_meta_info as cmi
import shared.directory_names_aux as dna
import get_price.get_futures_price as gfp
import shared.statistics as stats
import shared.calendar_utilities as cu
from statsmodels.tsa.stattools import adfuller
import signals.utils as su
import numpy as np
import pandas as pd
import os.path

def get_futures_butterfly_signals(**kwargs):

    ticker_list = kwargs['ticker_list']
    date_to = kwargs['date_to']

    #print(ticker_list)

    if 'tr_dte_list' in kwargs.keys():
        tr_dte_list = kwargs['tr_dte_list']
    else:
        tr_dte_list = [exp.get_futures_days2_expiration({'ticker': x,'date_to': date_to}) for x in ticker_list]

    if 'aggregation_method' in kwargs.keys() and 'contracts_back' in kwargs.keys():
        aggregation_method = kwargs['aggregation_method']
        contracts_back = kwargs['contracts_back']
    else:
        amcb_output = opUtil.get_aggregation_method_contracts_back(cmi.get_contract_specs(ticker_list[0]))
        aggregation_method = amcb_output['aggregation_method']
        contracts_back = amcb_output['contracts_back']

    if 'use_last_as_current' in kwargs.keys():
        use_last_as_current = kwargs['use_last_as_current']
    else:
        use_last_as_current = False

    if 'futures_data_dictionary' in kwargs.keys():
        futures_data_dictionary = kwargs['futures_data_dictionary']
    else:
        futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in [cmi.get_contract_specs(ticker_list[0])['ticker_head']]}

    if 'contract_multiplier' in kwargs.keys():
        contract_multiplier = kwargs['contract_multiplier']
    else:
        contract_multiplier = cmi.contract_multiplier[cmi.get_contract_specs(ticker_list[0])['ticker_head']]

    if 'datetime5_years_ago' in kwargs.keys():
        datetime5_years_ago = kwargs['datetime5_years_ago']
    else:
        date5_years_ago = cu.doubledate_shift(date_to,5*365)
        datetime5_years_ago = cu.convert_doubledate_2datetime(date5_years_ago)

    if 'datetime2_months_ago' in kwargs.keys():
        datetime2_months_ago = kwargs['datetime2_months_ago']
    else:
        date2_months_ago = cu.doubledate_shift(date_to,60)
        datetime2_months_ago = cu.convert_doubledate_2datetime(date2_months_ago)

    aligned_output = opUtil.get_aligned_futures_data(contract_list=ticker_list,
                                                          tr_dte_list=tr_dte_list,
                                                          aggregation_method=aggregation_method,
                                                          contracts_back=contracts_back,
                                                          date_to=date_to,
                                                          futures_data_dictionary=futures_data_dictionary,
                                                          use_last_as_current=use_last_as_current)
    if not aligned_output['success']:
        return {'success': False}

    current_data = aligned_output['current_data']
    aligned_data = aligned_output['aligned_data']

    month_diff_1 = 12*(current_data['c1']['ticker_year']-current_data['c2']['ticker_year'])+(current_data['c1']['ticker_month']-current_data['c2']['ticker_month'])
    month_diff_2 = 12*(current_data['c2']['ticker_year']-current_data['c3']['ticker_year'])+(current_data['c2']['ticker_month']-current_data['c3']['ticker_month'])

    weight_11 = 2*month_diff_2/(month_diff_1+month_diff_1)
    weight_12 = -2
    weight_13 = 2*month_diff_1/(month_diff_1+month_diff_1)

    price_1 = current_data['c1']['close_price']
    price_2 = current_data['c2']['close_price']
    price_3 = current_data['c3']['close_price']

    price_1_aligned = aligned_data['c1']['close_price']
    price_2_aligned = aligned_data['c2']['close_price']
    price_3_aligned = aligned_data['c3']['close_price']


    linear_interp_price2 = (weight_11*price_1_aligned+weight_13*price_3_aligned)/2

    butterfly_price = price_1_aligned-2*price_2_aligned+price_3_aligned

    price_ratio = linear_interp_price2/price_2_aligned

    linear_interp_price2_current = (weight_11*price_1+weight_13*price_3)/2

    price_ratio_current = linear_interp_price2_current/price_2

    q = stats.get_quantile_from_number({'x': price_ratio_current, 'y': price_ratio.values, 'clean_num_obs': max(100, round(3*len(price_ratio.values)/4))})
    qf = stats.get_quantile_from_number({'x': price_ratio_current, 'y': price_ratio.values[-40:], 'clean_num_obs': 30})

    recent_quantile_list = [stats.get_quantile_from_number({'x': x, 'y': price_ratio.values[-40:], 'clean_num_obs': 30}) for x in price_ratio.values[-40:]]

    weight1 = weight_11
    weight2 = weight_12
    weight3 = weight_13

    date3_years_ago = cu.doubledate_shift(date_to, 3 * 365)
    datetime3_years_ago = cu.convert_doubledate_2datetime(date3_years_ago)

    date10_years_ago = cu.doubledate_shift(date_to, 10 * 365)
    datetime10_years_ago = cu.convert_doubledate_2datetime(date10_years_ago)

    last10_years_indx = aligned_data['settle_date'] >= datetime10_years_ago
    last5_years_indx = aligned_data['settle_date']>=datetime5_years_ago
    last3_years_indx = aligned_data['settle_date'] >= datetime3_years_ago
    last2_months_indx = aligned_data['settle_date']>=datetime2_months_ago
    data_last5_years = aligned_data[last5_years_indx]

    yield1 = 100*(price_1_aligned-price_2_aligned)/price_2_aligned
    yield2 = 100*(price_2_aligned-price_3_aligned)/price_3_aligned

    yield1_last5_years = yield1[last5_years_indx]
    yield2_last5_years = yield2[last5_years_indx]

    yield1_current = 100*(price_1-price_2)/price_2
    yield2_current = 100*(price_2-price_3)/price_3

    butterfly_price_current = current_data['c1']['close_price']\
                            -2*current_data['c2']['close_price']\
                              +current_data['c3']['close_price']

    spread_1_aligned = price_1_aligned - price_2_aligned
    spread_2_aligned = price_2_aligned - price_3_aligned

    spread_1 = price_1-price_2
    spread_2 = price_2-price_3

    #return {'yield1': yield1, 'yield2': yield2, 'yield1_current':yield1_current, 'yield2_current': yield2_current}

    yield_regress_output = stats.get_regression_results({'x':yield2, 'y':yield1,'x_current': yield2_current, 'y_current': yield1_current,
                                                         'clean_num_obs': max(100, round(3*len(yield1.values)/4))})

    yield_regress_output2 = stats.get_regression_results({'x': yield1, 'y': yield2, 'x_current': yield1_current, 'y_current': yield2_current,
                                                         'clean_num_obs': max(100, round(3 * len(yield1.values) / 4))})

    yield_regress_output3 = stats.get_regression_results({'x': spread_2_aligned, 'y': spread_1_aligned, 'x_current': spread_2, 'y_current': spread_1,
                                                          'clean_num_obs': max(100, round(3 * len(yield1.values) / 4))})

    yield_regress_output4 = stats.get_regression_results(
        {'x': spread_1_aligned, 'y': spread_2_aligned, 'x_current': spread_1, 'y_current': spread_2,
         'clean_num_obs': max(100, round(3 * len(yield1.values) / 4))})

    spread1_last10_years = spread_1_aligned[last10_years_indx]
    spread2_last10_years = spread_2_aligned[last10_years_indx]

    spread1_last5_years = spread_1_aligned[last5_years_indx]
    spread2_last5_years = spread_2_aligned[last5_years_indx]

    spread1_last3_years = spread_1_aligned[last3_years_indx]
    spread2_last3_years = spread_2_aligned[last3_years_indx]


    try:
        adf_result = adfuller(yield_regress_output['results'].resid, autolag='AIC')
        adfp1 = adf_result[1]
    except:
        adfp1 = np.nan

    try:
        adf_result2 = adfuller(yield_regress_output2['results'].resid, autolag='AIC')
        adfp2 = adf_result2[1]
    except:
        adfp2 = np.nan

    yield_regress_output_last5_years = stats.get_regression_results({'x':yield2_last5_years, 'y':yield1_last5_years,
                                                                     'x_current': yield2_current, 'y_current': yield1_current,
                                                                     'clean_num_obs': max(100, round(3*len(yield1_last5_years.values)/4))})

    spread_regress_output_last10_years = stats.get_regression_results({'x': spread1_last10_years, 'y': spread2_last10_years,
         'x_current': spread_1,'y_current': spread_2,'clean_num_obs': max(100, round(3 * len(spread1_last10_years.values) / 4))})


    spread_regress_output_last5_years = stats.get_regression_results({'x': spread1_last5_years, 'y': spread2_last5_years,
                                                                     'x_current': spread_1,
                                                                     'y_current': spread_2,
                                                                     'clean_num_obs': max(100, round(
                                                                         3 * len(yield1_last5_years.values) / 4))})

    spread_regress_output_last3_years = stats.get_regression_results({'x': spread1_last3_years, 'y': spread2_last3_years,
                                                                     'x_current': spread_1,
                                                                     'y_current': spread_2,
                                                                     'clean_num_obs': max(100, round(
                                                                         3 * len(spread1_last3_years.values) / 4))})

    bf_qz_frame_short = pd.DataFrame()
    bf_qz_frame_long = pd.DataFrame()

    if (len(yield1) >= 40)&(len(yield2) >= 40):

        recent_zscore_list = [(yield1[-40+i]-yield_regress_output['alpha']-yield_regress_output['beta']*yield2[-40+i])/yield_regress_output['residualstd'] for i in range(40)]

        bf_qz_frame = pd.DataFrame.from_dict({'bf_price': butterfly_price.values[-40:],
                                              'q': recent_quantile_list,
                                              'zscore': recent_zscore_list})

        bf_qz_frame = np.round(bf_qz_frame, 8)
        bf_qz_frame.drop_duplicates(['bf_price'], keep='last', inplace=True)

    # return bf_qz_frame

        bf_qz_frame_short = bf_qz_frame[(bf_qz_frame['zscore'] >= 0.6) & (bf_qz_frame['q'] >= 85)]
        bf_qz_frame_long = bf_qz_frame[(bf_qz_frame['zscore'] <= -0.6) & (bf_qz_frame['q'] <= 12)]

    if bf_qz_frame_short.empty:
        short_price_limit = np.NAN
    else:
        short_price_limit = bf_qz_frame_short['bf_price'].min()

    if bf_qz_frame_long.empty:
        long_price_limit = np.NAN
    else:
        long_price_limit = bf_qz_frame_long['bf_price'].max()

    zscore1= yield_regress_output['zscore']
    rsquared1= yield_regress_output['rsquared']

    zscore2= yield_regress_output_last5_years['zscore']
    rsquared2= yield_regress_output_last5_years['rsquared']

    zscore12 = yield_regress_output2['zscore']

    zscore3 = yield_regress_output3['zscore']
    rsquared3 = yield_regress_output3['rsquared']

    zscore32 = yield_regress_output4['zscore']

    zscore4 = spread_regress_output_last10_years['zscore']
    zscore5 = spread_regress_output_last5_years['zscore']
    zscore6 = spread_regress_output_last3_years['zscore']

    rsquared4 = spread_regress_output_last10_years['rsquared']
    rsquared5 = spread_regress_output_last5_years['rsquared']
    rsquared6 = spread_regress_output_last3_years['rsquared']


    second_spread_weight_1 = yield_regress_output['beta']
    second_spread_weight_2 = yield_regress_output_last5_years['beta']
    second_spread_weight_12 = 1 / yield_regress_output2['beta']

    second_spread_weight_3 = yield_regress_output3['beta']
    second_spread_weight_32 = 1/yield_regress_output4['beta']

    second_spread_weight_4 = 1/spread_regress_output_last10_years['beta']
    second_spread_weight_5 = 1/spread_regress_output_last5_years['beta']
    second_spread_weight_6 = 1/spread_regress_output_last3_years['beta']

    butterfly_price_w3 = spread_1_aligned - second_spread_weight_3*spread_2_aligned
    butterfly_price_w4 = spread_1_aligned - second_spread_weight_4*spread_2_aligned
    butterfly_price_w5 = spread_1_aligned - second_spread_weight_5*spread_2_aligned
    butterfly_price_w6 = spread_1_aligned - second_spread_weight_6*spread_2_aligned

    butterfly_price_wc3 = spread_1 - second_spread_weight_3*spread_2
    butterfly_price_wc4 = spread_1 - second_spread_weight_4*spread_2
    butterfly_price_wc5 = spread_1 - second_spread_weight_5*spread_2
    butterfly_price_wc6 = spread_1 - second_spread_weight_6*spread_2

    qf3 = stats.get_quantile_from_number({'x': butterfly_price_wc3, 'y': butterfly_price_w3.values[-40:], 'clean_num_obs': 30})
    qf4 = stats.get_quantile_from_number({'x': butterfly_price_wc4, 'y': butterfly_price_w4.values[-40:], 'clean_num_obs': 30})
    qf5 = stats.get_quantile_from_number({'x': butterfly_price_wc5, 'y': butterfly_price_w5.values[-40:], 'clean_num_obs': 30})
    qf6 = stats.get_quantile_from_number({'x': butterfly_price_wc6, 'y': butterfly_price_w6.values[-40:], 'clean_num_obs': 30})



    butterfly_5_change = aligned_data['c1']['change_5']\
                             - (1+second_spread_weight_1)*aligned_data['c2']['change_5']\
                             + second_spread_weight_1*aligned_data['c3']['change_5']

    butterfly_5_change_current = current_data['c1']['change_5']\
                             - (1+second_spread_weight_1)*current_data['c2']['change_5']\
                             + second_spread_weight_1*current_data['c3']['change_5']

    butterfly_1_change = data_last5_years['c1']['change_1']\
                             - (1+second_spread_weight_1)*data_last5_years['c2']['change_1']\
                             + second_spread_weight_1*data_last5_years['c3']['change_1']

    percentile_vector = stats.get_number_from_quantile(y=butterfly_5_change.values,
                                                       quantile_list=[1, 15, 85, 99],
                                                       clean_num_obs=max(100, round(3*len(butterfly_5_change.values)/4)))

    downside = contract_multiplier*(percentile_vector[0]+percentile_vector[1])/2
    upside = contract_multiplier*(percentile_vector[2]+percentile_vector[3])/2


    ##################################


    butterfly_5_change12 = aligned_data['c1']['change_5'] \
                         - (1 + second_spread_weight_12) * aligned_data['c2']['change_5'] \
                         + second_spread_weight_12 * aligned_data['c3']['change_5']

    percentile_vector12 = stats.get_number_from_quantile(y=butterfly_5_change12.values,
                                                       quantile_list=[1, 15, 85, 99],
                                                       clean_num_obs=max(100,
                                                                         round(3 * len(butterfly_5_change12.values) / 4)))

    downside12 = contract_multiplier * (percentile_vector12[0] + percentile_vector12[1]) / 2
    upside12 = contract_multiplier * (percentile_vector12[2] + percentile_vector12[3]) / 2

    ###################################

    butterfly_5_change3 = aligned_data['c1']['change_5'] \
                           - (1 + second_spread_weight_3) * aligned_data['c2']['change_5'] \
                           + second_spread_weight_3 * aligned_data['c3']['change_5']

    percentile_vector3 = stats.get_number_from_quantile(y=butterfly_5_change3.values,
                                                         quantile_list=[1, 15, 85, 99],
                                                         clean_num_obs=max(100,
                                                                           round(3 * len(
                                                                               butterfly_5_change3.values) / 4)))

    downside3 = contract_multiplier * (percentile_vector3[0] + percentile_vector3[1]) / 2
    upside3 = contract_multiplier * (percentile_vector3[2] + percentile_vector3[3]) / 2

    ###################################

    butterfly_5_change32 = aligned_data['c1']['change_5'] \
                          - (1 + second_spread_weight_32) * aligned_data['c2']['change_5'] \
                          + second_spread_weight_32 * aligned_data['c3']['change_5']

    percentile_vector32 = stats.get_number_from_quantile(y=butterfly_5_change32.values,
                                                        quantile_list=[1, 15, 85, 99],
                                                        clean_num_obs=max(100,
                                                                          round(3 * len(
                                                                              butterfly_5_change3.values) / 4)))

    downside32 = contract_multiplier * (percentile_vector32[0] + percentile_vector32[1]) / 2
    upside32 = contract_multiplier * (percentile_vector32[2] + percentile_vector32[3]) / 2

    ###########################################

    butterfly_5_change4 = aligned_data['c1']['change_5'] \
                           - (1 + second_spread_weight_4) * aligned_data['c2']['change_5'] \
                           + second_spread_weight_4 * aligned_data['c3']['change_5']

    percentile_vector4 = stats.get_number_from_quantile(y=butterfly_5_change4.values,
                                                         quantile_list=[1, 15, 85, 99],
                                                         clean_num_obs=max(100,
                                                                           round(3 * len(
                                                                               butterfly_5_change4.values) / 4)))

    downside4 = contract_multiplier * (percentile_vector4[0] + percentile_vector4[1]) / 2
    upside4 = contract_multiplier * (percentile_vector4[2] + percentile_vector4[3]) / 2

    ###########################################

    butterfly_5_change5 = aligned_data['c1']['change_5'] \
                          - (1 + second_spread_weight_5) * aligned_data['c2']['change_5'] \
                          + second_spread_weight_5 * aligned_data['c3']['change_5']

    percentile_vector5 = stats.get_number_from_quantile(y=butterfly_5_change5.values,
                                                        quantile_list=[1, 15, 85, 99],
                                                        clean_num_obs=max(100,
                                                                          round(3 * len(
                                                                              butterfly_5_change5.values) / 4)))

    downside5 = contract_multiplier * (percentile_vector5[0] + percentile_vector5[1]) / 2
    upside5 = contract_multiplier * (percentile_vector5[2] + percentile_vector5[3]) / 2

    ###########################################

    butterfly_5_change6 = aligned_data['c1']['change_5'] \
                          - (1 + second_spread_weight_6) * aligned_data['c2']['change_5'] \
                          + second_spread_weight_6 * aligned_data['c3']['change_5']

    percentile_vector6 = stats.get_number_from_quantile(y=butterfly_5_change6.values,
                                                        quantile_list=[1, 15, 85, 99],
                                                        clean_num_obs=max(100,
                                                                          round(3 * len(
                                                                              butterfly_5_change6.values) / 4)))

    downside6 = contract_multiplier * (percentile_vector6[0] + percentile_vector6[1]) / 2
    upside6 = contract_multiplier * (percentile_vector6[2] + percentile_vector6[3]) / 2

    ###########################################



    recent_5day_pnl = contract_multiplier*butterfly_5_change_current

    residuals = yield1-yield_regress_output['alpha']-yield_regress_output['beta']*yield2

    residuals3 = spread_1_aligned - yield_regress_output3['alpha'] - yield_regress_output3['beta'] * spread_2_aligned

    regime_change_ind = (residuals[last5_years_indx].mean()-residuals.mean())/residuals.std()

    seasonal_residuals = residuals[aligned_data['c1']['ticker_month'] == current_data['c1']['ticker_month']]
    seasonal_clean_residuals = seasonal_residuals[np.isfinite(seasonal_residuals)]
    clean_residuals = residuals[np.isfinite(residuals)]

    contract_seasonality_ind = (seasonal_clean_residuals.mean()-clean_residuals.mean())/clean_residuals.std()

    yield1_quantile_list = stats.get_number_from_quantile(y=yield1, quantile_list=[10, 90])
    yield2_quantile_list = stats.get_number_from_quantile(y=yield2, quantile_list=[10, 90])

    if (yield1_quantile_list[0]==yield1_quantile_list[1]) or (yield2_quantile_list[0]==yield2_quantile_list[1]):
        return {'success': False}

    noise_ratio = (yield1_quantile_list[1]-yield1_quantile_list[0])/(yield2_quantile_list[1]-yield2_quantile_list[0])

    daily_noise_recent = stats.get_stdev(x=butterfly_1_change.values[-20:], clean_num_obs=15)
    daily_noise_past = stats.get_stdev(x=butterfly_1_change.values, clean_num_obs=max(100, round(3*len(butterfly_1_change.values)/4)))

    recent_vol_ratio = daily_noise_recent/daily_noise_past

    alpha1 = yield_regress_output['alpha']

    residuals_last5_years = residuals[last5_years_indx]
    residuals_last2_months = residuals[last2_months_indx]

    residual_current = yield1_current-alpha1-second_spread_weight_1*yield2_current

    z3 = (residual_current-residuals_last5_years.mean())/residuals.std()
    z4 = (residual_current-residuals_last2_months.mean())/residuals.std()

    yield_change = (alpha1+second_spread_weight_1*yield2_current-yield1_current)/(1+second_spread_weight_1)

    new_yield1 = yield1_current + yield_change
    new_yield2 = yield2_current - yield_change

    price_change1 = 100*((price_2*(new_yield1+100)/100)-price_1)/(200+new_yield1)
    price_change2 = 100*((price_3*(new_yield2+100)/100)-price_2)/(200+new_yield2)

    theo_pnl = contract_multiplier*(2*price_change1-2*second_spread_weight_1*price_change2)

    aligned_data['residuals'] = residuals
    aligned_data['residuals3'] = residuals3
    aligned_output['aligned_data'] = aligned_data

    grouped = aligned_data.groupby(aligned_data['c1']['cont_indx'])
    aligned_data['shifted_residuals'] = grouped['residuals'].shift(-5)
    aligned_data['residual_change'] = aligned_data['shifted_residuals']-aligned_data['residuals']

    aligned_data['shifted_residuals3'] = grouped['residuals3'].shift(-5)
    aligned_data['residual_change3'] = aligned_data['shifted_residuals3'] - aligned_data['residuals3']

    mean_reversion = stats.get_regression_results({'x':aligned_data['residuals'].values,
                                                         'y':aligned_data['residual_change'].values,
                                                          'clean_num_obs': max(100, round(2*len(yield1.values)/3))})

    mean_reversion3 = stats.get_regression_results({'x': aligned_data['residuals3'].values,
                                                   'y': aligned_data['residual_change3'].values,
                                                   'clean_num_obs': max(100, round(2 * len(yield1.values) / 3))})



    theo_spread_move_output = su.calc_theo_spread_move_from_ratio_normalization(ratio_time_series=price_ratio.values[-40:],
                                                  starting_quantile=qf,
                                                  num_price=linear_interp_price2_current,
                                                  den_price=current_data['c2']['close_price'],
                                                  favorable_quantile_move_list=[5, 10, 15, 20, 25])

    theo_pnl_list = [x*contract_multiplier*2  for x in theo_spread_move_output['theo_spread_move_list']]

    theo_butterfly_move_output3 = su.calc_theo_weighted_butterfly_move(price_time_series=butterfly_price_w3[-40:],
                                                                      starting_quantile=qf3,
                                                                      weighted_butterfly_price=butterfly_price_wc3,
                                                                      favorable_quantile_move_list=[20])

    theo_butterfly_move_output4 = su.calc_theo_weighted_butterfly_move(price_time_series=butterfly_price_w4[-40:],
                                                                       starting_quantile=qf4,
                                                                       weighted_butterfly_price=butterfly_price_wc4,
                                                                       favorable_quantile_move_list=[20])

    theo_butterfly_move_output5 = su.calc_theo_weighted_butterfly_move(price_time_series=butterfly_price_w5[-40:],
                                                                       starting_quantile=qf5,
                                                                       weighted_butterfly_price=butterfly_price_wc5,
                                                                       favorable_quantile_move_list=[20])

    theo_butterfly_move_output6 = su.calc_theo_weighted_butterfly_move(price_time_series=butterfly_price_w6[-40:],
                                                                       starting_quantile=qf6,
                                                                       weighted_butterfly_price=butterfly_price_wc6,
                                                                       favorable_quantile_move_list=[20])

    if qf3>=50:
        rr3 = -contract_multiplier*theo_butterfly_move_output3['theo_butterfly_move_list'][0]/upside3
    else:
        rr3 = contract_multiplier*theo_butterfly_move_output3['theo_butterfly_move_list'][0]/abs(downside3)

    if qf4>=50:
        rr4 = -contract_multiplier*theo_butterfly_move_output4['theo_butterfly_move_list'][0]/upside4
    else:
        rr4 = contract_multiplier*theo_butterfly_move_output4['theo_butterfly_move_list'][0]/abs(downside4)

    if qf5>=50:
        rr5 = -contract_multiplier*theo_butterfly_move_output5['theo_butterfly_move_list'][0]/upside5
    else:
        rr5 = contract_multiplier*theo_butterfly_move_output5['theo_butterfly_move_list'][0]/abs(downside5)

    if qf6>=50:
        rr6 = -contract_multiplier*theo_butterfly_move_output6['theo_butterfly_move_list'][0]/upside6
    else:
        rr6 = contract_multiplier*theo_butterfly_move_output6['theo_butterfly_move_list'][0]/abs(downside6)

    if np.isnan(mean_reversion['conf_int'][1, 0]):
        mean_reversion_signif = False
    else:
        mean_reversion_signif = (mean_reversion['conf_int'][1, :] < 0).all()

    if np.isnan(mean_reversion3['conf_int'][1, 0]):
        mean_reversion_signif3 = False
    else:
        mean_reversion_signif3 = (mean_reversion3['conf_int'][1, :] < 0).all()

    return {'success': True,'aligned_output': aligned_output,'current_data':current_data, 'q': q, 'qf': qf,
            'qf3': qf3, 'qf4': qf4, 'qf5': qf5, 'qf6': qf6,
            'theo_pnl_list': theo_pnl_list,'rr3': rr3, 'rr4': rr4, 'rr5': rr5, 'rr6': rr6,
            'ratio_target_list': theo_spread_move_output['ratio_target_list'],
            'weight1': weight1, 'weight2': weight2, 'weight3': weight3,
            'zscore1': zscore1, 'rsquared1': rsquared1, 'zscore2': zscore2, 'rsquared2': rsquared2, 'rsquared3': rsquared3,
            'rsquared4':rsquared4,'rsquared5':rsquared5,'rsquared6':rsquared6,
            'zscore3': z3, 'zscore4': z4,
            'zscore5': zscore1-regime_change_ind,
            'zscore6': zscore1-contract_seasonality_ind,
            'zscore7': zscore1-regime_change_ind-contract_seasonality_ind,
            'zscore8': -zscore12,
            'zscore9': zscore3,
            'zscore10': -zscore32,
            'zscore11': -zscore4,
            'zscore12': -zscore5,
            'zscore13': -zscore6,
            'adfp1':adfp1,'adfp2':adfp2,
            'theo_pnl': theo_pnl,
            'regime_change_ind' : regime_change_ind,'contract_seasonality_ind': contract_seasonality_ind,
            'second_spread_weight_1': second_spread_weight_1, 'second_spread_weight_2': second_spread_weight_2,
            'second_spread_weight_12': second_spread_weight_12, 'second_spread_weight_3': second_spread_weight_3, 'second_spread_weight_32': second_spread_weight_32,
            'second_spread_weight_4': second_spread_weight_4,'second_spread_weight_5': second_spread_weight_5,'second_spread_weight_6': second_spread_weight_6,
            'downside': downside, 'upside': upside,
            'downside12': downside12, 'upside12': upside12,
            'downside3': downside3, 'upside3': upside3,
            'downside32': downside32, 'upside32': upside32,
            'downside4': downside4, 'downside5': downside5, 'downside6': downside6,
            'upside4': upside4, 'upside5': upside5, 'upside6': upside6,
             'yield1': yield1, 'yield2': yield2, 'yield1_current': yield1_current, 'yield2_current': yield2_current,
            'bf_price': butterfly_price_current, 'short_price_limit': short_price_limit,'long_price_limit':long_price_limit,
            'noise_ratio': noise_ratio,
            'alpha1': alpha1, 'alpha2': yield_regress_output_last5_years['alpha'],
            'residual_std1': yield_regress_output['residualstd'], 'residual_std2': yield_regress_output_last5_years['residualstd'],
            'recent_vol_ratio': recent_vol_ratio, 'recent_5day_pnl': recent_5day_pnl,
            'price_1': price_1, 'price_2': price_2, 'price_3': price_3, 'last5_years_indx': last5_years_indx,
            'price_ratio': price_ratio,
            'mean_reversion_rsquared': mean_reversion['rsquared'],
            'mean_reversion_signif' : mean_reversion_signif,
            'mean_reversion_rsquared3': mean_reversion3['rsquared'],
            'mean_reversion_signif3' : mean_reversion_signif3}

def generate_futures_butterfly_sheet_4date(**kwargs):

    date_to = kwargs['date_to']

    output_dir = ts.create_strategy_output_dir(strategy_class='futures_butterfly', report_date=date_to)

    if os.path.isfile(output_dir + '/summary.pkl'):
        butterflies = pd.read_pickle(output_dir + '/summary.pkl')
        return {'butterflies': butterflies,'success': True}

    if 'volume_filter' not in kwargs.keys():
        kwargs['volume_filter'] = 100

    butterflies = get_futures_butterflies_4date(**kwargs)

    butterflies = butterflies[butterflies['trDte1'] >= 35]
    butterflies = butterflies.iloc[:5]

    butterflies.reset_index(drop=True,inplace=True)
    num_butterflies = len(butterflies)

    butterflies['Q'] = [np.nan]*num_butterflies
    butterflies['QF'] = [np.nan]*num_butterflies

    butterflies['QF3'] = [np.nan] * num_butterflies
    butterflies['QF4'] = [np.nan] * num_butterflies
    butterflies['QF5'] = [np.nan] * num_butterflies
    butterflies['QF6'] = [np.nan] * num_butterflies

    butterflies['rr3'] = [np.nan] * num_butterflies
    butterflies['rr4'] = [np.nan] * num_butterflies
    butterflies['rr5'] = [np.nan] * num_butterflies
    butterflies['rr6'] = [np.nan] * num_butterflies

    butterflies['z1'] = [np.nan]*num_butterflies
    butterflies['z2'] = [np.nan]*num_butterflies
    butterflies['z3'] = [np.nan]*num_butterflies
    butterflies['z4'] = [np.nan]*num_butterflies
    butterflies['z5'] = [np.nan]*num_butterflies
    butterflies['z6'] = [np.nan]*num_butterflies
    butterflies['z7'] = [np.nan]*num_butterflies
    butterflies['z8'] = [np.nan]*num_butterflies
    butterflies['z9'] = [np.nan]*num_butterflies
    butterflies['z10'] = [np.nan]*num_butterflies
    butterflies['z11'] = [np.nan]*num_butterflies
    butterflies['z12'] = [np.nan]*num_butterflies
    butterflies['z13'] = [np.nan]*num_butterflies

    butterflies['adfp1'] = [np.nan]*num_butterflies
    butterflies['adfp2'] = [np.nan]*num_butterflies

    butterflies['r1'] = [np.nan]*num_butterflies
    butterflies['r2'] = [np.nan]*num_butterflies
    butterflies['r3'] = [np.nan]*num_butterflies
    butterflies['r4'] = [np.nan]*num_butterflies
    butterflies['r5'] = [np.nan]*num_butterflies
    butterflies['r6'] = [np.nan]*num_butterflies

    butterflies['RC'] = [np.nan]*num_butterflies
    butterflies['seasonality'] = [np.nan]*num_butterflies
    butterflies['yield1'] = [np.nan]*num_butterflies
    butterflies['yield2'] = [np.nan]*num_butterflies
    butterflies['bf_price'] = [np.nan]*num_butterflies
    butterflies['bf_sell_limit'] = [np.nan]*num_butterflies
    butterflies['bf_buy_limit'] = [np.nan]*num_butterflies
    butterflies['noise_ratio'] = [np.nan]*num_butterflies
    butterflies['alpha1'] = [np.nan]*num_butterflies
    butterflies['alpha2'] = [np.nan]*num_butterflies

    butterflies['residual_std1'] = [np.nan]*num_butterflies
    butterflies['residual_std2'] = [np.nan]*num_butterflies

    butterflies['second_spread_weight_1'] = [np.nan]*num_butterflies
    butterflies['second_spread_weight_2'] = [np.nan]*num_butterflies
    butterflies['second_spread_weight_12'] = [np.nan]*num_butterflies
    butterflies['second_spread_weight_3'] = [np.nan]*num_butterflies
    butterflies['second_spread_weight_32'] = [np.nan]*num_butterflies
    butterflies['second_spread_weight_4'] = [np.nan]*num_butterflies
    butterflies['second_spread_weight_5'] = [np.nan]*num_butterflies
    butterflies['second_spread_weight_6'] = [np.nan]*num_butterflies

    butterflies['weight1'] = [np.nan]*num_butterflies
    butterflies['weight2'] = [np.nan]*num_butterflies
    butterflies['weight3'] = [np.nan]*num_butterflies
    butterflies['downside'] = [np.nan]*num_butterflies
    butterflies['upside'] = [np.nan]*num_butterflies

    butterflies['downside12'] = [np.nan]*num_butterflies
    butterflies['upside12'] = [np.nan]*num_butterflies

    butterflies['downside3'] = [np.nan]*num_butterflies
    butterflies['upside3'] = [np.nan]*num_butterflies

    butterflies['downside32'] = [np.nan]*num_butterflies
    butterflies['upside32'] = [np.nan]*num_butterflies

    butterflies['downside4'] = [np.nan]*num_butterflies
    butterflies['upside4'] = [np.nan]*num_butterflies

    butterflies['downside5'] = [np.nan]*num_butterflies
    butterflies['upside5'] = [np.nan]*num_butterflies

    butterflies['downside6'] = [np.nan]*num_butterflies
    butterflies['upside6'] = [np.nan]*num_butterflies

    butterflies['recent_5day_pnl'] = [np.nan]*num_butterflies
    butterflies['recent_vol_ratio'] = [np.nan]*num_butterflies
    butterflies['theo_pnl'] = [np.nan]*num_butterflies

    butterflies['theo_pnl5'] = [np.nan]*num_butterflies
    butterflies['theo_pnl10'] = [np.nan]*num_butterflies
    butterflies['theo_pnl15'] = [np.nan]*num_butterflies
    butterflies['theo_pnl20'] = [np.nan]*num_butterflies
    butterflies['theo_pnl25'] = [np.nan]*num_butterflies

    butterflies['ratio_target5'] = [np.nan]*num_butterflies
    butterflies['ratio_target10'] = [np.nan]*num_butterflies
    butterflies['ratio_target15'] = [np.nan]*num_butterflies
    butterflies['ratio_target20'] = [np.nan]*num_butterflies
    butterflies['ratio_target25'] = [np.nan]*num_butterflies

    butterflies['price1'] = [np.nan]*num_butterflies
    butterflies['price2'] = [np.nan]*num_butterflies
    butterflies['price3'] = [np.nan]*num_butterflies

    butterflies['mean_reversion_rsquared'] = [np.nan]*num_butterflies
    butterflies['mean_reversion_signif'] = [np.nan]*num_butterflies

    butterflies['mean_reversion_rsquared3'] = [np.nan]*num_butterflies
    butterflies['mean_reversion_signif3'] = [np.nan]*num_butterflies

    futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in cmi.futures_butterfly_strategy_tickerhead_list}

    date5_years_ago = cu.doubledate_shift(date_to,5*365)
    datetime5_years_ago = cu.convert_doubledate_2datetime(date5_years_ago)

    for i in range(num_butterflies):
        bf_signals_output = fs.get_futures_butterfly_signals(ticker_list=[butterflies['ticker1'][i], butterflies['ticker2'][i], butterflies['ticker3'][i]],
                                          tr_dte_list=[butterflies['trDte1'][i], butterflies['trDte2'][i], butterflies['trDte3'][i]],
                                          aggregation_method=butterflies['agg'][i],
                                          contracts_back=butterflies['cBack'][i],
                                          date_to=date_to,
                                          futures_data_dictionary=futures_data_dictionary,
                                          contract_multiplier=butterflies['multiplier'][i],
                                          datetime5_years_ago=datetime5_years_ago)

        if not bf_signals_output['success']:
            continue

        butterflies.loc[i, 'Q'] = bf_signals_output['q']
        butterflies.loc[i, 'QF'] = bf_signals_output['qf']
        butterflies.loc[i, 'QF3'] = bf_signals_output['qf3']
        butterflies.loc[i, 'QF4'] = bf_signals_output['qf4']
        butterflies.loc[i, 'QF5'] = bf_signals_output['qf5']
        butterflies.loc[i, 'QF6'] = bf_signals_output['qf6']

        butterflies.loc[i, 'rr3'] = bf_signals_output['rr3']
        butterflies.loc[i, 'rr4'] = bf_signals_output['rr4']
        butterflies.loc[i, 'rr5'] = bf_signals_output['rr5']
        butterflies.loc[i, 'rr6'] = bf_signals_output['rr6']

        butterflies.loc[i, 'z1'] = bf_signals_output['zscore1']
        butterflies.loc[i, 'z2'] = bf_signals_output['zscore2']
        butterflies.loc[i, 'z3'] = bf_signals_output['zscore3']
        butterflies.loc[i, 'z4'] = bf_signals_output['zscore4']
        butterflies.loc[i, 'z5'] = bf_signals_output['zscore5']
        butterflies.loc[i, 'z6'] = bf_signals_output['zscore6']
        butterflies.loc[i, 'z7'] = bf_signals_output['zscore7']
        butterflies.loc[i, 'z8'] = bf_signals_output['zscore8']
        butterflies.loc[i, 'z9'] = bf_signals_output['zscore9']
        butterflies.loc[i, 'z10'] = bf_signals_output['zscore10']
        butterflies.loc[i, 'z11'] = bf_signals_output['zscore11']
        butterflies.loc[i, 'z12'] = bf_signals_output['zscore12']
        butterflies.loc[i, 'z13'] = bf_signals_output['zscore13']

        butterflies.loc[i, 'adfp1'] = bf_signals_output['adfp1']
        butterflies.loc[i, 'adfp2'] = bf_signals_output['adfp2']

        butterflies.loc[i, 'r1'] = bf_signals_output['rsquared1']
        butterflies.loc[i, 'r2'] = bf_signals_output['rsquared2']
        butterflies.loc[i, 'r3'] = bf_signals_output['rsquared3']
        butterflies.loc[i, 'r4'] = bf_signals_output['rsquared4']
        butterflies.loc[i, 'r5'] = bf_signals_output['rsquared5']
        butterflies.loc[i, 'r6'] = bf_signals_output['rsquared6']

        butterflies.loc[i, 'RC'] = bf_signals_output['regime_change_ind']
        butterflies.loc[i, 'seasonality'] = bf_signals_output['contract_seasonality_ind']
        butterflies.loc[i, 'yield1'] = bf_signals_output['yield1_current']
        butterflies.loc[i, 'yield2'] = bf_signals_output['yield2_current']
        butterflies.loc[i, 'bf_price'] = bf_signals_output['bf_price']
        butterflies.loc[i, 'bf_sell_limit'] = bf_signals_output['short_price_limit']
        butterflies.loc[i, 'bf_buy_limit'] = bf_signals_output['long_price_limit']

        butterflies.loc[i, 'noise_ratio'] = bf_signals_output['noise_ratio']
        butterflies.loc[i, 'alpha1'] = bf_signals_output['alpha1']
        butterflies.loc[i, 'alpha2'] = bf_signals_output['alpha2']

        butterflies.loc[i, 'residual_std1'] = bf_signals_output['residual_std1']
        butterflies.loc[i, 'residual_std1'] = bf_signals_output['residual_std2']

        butterflies.loc[i, 'second_spread_weight_1'] = bf_signals_output['second_spread_weight_1']
        butterflies.loc[i, 'second_spread_weight_2'] = bf_signals_output['second_spread_weight_2']
        butterflies.loc[i, 'second_spread_weight_12'] = bf_signals_output['second_spread_weight_12']
        butterflies.loc[i, 'second_spread_weight_3'] = bf_signals_output['second_spread_weight_3']
        butterflies.loc[i, 'second_spread_weight_32'] = bf_signals_output['second_spread_weight_32']
        butterflies.loc[i, 'second_spread_weight_4'] = bf_signals_output['second_spread_weight_4']
        butterflies.loc[i, 'second_spread_weight_5'] = bf_signals_output['second_spread_weight_5']
        butterflies.loc[i, 'second_spread_weight_6'] = bf_signals_output['second_spread_weight_6']

        butterflies.loc[i, 'weight1'] = bf_signals_output['weight1']
        butterflies.loc[i, 'weight2'] = bf_signals_output['weight2']
        butterflies.loc[i, 'weight3'] = bf_signals_output['weight3']

        butterflies.loc[i, 'downside'] = bf_signals_output['downside']
        butterflies.loc[i, 'upside'] = bf_signals_output['upside']

        butterflies.loc[i, 'downside12'] = bf_signals_output['downside12']
        butterflies.loc[i, 'upside12'] = bf_signals_output['upside12']

        butterflies.loc[i, 'downside3'] = bf_signals_output['downside3']
        butterflies.loc[i, 'upside3'] = bf_signals_output['upside3']

        butterflies.loc[i, 'downside32'] = bf_signals_output['downside32']
        butterflies.loc[i, 'upside32'] = bf_signals_output['upside32']

        butterflies.loc[i, 'downside4'] = bf_signals_output['downside4']
        butterflies.loc[i, 'upside4'] = bf_signals_output['upside4']

        butterflies.loc[i, 'downside5'] = bf_signals_output['downside5']
        butterflies.loc[i, 'upside5'] = bf_signals_output['upside5']

        butterflies.loc[i, 'downside6'] = bf_signals_output['downside6']
        butterflies.loc[i, 'upside6'] = bf_signals_output['upside6']

        butterflies.loc[i, 'recent_5day_pnl'] = bf_signals_output['recent_5day_pnl']
        butterflies.loc[i, 'recent_vol_ratio'] = bf_signals_output['recent_vol_ratio']
        butterflies.loc[i, 'theo_pnl'] = bf_signals_output['theo_pnl']

        butterflies.loc[i, 'theo_pnl5'] = bf_signals_output['theo_pnl_list'][0]
        butterflies.loc[i, 'theo_pnl10'] = bf_signals_output['theo_pnl_list'][1]
        butterflies.loc[i, 'theo_pnl15'] = bf_signals_output['theo_pnl_list'][2]
        butterflies.loc[i, 'theo_pnl20'] = bf_signals_output['theo_pnl_list'][3]
        butterflies.loc[i, 'theo_pnl25'] = bf_signals_output['theo_pnl_list'][4]

        butterflies.loc[i, 'ratio_target5'] = bf_signals_output['ratio_target_list'][0]
        butterflies.loc[i, 'ratio_target10'] = bf_signals_output['ratio_target_list'][1]
        butterflies.loc[i, 'ratio_target15'] = bf_signals_output['ratio_target_list'][2]
        butterflies.loc[i, 'ratio_target20'] = bf_signals_output['ratio_target_list'][3]
        butterflies.loc[i, 'ratio_target25'] = bf_signals_output['ratio_target_list'][4]

        butterflies.loc[i, 'price1'] = bf_signals_output['price_1']
        butterflies.loc[i, 'price2'] = bf_signals_output['price_2']
        butterflies.loc[i, 'price3'] = bf_signals_output['price_3']

        butterflies.loc[i, 'mean_reversion_rsquared'] = bf_signals_output['mean_reversion_rsquared']
        butterflies.loc[i, 'mean_reversion_signif'] = bf_signals_output['mean_reversion_signif']
        butterflies.loc[i, 'mean_reversion_rsquared3'] = bf_signals_output['mean_reversion_rsquared3']
        butterflies.loc[i, 'mean_reversion_signif3'] = bf_signals_output['mean_reversion_signif3']

    butterflies['z1'] = butterflies['z1'].astype(float).round(2)
    butterflies['z2'] = butterflies['z2'].astype(float).round(2)
    butterflies['z3'] = butterflies['z3'].astype(float).round(2)
    butterflies['z4'] = butterflies['z4'].astype(float).round(2)
    butterflies['z5'] = butterflies['z5'].astype(float).round(2)
    butterflies['z6'] = butterflies['z6'].astype(float).round(2)
    butterflies['z7'] = butterflies['z7'].astype(float).round(2)
    butterflies['r1'] = butterflies['r1'].astype(float).round(2)
    butterflies['r2'] = butterflies['r2'].astype(float).round(2)
    butterflies['RC'] = butterflies['RC'].astype(float).round(2)
    butterflies['seasonality'] = butterflies['seasonality'].astype(float).round(2)
    butterflies['second_spread_weight_1'] = butterflies['second_spread_weight_1'].astype(float).round(2)
    butterflies['second_spread_weight_2'] = butterflies['second_spread_weight_1'].astype(float).round(2)

    butterflies['yield1'] = butterflies['yield1'].astype(float).round(3)
    butterflies['yield2'] = butterflies['yield2'].astype(float).round(3)

    butterflies['noise_ratio'] = butterflies['noise_ratio'].astype(float).round(3)
    butterflies['alpha1'] = butterflies['alpha1'].astype(float).round(3)
    butterflies['alpha2'] = butterflies['alpha2'].astype(float).round(3)

    butterflies['residual_std1'] = butterflies['residual_std1'].astype(float).round(3)
    butterflies['residual_std2'] = butterflies['residual_std2'].astype(float).round(3)

    butterflies['downside'] = butterflies['downside'].astype(float).round(3)
    butterflies['upside'] = butterflies['upside'].astype(float).round(3)

    butterflies['recent_5day_pnl'] = butterflies['recent_5day_pnl'].astype(float).round(3)
    butterflies['recent_vol_ratio'] = butterflies['recent_vol_ratio'].astype(float).round(2)
    butterflies['theo_pnl'] = butterflies['theo_pnl'].astype(float).round(3)

    butterflies['price1'] = butterflies['price1'].astype(float).round(4)
    butterflies['price2'] = butterflies['price2'].astype(float).round(4)
    butterflies['price3'] = butterflies['price3'].astype(float).round(4)

    butterflies['mean_reversion_rsquared'] = butterflies['mean_reversion_rsquared'].astype(float).round(2)

    butterflies.to_pickle(output_dir + '/summary.pkl')

    return {'butterflies': butterflies,'success': True}

def get_futures_butterfly_filters(**kwargs):

    data_frame_input = kwargs['data_frame_input']
    filter_list = kwargs['filter_list']

    selection_indx = [False]*len(data_frame_input.index)

    if 'long1' in filter_list:
        selection_indx = selection_indx|((data_frame_input['z1'] <= -1.2) & (data_frame_input['QF'] <= 12)&(data_frame_input['second_spread_weight_1'] > 0.66) &
                                         (data_frame_input['second_spread_weight_1'] < 1.5))

    if 'short1' in filter_list:
        selection_indx = selection_indx|((data_frame_input['z1'] >= 0.6) & (data_frame_input['QF'] >= 85)&(data_frame_input['second_spread_weight_1'] > 0.66) &
                                         (data_frame_input['second_spread_weight_1'] < 1.5))

    if 'long2' in filter_list:
        selection_indx = selection_indx|((data_frame_input['z1'] <= -0.6) & (data_frame_input['QF'] <= 12))

    if 'short2' in filter_list:
        selection_indx = selection_indx|((data_frame_input['z1'] >= 0.6) & (data_frame_input['QF'] >= 85))

    if 'long3' in filter_list:
        selection_indx = selection_indx|((data_frame_input['Q'] <= 15) & (data_frame_input['QF'] <= 40) &
                                         (data_frame_input['recent_5day_pnl'] > 2*data_frame_input['downside']))

    if 'short3' in filter_list:
        selection_indx = selection_indx|((data_frame_input['Q'] >= 85) & (data_frame_input['QF'] >= 60) &
                                         (data_frame_input['recent_5day_pnl'] < 2*data_frame_input['upside']))

    if 'long4' in filter_list:
        selection_indx = selection_indx|((data_frame_input['z1'] <= -1.2) &
                                         (data_frame_input['QF'] <= 12) &
                                         (data_frame_input['second_spread_weight_1'] > 0.5) &
                                         (data_frame_input['second_spread_weight_1'] < 1.5))

    if 'short4' in filter_list:
        selection_indx = selection_indx|((data_frame_input['z1'] >= 0.6) &
                                         (data_frame_input['QF'] >= 85) &
                                         (data_frame_input['second_spread_weight_1'] > 0.5) &
                                         (data_frame_input['second_spread_weight_1'] < 1.5))

    if 'long5' in filter_list:
        selection_indx = selection_indx|((data_frame_input['z1'] <= -1.2) &
                                         (data_frame_input['QF'] <= 12) &
                                         (data_frame_input['tickerHead'] != 'S') &
                                         (data_frame_input['tickerHead'] != 'RB'))

    if 'short5' in filter_list:
        selection_indx = selection_indx|((data_frame_input['z1'] >= 0.6) &
                                         (data_frame_input['QF'] >= 85) &
                                           (data_frame_input['tickerHead'] != 'S') &
                                         (data_frame_input['tickerHead'] != 'RB'))

    if 'long6' in filter_list:
        selection_indx = selection_indx|((data_frame_input['z1'] <= -1.2) &
                                         (data_frame_input['QF'] <= 12) &
                                         (data_frame_input['mean_reversion_signif'] == True))

    if 'short6' in filter_list:
        selection_indx = selection_indx|((data_frame_input['z1'] >= 0.6) &
                                         (data_frame_input['QF'] >= 85) &
                                         (data_frame_input['mean_reversion_signif'] == True))

    if 'long7' in filter_list:
        selection_indx = selection_indx&(data_frame_input['second_spread_weight_1'] > 0.66) &(data_frame_input['second_spread_weight_1'] < 1.5)
        selection_indx = selection_indx|((data_frame_input['z1'] <= -1.2) & (data_frame_input['QF'] <= 12) & (data_frame_input['tickerHead'].isin(['CL', 'B'])) & (data_frame_input['z6'] <= -0.6))|\
                                        ((data_frame_input['z1'] <= -1.2) &(data_frame_input['QF'] <= 12) & (np.logical_not(data_frame_input['tickerHead'].isin(['CL', 'B']))))

    if 'short7' in filter_list:
        selection_indx = selection_indx & (data_frame_input['second_spread_weight_1'] > 0.66) & (data_frame_input['second_spread_weight_1'] < 1.5)
        selection_indx = selection_indx|((data_frame_input['z1'] >= 0.6) &(data_frame_input['QF'] >= 85)& (data_frame_input['tickerHead'].isin(['CL', 'B'])) & (data_frame_input['z6'] >= 0.3))|\
                                        ((data_frame_input['z1'] >= 0.6) &(data_frame_input['QF'] >= 85)& (np.logical_not(data_frame_input['tickerHead'].isin(['CL', 'B']))))

    if 'long8' in filter_list:
        selection_indx = selection_indx|((data_frame_input['z10'] <= -0.28) & (data_frame_input['QF3'] <= 13)&(data_frame_input['rr3'] >0.4)&(data_frame_input['second_spread_weight_32'] > 0.66) &
                                         (data_frame_input['second_spread_weight_32'] < 1.5)&(data_frame_input['trDte1']>=65))

    if 'short8' in filter_list:
        selection_indx = selection_indx|((data_frame_input['z10'] >= 0.38) & (data_frame_input['QF3'] >= 85)&(data_frame_input['rr3'] >0.4)&(data_frame_input['second_spread_weight_32'] > 0.66) &
                                         (data_frame_input['second_spread_weight_32'] < 1.5)&(data_frame_input['trDte1']>=65))

    if 'long9' in filter_list:
        selection_indx = selection_indx|((data_frame_input['z10'] <= -0.28) & (data_frame_input['QF3'] <= 13)&(data_frame_input['second_spread_weight_32'] > 0.66) &
                                         (data_frame_input['second_spread_weight_32'] < 1.5)&(data_frame_input['trDte1']>=65))

    if 'short9' in filter_list:
        selection_indx = selection_indx|((data_frame_input['z10'] >= 0.38) & (data_frame_input['QF3'] >= 85)&(data_frame_input['second_spread_weight_32'] > 0.66) &
                                         (data_frame_input['second_spread_weight_32'] < 1.5)&(data_frame_input['trDte1']>=65))

    if 'long10' in filter_list:
        selection_indx = selection_indx|((data_frame_input['QF3'] <= 13)&(data_frame_input['second_spread_weight_32'] > 0.66) &
                                         (data_frame_input['second_spread_weight_32'] < 1.5)&(data_frame_input['trDte1']>=65))

    if 'short10' in filter_list:
        selection_indx = selection_indx|((data_frame_input['QF3'] >= 85)&(data_frame_input['second_spread_weight_32'] > 0.66) &
                                         (data_frame_input['second_spread_weight_32'] < 1.5)&(data_frame_input['trDte1']>=65))

    if 'long11' in filter_list:
        selection_indx = selection_indx|((data_frame_input['z10'] <= -0.28) & (data_frame_input['QF3'] <= 13)&(data_frame_input['rr3'] >0.2)&(data_frame_input['second_spread_weight_32'] > 0.66) &
                                         (data_frame_input['second_spread_weight_32'] < 1.5)&(data_frame_input['trDte1']>=65))

    if 'short11' in filter_list:
        selection_indx = selection_indx|((data_frame_input['z10'] >= 0.38) & (data_frame_input['QF3'] >= 85)&(data_frame_input['rr3'] >0.2)&(data_frame_input['second_spread_weight_32'] > 0.66) &
                                         (data_frame_input['second_spread_weight_32'] < 1.5)&(data_frame_input['trDte1']>=65))

    if 'long12' in filter_list:
        selection_indx = selection_indx|((data_frame_input['z10'] <= -0.28) & (data_frame_input['QF3'] <= 13)&(data_frame_input['rr3'] >0.2)&(data_frame_input['second_spread_weight_32'] > 0.66) &
                                         (data_frame_input['second_spread_weight_32'] < 2)&(data_frame_input['trDte1']>=65))

    if 'short12' in filter_list:
        selection_indx = selection_indx|((data_frame_input['z10'] >= 0.38) & (data_frame_input['QF3'] >= 85)&(data_frame_input['rr3'] >0.2)&(data_frame_input['second_spread_weight_32'] > 0.66) &
                                         (data_frame_input['second_spread_weight_32'] < 2)&(data_frame_input['trDte1']>=65))

    if 'long13' in filter_list:
        selection_indx = selection_indx & (data_frame_input['second_spread_weight_32'] > 0.66) & (data_frame_input['second_spread_weight_32'] < 2)&(data_frame_input['trDte1']>=65)
        selection_indx = selection_indx|((data_frame_input['tickerHead'].isin(['CL', 'B'])) & (data_frame_input['z6'] <= -0.6) &(data_frame_input['z10'] <= -0.28) & (data_frame_input['QF3'] <= 13)&(data_frame_input['rr3'] >0.2))
        selection_indx = selection_indx|((np.logical_not(data_frame_input['tickerHead'].isin(['CL', 'B']))) &(data_frame_input['z10'] <= -0.28) & (data_frame_input['QF3'] <= 13)&(data_frame_input['rr3'] >0.2))


    if 'short13' in filter_list:
        selection_indx = selection_indx & (data_frame_input['second_spread_weight_32'] > 0.66) & (data_frame_input['second_spread_weight_32'] < 2)&(data_frame_input['trDte1']>=65)
        selection_indx = selection_indx | ((data_frame_input['tickerHead'].isin(['CL', 'B'])) & (data_frame_input['z6'] >= 0.3) & (data_frame_input['z10'] >= 0.38) & (data_frame_input['QF3'] >= 85) & (data_frame_input['rr3'] > 0.2))
        selection_indx = selection_indx | ((np.logical_not(data_frame_input['tickerHead'].isin(['CL', 'B']))) & (data_frame_input['z10']>= 0.38) & (data_frame_input['QF3'] >= 85) & (data_frame_input['rr3'] > 0.2))


    return {'selected_frame': data_frame_input[selection_indx],'selection_indx': selection_indx }
