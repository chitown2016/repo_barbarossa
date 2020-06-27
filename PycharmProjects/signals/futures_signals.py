__author__ = 'kocat_000'

import sys
sys.path.append(r'C:\Users\kocat_000\quantFinance\PycharmProjects')
import contract_utilities.expiration as exp
import opportunity_constructs.utilities as opUtil
import contract_utilities.contract_meta_info as cmi
import shared.directory_names_aux as dna
import get_price.get_futures_price as gfp
import shared.statistics as stats
import shared.calendar_utilities as cu
import signals.utils as su
import numpy as np
import pandas as pd
import os.path


def get_futures_butterfly_signals(**kwargs):

    ticker_list = kwargs['ticker_list']
    date_to = kwargs['date_to']

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

    linear_interp_price2 = (weight_11*aligned_data['c1']['close_price']+weight_13*aligned_data['c3']['close_price'])/2

    butterfly_price = aligned_data['c1']['close_price']-2*aligned_data['c2']['close_price']+aligned_data['c3']['close_price']

    price_ratio = linear_interp_price2/aligned_data['c2']['close_price']

    linear_interp_price2_current = (weight_11*price_1+weight_13*price_3)/2

    price_ratio_current = linear_interp_price2_current/price_2

    q = stats.get_quantile_from_number({'x': price_ratio_current, 'y': price_ratio.values, 'clean_num_obs': max(100, round(3*len(price_ratio.values)/4))})
    qf = stats.get_quantile_from_number({'x': price_ratio_current, 'y': price_ratio.values[-40:], 'clean_num_obs': 30})

    recent_quantile_list = [stats.get_quantile_from_number({'x': x, 'y': price_ratio.values[-40:], 'clean_num_obs': 30}) for x in price_ratio.values[-40:]]

    weight1 = weight_11
    weight2 = weight_12
    weight3 = weight_13

    last5_years_indx = aligned_data['settle_date']>=datetime5_years_ago
    last2_months_indx = aligned_data['settle_date']>=datetime2_months_ago
    data_last5_years = aligned_data[last5_years_indx]

    yield1 = 100*(aligned_data['c1']['close_price']-aligned_data['c2']['close_price'])/aligned_data['c2']['close_price']
    yield2 = 100*(aligned_data['c2']['close_price']-aligned_data['c3']['close_price'])/aligned_data['c3']['close_price']

    yield1_last5_years = yield1[last5_years_indx]
    yield2_last5_years = yield2[last5_years_indx]

    yield1_current = 100*(current_data['c1']['close_price']-current_data['c2']['close_price'])/current_data['c2']['close_price']
    yield2_current = 100*(current_data['c2']['close_price']-current_data['c3']['close_price'])/current_data['c3']['close_price']

    butterfly_price_current = current_data['c1']['close_price']\
                            -2*current_data['c2']['close_price']\
                              +current_data['c3']['close_price']

    #return {'yield1': yield1, 'yield2': yield2, 'yield1_current':yield1_current, 'yield2_current': yield2_current}

    yield_regress_output = stats.get_regression_results({'x':yield2, 'y':yield1,'x_current': yield2_current, 'y_current': yield1_current,
                                                         'clean_num_obs': max(100, round(3*len(yield1.values)/4))})


    yield_regress_output_last5_years = stats.get_regression_results({'x':yield2_last5_years, 'y':yield1_last5_years,
                                                                     'x_current': yield2_current, 'y_current': yield1_current,
                                                                     'clean_num_obs': max(100, round(3*len(yield1_last5_years.values)/4))})

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

    second_spread_weight_1 = yield_regress_output['beta']
    second_spread_weight_2 = yield_regress_output_last5_years['beta']

    butterfly_5_change = data_last5_years['c1']['change_5']\
                             - (1+second_spread_weight_1)*data_last5_years['c2']['change_5']\
                             + second_spread_weight_1*data_last5_years['c3']['change_5']

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
    recent_5day_pnl = contract_multiplier*butterfly_5_change_current

    residuals = yield1-yield_regress_output['alpha']-yield_regress_output['beta']*yield2

    regime_change_ind = (residuals[last5_years_indx].mean()-residuals.mean())/residuals.std()

    seasonal_residuals = residuals[aligned_data['c1']['ticker_month'] == current_data['c1']['ticker_month']]
    seasonal_clean_residuals = seasonal_residuals[np.isfinite(seasonal_residuals)]
    clean_residuals = residuals[np.isfinite(residuals)]

    contract_seasonality_ind = (seasonal_clean_residuals.mean()-clean_residuals.mean())/clean_residuals.std()

    yield1_quantile_list = stats.get_number_from_quantile(y=yield1, quantile_list=[10, 90])
    yield2_quantile_list = stats.get_number_from_quantile(y=yield2, quantile_list=[10, 90])

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
    aligned_output['aligned_data'] = aligned_data

    grouped = aligned_data.groupby(aligned_data['c1']['cont_indx'])
    aligned_data['shifted_residuals'] = grouped['residuals'].shift(-5)
    aligned_data['residual_change'] = aligned_data['shifted_residuals']-aligned_data['residuals']

    mean_reversion = stats.get_regression_results({'x':aligned_data['residuals'].values,
                                                         'y':aligned_data['residual_change'].values,
                                                          'clean_num_obs': max(100, round(3*len(yield1.values)/4))})

    theo_spread_move_output = su.calc_theo_spread_move_from_ratio_normalization(ratio_time_series=price_ratio.values[-40:],
                                                  starting_quantile=qf,
                                                  num_price=linear_interp_price2_current,
                                                  den_price=current_data['c2']['close_price'],
                                                  favorable_quantile_move_list=[5, 10, 15, 20, 25])

    theo_pnl_list = [x*contract_multiplier*2  for x in theo_spread_move_output['theo_spread_move_list']]

    return {'success': True,'aligned_output': aligned_output, 'q': q, 'qf': qf,
            'theo_pnl_list': theo_pnl_list,
            'ratio_target_list': theo_spread_move_output['ratio_target_list'],
            'weight1': weight1, 'weight2': weight2, 'weight3': weight3,
            'zscore1': zscore1, 'rsquared1': rsquared1, 'zscore2': zscore2, 'rsquared2': rsquared2,
            'zscore3': z3, 'zscore4': z4,
            'zscore5': zscore1-regime_change_ind,
            'zscore6': zscore1-contract_seasonality_ind,
            'zscore7': zscore1-regime_change_ind-contract_seasonality_ind,
            'theo_pnl': theo_pnl,
            'regime_change_ind' : regime_change_ind,'contract_seasonality_ind': contract_seasonality_ind,
            'second_spread_weight_1': second_spread_weight_1, 'second_spread_weight_2': second_spread_weight_2,
            'downside': downside, 'upside': upside,
             'yield1': yield1, 'yield2': yield2, 'yield1_current': yield1_current, 'yield2_current': yield2_current,
            'bf_price': butterfly_price_current, 'short_price_limit': short_price_limit,'long_price_limit':long_price_limit,
            'noise_ratio': noise_ratio,
            'alpha1': alpha1, 'alpha2': yield_regress_output_last5_years['alpha'],
            'residual_std1': yield_regress_output['residualstd'], 'residual_std2': yield_regress_output_last5_years['residualstd'],
            'recent_vol_ratio': recent_vol_ratio, 'recent_5day_pnl': recent_5day_pnl,
            'price_1': price_1, 'price_2': price_2, 'price_3': price_3, 'last5_years_indx': last5_years_indx,
            'price_ratio': price_ratio,
            'mean_reversion_rsquared': mean_reversion['rsquared'],
            'mean_reversion_signif' : (mean_reversion['conf_int'][1, :] < 0).all()}


def get_futures_spread_carry_signals(**kwargs):

    ticker_list = kwargs['ticker_list']
    date_to = kwargs['date_to']

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

    aligned_output = opUtil.get_aligned_futures_data(contract_list=ticker_list,
                                                          tr_dte_list=tr_dte_list,
                                                          aggregation_method=aggregation_method,
                                                          contracts_back=contracts_back,
                                                          date_to=date_to,
                                                          futures_data_dictionary=futures_data_dictionary,
                                                          use_last_as_current=use_last_as_current)

    aligned_data = aligned_output['aligned_data']
    current_data = aligned_output['current_data']

    last5_years_indx = aligned_data['settle_date']>=datetime5_years_ago
    data_last5_years = aligned_data[last5_years_indx]

    ticker1_list = [current_data['c' + str(x+1)]['ticker'] for x in range(len(ticker_list)-1)]
    ticker2_list = [current_data['c' + str(x+2)]['ticker'] for x in range(len(ticker_list)-1)]
    yield_current_list = [100*(current_data['c' + str(x+1)]['close_price']-
                           current_data['c' + str(x+2)]['close_price'])/
                           current_data['c' + str(x+2)]['close_price']
                            for x in range(len(ticker_list)-1)]

    butterfly_current_list = [100*(current_data['c' + str(x+1)]['close_price']-
                           2*current_data['c' + str(x+2)]['close_price']+
                             current_data['c' + str(x+3)]['close_price'])/
                           current_data['c' + str(x+2)]['close_price']
                            for x in range(len(ticker_list)-2)]

    price_current_list = [round(current_data['c' + str(x+1)]['close_price']-current_data['c' + str(x+2)]['close_price'],8)
                            for x in range(len(ticker_list)-1)]

    yield_history = [100*(aligned_data['c' + str(x+1)]['close_price']-
                           aligned_data['c' + str(x+2)]['close_price'])/
                           aligned_data['c' + str(x+2)]['close_price']
                            for x in range(len(ticker_list)-1)]

    butterfly_history = [100*(aligned_data['c' + str(x+1)]['close_price']-
                              2*aligned_data['c' + str(x+2)]['close_price']+
                              aligned_data['c' + str(x+3)]['close_price'])/
                         aligned_data['c' + str(x+2)]['close_price']
                            for x in range(len(ticker_list)-2)]

    change_5_history = [data_last5_years['c' + str(x+1)]['change_5']-
                           data_last5_years['c' + str(x+2)]['change_5']
                            for x in range(len(ticker_list)-1)]

    change5 = [contract_multiplier*(current_data['c' + str(x+1)]['change5']-
                           current_data['c' + str(x+2)]['change5'])
                            for x in range(len(ticker_list)-1)]

    change10 = [contract_multiplier*(current_data['c' + str(x+1)]['change10']-
                           current_data['c' + str(x+2)]['change10'])
                            for x in range(len(ticker_list)-1)]

    change20 = [contract_multiplier*(current_data['c' + str(x+1)]['change20']-
                           current_data['c' + str(x+2)]['change20'])
                            for x in range(len(ticker_list)-1)]

    front_tr_dte = [current_data['c' + str(x+1)]['tr_dte'] for x in range(len(ticker_list)-1)]

    q_list = [stats.get_quantile_from_number({'x': yield_current_list[x],
                                'y': yield_history[x].values,
                                'clean_num_obs': max(100, round(3*len(yield_history[x].values)/4))})
                                for x in range(len(ticker_list)-1)]

    butterfly_q_list = [stats.get_quantile_from_number({'x': butterfly_current_list[x],
                                'y': butterfly_history[x].values[-40:],
                                'clean_num_obs': round(3*len(butterfly_history[x].values[-40:])/4)})
                                for x in range(len(ticker_list)-2)]

    extreme_quantiles_list = [stats.get_number_from_quantile(y=x.values[:-40], quantile_list=[10,25,35,50,65,75,90]) for x in butterfly_history]
    butterfly_q10 = [x[0] for x in extreme_quantiles_list]
    butterfly_q25 = [x[1] for x in extreme_quantiles_list]
    butterfly_q35 = [x[2] for x in extreme_quantiles_list]
    butterfly_q50 = [x[3] for x in extreme_quantiles_list]
    butterfly_q65 = [x[4] for x in extreme_quantiles_list]
    butterfly_q75 = [x[5] for x in extreme_quantiles_list]
    butterfly_q90 = [x[6] for x in extreme_quantiles_list]

    butterfly_noise_list = [stats.get_stdev(x=butterfly_history[i].values[-20:]) for i in range(len(ticker_list)-2)]
    butterfly_mean_list = [stats.get_mean(x=butterfly_history[i].values[-10:]) for i in range(len(ticker_list)-2)]

    butterfly_z_list = [(butterfly_current_list[i] - butterfly_mean_list[i])/butterfly_noise_list[i] for i in range(len(ticker_list)-2)]

    percentile_vector = [stats.get_number_from_quantile(y=change_5_history[x].values,
                                                       quantile_list=[1, 15, 85, 99],
                                                       clean_num_obs=max(100, round(3*len(change_5_history[x].values)/4)))
                                                       for x in range(len(ticker_list)-1)]

    q1 = [x[0] for x in percentile_vector]
    q15 = [x[1] for x in percentile_vector]
    q85 = [x[2] for x in percentile_vector]
    q99 = [x[3] for x in percentile_vector]

    downside = [contract_multiplier*(q1[x]+q15[x])/2 for x in range(len(q1))]
    upside = [contract_multiplier*(q85[x]+q99[x])/2 for x in range(len(q1))]
    carry = [contract_multiplier*(price_current_list[x]-price_current_list[x+1]) for x in range(len(q_list)-1)]
    q_carry = [q_list[x]-q_list[x+1] for x in range(len(q_list)-1)]

    q_average = np.cumsum(q_list)/range(1, len(q_list)+1)
    q_series = pd.Series(q_list)
    q_min = q_series.cummin().values
    q_max = q_series.cummax().values
    q_carry_average = [q_average[x]-q_list[x+1] for x in range(len(q_list)-1)]
    q_carry_max = [q_max[x]-q_list[x+1] for x in range(len(q_list)-1)]
    q_carry_min = [q_min[x]-q_list[x+1] for x in range(len(q_list)-1)]


    reward_risk = [5*carry[x]/((front_tr_dte[x+1]-front_tr_dte[x])*abs(downside[x+1])) if carry[x]>0
      else 5*carry[x]/((front_tr_dte[x+1]-front_tr_dte[x])*upside[x+1]) for x in range(len(carry))]

    return pd.DataFrame.from_dict({'ticker1': ticker1_list,
                                    'ticker2': ticker2_list,
                                    'ticker1L': [''] + ticker1_list[:-1],
                                    'ticker2L': [''] + ticker2_list[:-1],
                         'ticker_head': cmi.get_contract_specs(ticker_list[0])['ticker_head'],
                         'front_tr_dte': front_tr_dte,
                         'front_tr_dteL': [np.NAN] + front_tr_dte[:-1],
                         'carry': [np.NAN]+carry,
                         'q_carry': [np.NAN]+q_carry,
                         'q_carry_average': [np.NAN]+q_carry_average,
                         'q_carry_max' : [np.NAN] + q_carry_max,
                         'q_carry_min' : [np.NAN] + q_carry_min,
                         'butterfly_q': [np.NAN]+butterfly_q_list,
                         'butterfly_z': [np.NAN]+butterfly_z_list,
                         'reward_risk': [np.NAN]+reward_risk,
                         'price': price_current_list,
                         'priceL': [np.NAN] + price_current_list[:-1],
                         'butterfly_q10': [np.NAN]+butterfly_q10,
                         'butterfly_q25': [np.NAN]+butterfly_q25,
                         'butterfly_q35': [np.NAN] + butterfly_q35,
                         'butterfly_q50': [np.NAN] + butterfly_q50,
                         'butterfly_q65': [np.NAN] + butterfly_q65,
                         'butterfly_q75': [np.NAN] + butterfly_q75,
                         'butterfly_q90': [np.NAN] + butterfly_q90,
                         'butterfly_mean': [np.NAN]+butterfly_mean_list,
                         'butterfly_noise': [np.NAN]+butterfly_noise_list,
                         'q': q_list,
                         'upside': upside,
                         'downside': downside,
                         'upsideL': [np.NAN] + upside[:-1],
                         'downsideL': [np.NAN] + downside[:-1],
                         'change5': change5,
                         'change10': change10,
                         'change20': change20})


def get_pca_seasonality_adjustments(**kwargs):

    ticker_head = kwargs['ticker_head']

    if 'file_date_to' in kwargs.keys():
        file_date_to = kwargs['file_date_to']
    else:
        file_date_to = 20160219

    if 'years_back' in kwargs.keys():
        years_back = kwargs['years_back']
    else:
        years_back = 10

    if 'date_to' in kwargs.keys():
        date_to = kwargs['date_to']
    else:
        date_to = file_date_to

    date5_years_ago = cu.doubledate_shift(date_to, 5*365)

    backtest_output_dir = dna.get_directory_name(ext='backtest_results')

    file_name = ticker_head + '_' + str(file_date_to) + '_' + str(years_back) + '_z'

    if os.path.isfile(backtest_output_dir + '/curve_pca/' + file_name + '.pkl'):
        backtest_results = pd.read_pickle(backtest_output_dir + '/curve_pca/' + file_name + '.pkl')
    else:
        return pd.DataFrame.from_items([('monthSpread',[1]*12+[6]*2),
                        ('ticker_month_front',list(range(1,13))+[6]+[12]),
                        ('z_seasonal_mean',[0]*14)])

    entire_report = pd.concat(backtest_results['report_results_list'])
    selected_report = entire_report[(entire_report['report_date'] <= date_to) & (entire_report['report_date'] >= date5_years_ago)]
    selected_report = selected_report[(selected_report['tr_dte_front'] > 80)&(selected_report['monthSpread'] < 12)]

    grouped = selected_report.groupby(['monthSpread','ticker_month_front'])

    seasonality_adjustment = pd.DataFrame()
    seasonality_adjustment['monthSpread'] = (grouped['monthSpread'].first()).values
    seasonality_adjustment['ticker_month_front'] = (grouped['ticker_month_front'].first()).values
    seasonality_adjustment['z_seasonal_mean'] = (grouped['z'].mean()).values

    return seasonality_adjustment

