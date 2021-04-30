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

    price_1 = current_data['c1']['close_price']
    price_2 = current_data['c2']['close_price']
    price_3 = current_data['c3']['close_price']

    price_1_aligned = aligned_data['c1']['close_price']
    price_2_aligned = aligned_data['c2']['close_price']
    price_3_aligned = aligned_data['c3']['close_price']

    yield1 = 100*(price_1_aligned-price_2_aligned)/price_2_aligned
    yield2 = 100*(price_2_aligned-price_3_aligned)/price_3_aligned

    yield1_current = 100*(price_1-price_2)/price_2
    yield2_current = 100*(price_2-price_3)/price_3

    butterfly_price_current = current_data['c1']['close_price']\
                            -2*current_data['c2']['close_price']\
                              +current_data['c3']['close_price']

    spread_1_aligned = price_1_aligned - price_2_aligned
    spread_2_aligned = price_2_aligned - price_3_aligned

    spread_1 = price_1-price_2
    spread_2 = price_2-price_3

    yield_regress_output = stats.get_regression_results({'x':yield2, 'y':yield1,'x_current': yield2_current, 'y_current': yield1_current,
                                                         'clean_num_obs': max(100, round(3*len(yield1.values)/4))})

    spread_regress_output = stats.get_regression_results(
        {'x': spread_1_aligned, 'y': spread_2_aligned, 'x_current': spread_1, 'y_current': spread_2,
         'clean_num_obs': max(100, round(3 * len(yield1.values) / 4))})

    zscore_yield = yield_regress_output['zscore']
    zscore_spread = spread_regress_output['zscore']

    second_spread_weight = 1/spread_regress_output['beta']

    butterfly_price_w = spread_1_aligned - second_spread_weight*spread_2_aligned
    butterfly_price_wc = spread_1 - second_spread_weight*spread_2

    qf = stats.get_quantile_from_number({'x': butterfly_price_wc, 'y': butterfly_price_w.values[-40:], 'clean_num_obs': 30})

    butterfly_5_change = aligned_data['c1']['change_5']\
                             - (1+second_spread_weight)*aligned_data['c2']['change_5']\
                             + second_spread_weight*aligned_data['c3']['change_5']

    butterfly_5_change_current = current_data['c1']['change_5']\
                             - (1+second_spread_weight)*current_data['c2']['change_5']\
                             + second_spread_weight*current_data['c3']['change_5']

    percentile_vector = stats.get_number_from_quantile(y=butterfly_5_change.values,
                                                       quantile_list=[1, 15, 85, 99],
                                                       clean_num_obs=max(100, round(3*len(butterfly_5_change.values)/4)))

    downside = contract_multiplier*(percentile_vector[0]+percentile_vector[1])/2
    upside = contract_multiplier*(percentile_vector[2]+percentile_vector[3])/2

    recent_5day_pnl = contract_multiplier*butterfly_5_change_current

    residuals = yield1-yield_regress_output['alpha']-yield_regress_output['beta']*yield2

    seasonal_residuals = residuals[aligned_data['c1']['ticker_month'] == current_data['c1']['ticker_month']]
    seasonal_clean_residuals = seasonal_residuals[np.isfinite(seasonal_residuals)]
    clean_residuals = residuals[np.isfinite(residuals)]

    contract_seasonality_ind = (seasonal_clean_residuals.mean()-clean_residuals.mean())/clean_residuals.std()

    yield1_quantile_list = stats.get_number_from_quantile(y=yield1, quantile_list=[10, 90])
    yield2_quantile_list = stats.get_number_from_quantile(y=yield2, quantile_list=[10, 90])

    if (yield1_quantile_list[0]==yield1_quantile_list[1]) or (yield2_quantile_list[0]==yield2_quantile_list[1]):
        return {'success': False}

    aligned_data['residuals'] = residuals
    aligned_output['aligned_data'] = aligned_data

    theo_butterfly_move_output = su.calc_theo_weighted_butterfly_move(price_time_series=butterfly_price_w[-40:],
                                                                      starting_quantile=qf,
                                                                      weighted_butterfly_price=butterfly_price_wc,
                                                                      favorable_quantile_move_list=[20])
    if qf>=50:
        rr = -contract_multiplier*theo_butterfly_move_output['theo_butterfly_move_list'][0]/upside
    else:
        rr = contract_multiplier*theo_butterfly_move_output['theo_butterfly_move_list'][0]/abs(downside)

    return {'success': True,'aligned_output': aligned_output,'current_data':current_data, 'qf': qf, 'rr': rr,
            'zscore1': -zscore_spread,
            'zscore2': zscore_yield-contract_seasonality_ind,
            'second_spread_weight': second_spread_weight,
            'downside': downside, 'upside': upside,
            'bf_price': butterfly_price_current,'bfw_price': butterfly_price_wc,
             'recent_5day_pnl': recent_5day_pnl}


def get_futures_spread_carry_signals(**kwargs):

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

    date1_years_ago = cu.doubledate_shift(date_to, 365)
    datetime1_years_ago = cu.convert_doubledate_2datetime(date1_years_ago)

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

    last1_years_indx = aligned_data['settle_date'] >= datetime1_years_ago
    data_last1_years = aligned_data[last1_years_indx]

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

    yield_history5 = [100 * (data_last5_years['c' + str(x + 1)]['close_price'] -
                            data_last5_years['c' + str(x + 2)]['close_price']) /
                     data_last5_years['c' + str(x + 2)]['close_price']
                     for x in range(len(ticker_list) - 1)]

    yield_history1 = [100 * (data_last1_years['c' + str(x + 1)]['close_price'] -
                             data_last1_years['c' + str(x + 2)]['close_price']) /
                      data_last1_years['c' + str(x + 2)]['close_price']
                      for x in range(len(ticker_list) - 1)]

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

    q5_list = [stats.get_quantile_from_number({'x': yield_current_list[x],
                                              'y': yield_history5[x].values,
                                              'clean_num_obs': max(100, round(3 * len(yield_history5[x].values) / 4))})
              for x in range(len(ticker_list) - 1)]

    q1_list = [stats.get_quantile_from_number({'x': yield_current_list[x],
                                               'y': yield_history1[x].values,
                                               'clean_num_obs': max(30, round(3 * len(yield_history1[x].values) / 4))})
               for x in range(len(ticker_list) - 1)]

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

    butterfly_noise_list_raw = [stats.get_stdev(x=butterfly_history[i].values[-20:]) for i in range(len(ticker_list)-2)]
    butterfly_noise_list = [np.nan if x == 0 else x for x in butterfly_noise_list_raw]
    #print(butterfly_noise_list)
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
                         'q': q_list,'q5': q5_list,'q1': q1_list,
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

