__author__ = 'kocat_000'

import numpy as np
import pandas as pd
import shared.statistics as stats
import shared.utils as su
import signals.utils as sigut
import scipy.stats as scs
from collections import OrderedDict


def get_summary_stats(pnl_series):

    output = dict()

    output['total_pnl'] = float('NaN')
    output['mean_pnl'] = float('NaN')
    output['downside20'] = float('NaN')
    output['downside5'] = float('NaN')
    output['reward_risk'] = float('NaN')

    if len(pnl_series)==0:
        return output

    output['total_pnl'] = np.nansum(pnl_series)
    output['mean_pnl'] = np.nanmean(pnl_series)

    if len(pnl_series) >= 10:
        output['downside20'] = stats.get_number_from_quantile(y=pnl_series, quantile_list=[20])[0]

    if len(pnl_series) >= 40:
        output['downside5'] = stats.get_number_from_quantile(y=pnl_series, quantile_list=[5])[0]

    output['reward_risk'] = output['mean_pnl']/abs(output['downside20'] )

    return output


def get_indicator_rr_table(**kwargs):

    trade_data = kwargs['trade_data']
    indicator_name = kwargs['indicator_name']
    strategy_class = kwargs['strategy_class']
    long_pnl_field = kwargs['long_pnl_field']
    short_pnl_field = kwargs['short_pnl_field']

    if 'num_buckets' in kwargs.keys():
        num_buckets = kwargs['num_buckets']
    else:
        num_buckets = 9

    trade_data = trade_data[(np.isfinite(trade_data[long_pnl_field].values.astype(np.float64))) &
                            (np.isfinite(trade_data[short_pnl_field].values.astype(np.float64)))&
                            (np.isfinite(trade_data[indicator_name].values.astype(np.float64)))]

    signal_correlation = sigut.get_signal_correlation(strategy_class=strategy_class,signal_name=indicator_name)
    ascending_q = True if signal_correlation<0 else False

    bucket_data_output = su.bucket_data(data_input=trade_data, bucket_var=indicator_name, num_buckets=num_buckets,ascending_q=ascending_q)
    bucket_data_list = bucket_data_output['bucket_data_list']
    bucket_limits = bucket_data_output['bucket_limits']

    mean_pnl_list = []
    reward_risk_list = []

    for i in range(num_buckets):

        bucket_data = bucket_data_list[i]

        if i <= (num_buckets/2):
            signed_pnl = bucket_data[long_pnl_field]
        else:
            signed_pnl = bucket_data[short_pnl_field]

        stats_output = get_summary_stats(signed_pnl.values.astype(np.float64))
        mean_pnl_list.append(stats_output['mean_pnl'])
        reward_risk_list.append(stats_output['reward_risk'])

    if ascending_q:
        indicator_ulimit = np.append(bucket_limits, np.NAN)
    else:
        indicator_ulimit = np.append(np.NAN,bucket_limits)

    return pd.DataFrame.from_dict(OrderedDict([('indicator_ulimit', indicator_ulimit),
                 ('mean_pnl', mean_pnl_list),
                 ('reward_risk',reward_risk_list)]))


def get_indicator_rr_double_table(**kwargs):

    trade_data = kwargs['trade_data']
    indicator_list = kwargs['indicator_list']
    strategy_class = kwargs['strategy_class']
    long_pnl_field = kwargs['long_pnl_field']
    short_pnl_field = kwargs['short_pnl_field']

    #print(indicator_list)

    if 'num_buckets' in kwargs.keys():
        num_buckets = kwargs['num_buckets']
    else:
        num_buckets = 3

    trade_data = trade_data[(np.isfinite(trade_data[long_pnl_field].values.astype(np.float64))) &
                            (np.isfinite(trade_data[short_pnl_field].values.astype(np.float64)))&
                            (np.isfinite(trade_data[indicator_list[0]].values.astype(np.float64)))&
                            (np.isfinite(trade_data[indicator_list[1]].values.astype(np.float64)))]

    signal_correlation1 = sigut.get_signal_correlation(strategy_class=strategy_class,signal_name=indicator_list[0])
    signal_correlation2 = sigut.get_signal_correlation(strategy_class=strategy_class,signal_name=indicator_list[1])

    ascending_q1 = True if signal_correlation1<0 else False
    ascending_q2 = True if signal_correlation2<0 else False

    bucket_data_output = su.bucket_data(data_input=trade_data, bucket_var=indicator_list[0],
                                        num_buckets=num_buckets, ascending_q=ascending_q1)
    bucket_data_list1 = bucket_data_output['bucket_data_list']

    if ascending_q1:
        bucket_limits1_full = np.repeat(np.append(bucket_data_output['bucket_limits'], np.NAN), num_buckets)
    else:
        bucket_limits1_full = np.repeat(np.append(np.NAN,bucket_data_output['bucket_limits']), num_buckets)


    bucket_limits2_full = np.empty([1, 0])

    mean_pnl_list = []
    reward_risk_list = []

    for i in range(len(bucket_data_list1)):
        bucket_data_output = su.bucket_data(data_input=bucket_data_list1[i],
                                            bucket_var=indicator_list[1],
                                            num_buckets=num_buckets,
                                            ascending_q=ascending_q2)
        bucket_data_list2 = bucket_data_output['bucket_data_list']
        bucket_limits2 = bucket_data_output['bucket_limits']

        if ascending_q2:
            bucket_limits2_full = np.append(bucket_limits2_full,
                                            np.append(bucket_limits2, np.NAN))
        else:
            bucket_limits2_full = np.append(bucket_limits2_full,
                                            np.append(np.NAN,bucket_limits2))

        for j in range(len(bucket_data_list2)):

            if i <= (num_buckets/2):
                signed_pnl = bucket_data_list2[j][long_pnl_field]
            else:
                signed_pnl = bucket_data_list2[j][short_pnl_field]

            stats_output = get_summary_stats(signed_pnl.values)
            mean_pnl_list.append(stats_output['mean_pnl'])
            reward_risk_list.append(stats_output['reward_risk'])



    return pd.DataFrame.from_dict(OrderedDict([('indicator1_ulimit', bucket_limits1_full),
                 ('indicator2_ulimit', bucket_limits2_full),
                 ('mean_pnl', mean_pnl_list),
                 ('reward_risk',reward_risk_list)]))


def get_indicator_ranking(**kwargs):
    trade_data = kwargs['trade_data']
    indicator_list = kwargs['indicator_list']
    strategy_class = kwargs['strategy_class']
    long_pnl_field = kwargs['long_pnl_field']
    short_pnl_field = kwargs['short_pnl_field']

    trade_data = trade_data[(np.isfinite(trade_data[long_pnl_field])) & (np.isfinite(trade_data[short_pnl_field]))]

    long_rr_list = []
    short_rr_list = []

    for i in range(len(indicator_list)):

        if isinstance(indicator_list[i],list):
            q_rr_table = get_indicator_rr_double_table(trade_data=trade_data, indicator_list=[indicator_list[i][0],
                                                                                indicator_list[i][1]],
                                                       long_pnl_field=long_pnl_field,
                                                       short_pnl_field=short_pnl_field,
                                                       strategy_class=strategy_class)

        else:
            q_rr_table = get_indicator_rr_table(trade_data=trade_data, indicator_name=indicator_list[i],
                                                long_pnl_field=long_pnl_field,
                                                       short_pnl_field=short_pnl_field,
                                                strategy_class=strategy_class)
        long_rr_list.append(q_rr_table['reward_risk'].iloc[0])
        short_rr_list.append(q_rr_table['reward_risk'].iloc[-1])

    long_ranking = np.array(long_rr_list).argsort().argsort()
    short_ranking = np.array(short_rr_list).argsort().argsort()

    return pd.DataFrame.from_dict(OrderedDict([('indicator',indicator_list ),
                                        ('ranking', long_ranking+short_ranking)]))


def rank_indicators(**kwargs):
    trade_data = kwargs['trade_data']
    indicator_list_raw = kwargs['indicator_list']
    strategy_class = kwargs['strategy_class']
    long_pnl_field = kwargs['long_pnl_field']
    short_pnl_field = kwargs['short_pnl_field']

    if 'granular_ranking_type' in kwargs.keys():
        granular_ranking_type = kwargs['granular_ranking_type']

    selection_indx = [True]*len(trade_data.index)

    for indicator_i in indicator_list_raw:

        selection_indx = (selection_indx)&(np.isfinite(trade_data[indicator_i]))

    trade_data = trade_data[selection_indx]

    indicator_list = indicator_list_raw[:]

    for i in range(len(indicator_list_raw)):
        for j in range(len(indicator_list_raw)):
            if i == j:
                continue
            indicator_list.append([indicator_list_raw[i],
                               indicator_list_raw[j]])

    indicator_ranking_total = get_indicator_ranking(trade_data=trade_data, indicator_list=indicator_list,
                                                    long_pnl_field=long_pnl_field,
                                                         short_pnl_field=short_pnl_field,
                                                    strategy_class=strategy_class)
    indicator_ranking_total.sort_values(by='ranking',ascending=False,inplace=True)

    if granular_ranking_type == 'ticker_head':
        ticker_head_list = list(trade_data['tickerHead'].unique())
        ranking_list = []

        for i in range(len(ticker_head_list)):
            data_4tickerhead = trade_data[trade_data['tickerHead'] == ticker_head_list[i]]
            indicator_ranking_output = get_indicator_ranking(trade_data=data_4tickerhead,
                                                 indicator_list=indicator_list,
                                                         long_pnl_field=long_pnl_field,
                                                         short_pnl_field=short_pnl_field,
                                                         strategy_class=strategy_class)

            ranking_list.append(indicator_ranking_output['ranking'].values)

        granular_ranking_sums = pd.DataFrame(ranking_list).sum()

        granular_ranking_frame = pd.DataFrame.from_dict(OrderedDict([('indicator', indicator_list),
                                            ('ranking', granular_ranking_sums)]))

    elif granular_ranking_type == 'ticker_class':

        ticker_class_list = list(trade_data['tickerClass'].unique())
        ranking_list = []

        for i in range(len(ticker_class_list)):
            print(ticker_class_list[i])
            data_4tickerclass = trade_data[trade_data['tickerClass'] == ticker_class_list[i]]
            indicator_ranking_output = get_indicator_ranking(trade_data=data_4tickerclass,
                                                 indicator_list=indicator_list,
                                                         long_pnl_field=long_pnl_field,
                                                         short_pnl_field=short_pnl_field,
                                                         strategy_class=strategy_class)

            ranking_list.append(indicator_ranking_output['ranking'].values)

        granular_ranking_sums = pd.DataFrame(ranking_list).sum()

        granular_ranking_frame = pd.DataFrame.from_items([('indicator', indicator_list),
                             ('ranking', granular_ranking_sums)])

    return {'indicator_ranking_total': indicator_ranking_total,
            'indicator_ranking_granular_total': granular_ranking_frame.sort_values(by='ranking', ascending=False,inplace=False)}


def get_chisquare_independence_results(**kwargs):

    indicator_name = kwargs['indicator_name']
    target_name = kwargs['target_name']
    dataframe_input = kwargs['dataframe_input']

    dataframe_input = dataframe_input[dataframe_input[indicator_name].notnull()&dataframe_input[target_name].notnull()]

    dataframe_input['target_category'] = 0
    dataframe_input['indicator_category'] = 0

    quantile_values = stats.get_number_from_quantile(y=dataframe_input[target_name].values,quantile_list=[33, 66])
    dataframe_input.loc[dataframe_input[target_name]<quantile_values[0],'target_category'] = -1
    dataframe_input.loc[dataframe_input[target_name]>quantile_values[1],'target_category'] = 1

    quantile_values = stats.get_number_from_quantile(y=dataframe_input[indicator_name].values,quantile_list=[10, 90])
    dataframe_input.loc[dataframe_input[indicator_name]<quantile_values[0],'indicator_category'] = -1
    dataframe_input.loc[dataframe_input[indicator_name]>quantile_values[1],'indicator_category'] = 1

    dataframe_input = dataframe_input[dataframe_input['indicator_category']!=0]

    contingency_table = [[sum((dataframe_input['indicator_category'] == cat1) & (dataframe_input['target_category'] == cat2))
               for cat2 in [-1,0,1]]
              for cat1 in [-1,1]]

    chi_square_output = scs.chi2_contingency(contingency_table)

    chi_square_stat = chi_square_output[0]

    return {'chi_square': chi_square_stat, 'cramers_v': np.sqrt(chi_square_stat/len(dataframe_input.index))}










