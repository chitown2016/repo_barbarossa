
import get_price.get_futures_price as gfp
import contract_utilities.contract_meta_info as cmi
import opportunity_constructs.utilities as opUtil
import machine_learning.calculate_outcomes as co
import signals.technical_indicators as ti
import shared.calendar_utilities as cu
import shared.statistics as stats
import statsmodels.api as sm
import fundamental_data.cot_data as cot
import numpy as np
import pandas as pd
import datetime as dt
pd.options.mode.chained_assignment = None


def get_fm_signals(**kwargs):

    ticker_head = kwargs['ticker_head']
    date_to = kwargs['date_to']

    #print(ticker_head)

    ticker_class = cmi.ticker_class[ticker_head]

    datetime_to = cu.convert_doubledate_2datetime(date_to)

    date5_years_ago = cu.doubledate_shift(date_to,5*365)
    datetime5_years_ago = cu.convert_doubledate_2datetime(date5_years_ago)

    data_out = gfp.get_futures_price_preloaded(ticker_head=ticker_head,settle_date_to=datetime_to)

    data4day = data_out[data_out['settle_date']==datetime_to]
    data4day = data4day[data4day['tr_dte']>=20]

    if len(data4day.index)<2:
        return {'ticker': '', 'comm_net': np.nan, 'spec_net': np.nan,
            'comm_cot_index_slow': np.nan, 'comm_cot_index_fast': np.nan,
            'comm_net_d': np.nan, 'spec_net_d': np.nan,
            'trend_direction': np.nan,'curve_slope': np.nan,
            'rsi_3': np.nan, 'rsi_7': np.nan, 'rsi_14': np.nan,'rsi_14_d': np.nan,
            'macd_histogram_q': np.nan,
            'macd_histogram_d': np.nan,
            'bollinger_dev': np.nan,
            'change1': np.nan,
            'change1_instant': np.nan,
            'change5': np.nan,
            'change10': np.nan,
            'change20': np.nan,
            'change1_dollar': np.nan,
            'change1_instant_dollar': np.nan,
            'change5_dollar': np.nan,
            'change10_dollar': np.nan,
            'change20_dollar': np.nan,
            'long_outcome1': np.nan,
            'short_outcome1': np.nan,
            'long_outcome2': np.nan,
            'short_outcome2': np.nan}

    data4day.sort_values('volume', ascending=False, inplace=True)
    data4day = data4day.iloc[:2]

    data4day.sort_values('tr_dte',ascending=True,inplace=True)

    ticker1 = data4day['ticker'].iloc[0]
    ticker2 = data4day['ticker'].iloc[1]

    tr_dte_list = [data4day['tr_dte'].iloc[0], data4day['tr_dte'].iloc[1]]

    amcb_output = opUtil.get_aggregation_method_contracts_back({'ticker_head': ticker_head, 'ticker_class': cmi.ticker_class[ticker_head]})
    aggregation_method = amcb_output['aggregation_method']
    contracts_back = amcb_output['contracts_back']

    futures_data_dictionary = {ticker_head: data_out}

    aligned_output = opUtil.get_aligned_futures_data(contract_list=[ticker1,ticker2],
                                                          tr_dte_list=tr_dte_list,
                                                          aggregation_method=aggregation_method,
                                                          contracts_back=contracts_back,
                                                          date_to=date_to,
                                                          futures_data_dictionary=futures_data_dictionary,
                                                          use_last_as_current=False)

    aligned_data = aligned_output['aligned_data']
    current_data = aligned_output['current_data']

    yield1_current = 100*(current_data['c1']['close_price']-current_data['c2']['close_price'])/current_data['c2']['close_price']
    yield1 = 100*(aligned_data['c1']['close_price']-aligned_data['c2']['close_price'])/aligned_data['c2']['close_price']

    last5_years_indx = aligned_data['settle_date']>=datetime5_years_ago

    yield1_last5_years = yield1[last5_years_indx]
    curve_slope = stats.get_quantile_from_number({'x':yield1_current,'y': yield1_last5_years})

    ticker_head_data = gfp.get_futures_price_preloaded(ticker_head=ticker_head)
    ticker_head_data = ticker_head_data[ticker_head_data['settle_date'] <= datetime_to]

    if ticker_class in ['Index', 'FX', 'Metal', 'Treasury', 'STIR']:

        merged_data = ticker_head_data[ticker_head_data['tr_dte'] >= 10]
        merged_data.sort_values(['settle_date', 'tr_dte'],ascending=True,inplace=True)
        merged_data.drop_duplicates(subset=['settle_date'], keep='first', inplace=True)

        merged_data['ma200'] = merged_data['close_price'].rolling(200).mean()
        merged_data['ma200_10'] = merged_data['ma200']-merged_data['ma200'].shift(10)
    else:
        data_out_front = ticker_head_data[ticker_head_data['tr_dte'] <= 60]
        data_out_front.drop_duplicates(subset=['settle_date'], keep='last', inplace=True)

        data_out_back = ticker_head_data[ticker_head_data['tr_dte'] > 60]
        data_out_back.drop_duplicates(subset=['settle_date'], keep='last', inplace=True)

        merged_data = pd.merge(data_out_front[['settle_date','tr_dte','close_price']],data_out_back[['tr_dte','close_price','settle_date','ticker','change_1']],how='inner',on='settle_date')
        merged_data['const_mat']=((merged_data['tr_dte_y']-60)*merged_data['close_price_x']+
                                  (60-merged_data['tr_dte_x'])*merged_data['close_price_y'])/\
                                 (merged_data['tr_dte_y']-merged_data['tr_dte_x'])

        merged_data['ma200'] = merged_data['const_mat'].rolling(200).mean()
        merged_data['ma200_10'] = merged_data['ma200']-merged_data['ma200'].shift(10)

    merged_data = merged_data[merged_data['settle_date']==datetime_to]

    if len(merged_data.index) == 0:
        trend_direction = np.nan
    elif merged_data['ma200_10'].iloc[0]>=0:
        trend_direction = 1
    else:
        trend_direction = -1

    ticker_data = gfp.get_futures_price_preloaded(ticker=ticker2)
    ticker_data.rename(columns={'close_price': 'close', 'high_price': 'high', 'low_price': 'low', 'open_price': 'open'},inplace=True)
    ticker_data.reset_index(drop=True, inplace=True)

    ticker_data = ti.get_atr(data_frame_input=ticker_data, period=14)
    forward_data = ticker_data[ticker_data['settle_date']>=datetime_to]

    forward_data = co.calculate_volatility_based_outcomes(data_frame_input=forward_data[:21], volatility_field='atr_14', calculate_first_row_onlyQ=True)

    ticker_data = ticker_data[ticker_data['settle_date']<=datetime_to]
    ticker_data = ti.rsi(data_frame_input=ticker_data, change_field='change_1', period=3)
    ticker_data = ti.rsi(data_frame_input=ticker_data, change_field='change_1', period=7)
    ticker_data = ti.rsi(data_frame_input=ticker_data, change_field='change_1', period=14)

    ticker_data = ti.get_macd(data_frame_input=ticker_data, period1=12, period2=26, period3=9)
    macd_histogram_q = stats.get_quantile_from_number({'x': ticker_data['macd_hist_12_26_9'].iloc[-1], 'y': ticker_data['macd_hist_12_26_9']})

    ticker_data = ti.get_bollinger_deviation(data_frame_input=ticker_data,period=20)

    cot_output = cot.get_cot_data(ticker_head=ticker_head, date_to=date_to)

    daily_noise = np.std(ticker_data['change_1'].iloc[-60:])

    if len(cot_output.index)>0:

        if ticker_class in ['FX','STIR','Index','Treasury']:

            cot_output['comm_long'] = cot_output['Asset Manager Longs']+cot_output['Dealer Longs']
            cot_output['comm_short'] = cot_output['Asset Manager Shorts']+cot_output['Dealer Shorts']
            cot_output['comm_net'] = cot_output['comm_long']-cot_output['comm_short']

            cot_output['spec_long'] = cot_output['Leveraged Funds Longs']
            cot_output['spec_short'] = cot_output['Leveraged Funds Shorts']
            cot_output['spec_net'] = cot_output['spec_long']-cot_output['spec_short']

        else:
            cot_output['comm_long'] = cot_output['Producer/Merchant/Processor/User Longs']+cot_output['Swap Dealer Longs']
            cot_output['comm_short'] = cot_output['Producer/Merchant/Processor/User Shorts']+cot_output['Swap Dealer Shorts']
            cot_output['comm_net'] = cot_output['comm_long']-cot_output['comm_short']
            cot_output['spec_long'] = cot_output['Money Manager Longs']+cot_output['Other Reportable Longs']
            cot_output['spec_short'] = cot_output['Money Manager Shorts']+cot_output['Other Reportable Shorts']
            cot_output['spec_net'] = cot_output['spec_long']-cot_output['spec_short']

        if (datetime_to-cot_output['settle_date'].iloc[-1]).days>=10:
            comm_net = np.nan
            spec_net = np.nan
        else:
            comm_net = cot_output['comm_net'].iloc[-1]
            spec_net = cot_output['spec_net'].iloc[-1]

        comm_net_d = np.sign(comm_net-cot_output['comm_net'].iloc[-3])
        spec_net_d = np.sign(spec_net-cot_output['spec_net'].iloc[-3])

        comm_net_min_slow = cot_output['comm_net'].iloc[-156:].min()
        comm_net_max_slow = cot_output['comm_net'].iloc[-156:].max()

        comm_cot_index_slow = 100*(comm_net-comm_net_min_slow)/(comm_net_max_slow-comm_net_min_slow)

        comm_net_min_fast = cot_output['comm_net'].iloc[-52:].min()
        comm_net_max_fast = cot_output['comm_net'].iloc[-52:].max()

        comm_cot_index_fast = 100*(comm_net-comm_net_min_fast)/(comm_net_max_fast-comm_net_min_fast)

    else:
        comm_net = np.nan
        spec_net = np.nan
        comm_cot_index_slow = np.nan
        comm_cot_index_fast = np.nan
        comm_net_d = np.nan
        spec_net_d = np.nan

    contract_multiplier = cmi.contract_multiplier[ticker_head]
    print(ticker_head)

    return {'ticker': ticker2, 'comm_net': comm_net, 'spec_net': spec_net,
            'comm_net_d': comm_net_d,
            'spec_net_d': spec_net_d,
            'comm_cot_index_slow': comm_cot_index_slow, 'comm_cot_index_fast': comm_cot_index_fast, 'trend_direction': trend_direction,'curve_slope': curve_slope,
            'rsi_3': ticker_data['rsi_3'].iloc[-1], 'rsi_7': ticker_data['rsi_7'].iloc[-1], 'rsi_14': ticker_data['rsi_14'].iloc[-1],
            'rsi_14_d': np.sign(ticker_data['rsi_14'].iloc[-1]-ticker_data['rsi_14'].iloc[-3]),
            'macd_histogram_q':macd_histogram_q,
            'macd_histogram_d': np.sign(ticker_data['macd_hist_12_26_9'].iloc[-1]-ticker_data['macd_hist_12_26_9'].iloc[-3]),
            'bollinger_dev': ticker_data['bollinger_dev_20'].iloc[-1],
            'change1': ticker_data['change1'].iloc[-1]/daily_noise,
            'change1_instant': ticker_data['change1_instant'].iloc[-1]/daily_noise,
            'change5': ticker_data['change5'].iloc[-1]/daily_noise,
            'change10': ticker_data['change10'].iloc[-1]/daily_noise,
            'change20': ticker_data['change20'].iloc[-1]/daily_noise,
            'change1_dollar': ticker_data['change1'].iloc[-1]*contract_multiplier,
            'change1_instant_dollar': ticker_data['change1_instant'].iloc[-1]*contract_multiplier,
            'change5_dollar': ticker_data['change5'].iloc[-1]*contract_multiplier,
            'change10_dollar': ticker_data['change10'].iloc[-1]*contract_multiplier,
            'change20_dollar': ticker_data['change20'].iloc[-1]*contract_multiplier,
            'long_outcome1': forward_data['long_outcome1'].iloc[0],
            'short_outcome1': forward_data['short_outcome1'].iloc[0],
            'long_outcome2': forward_data['long_outcome2'].iloc[0],
            'short_outcome2': forward_data['short_outcome2'].iloc[0]}

def get_cot_strategy_signals(**kwargs):

    ticker_head = kwargs['ticker_head']
    date_to = kwargs['date_to']
    target_noise = 5000

    ticker_class = cmi.ticker_class[ticker_head]
    datetime_to = cu.convert_doubledate_2datetime(date_to)

    data_out = gfp.get_futures_price_preloaded(ticker_head=ticker_head)
    data_out = data_out[data_out['settle_date'] <= datetime_to]

    data_out_front = data_out[data_out['tr_dte'] <= 60]
    data_out_front.drop_duplicates(subset=['settle_date'], keep='last', inplace=True)

    data_out_back = data_out[data_out['tr_dte'] > 60]
    data_out_back.drop_duplicates(subset=['settle_date'], keep='last', inplace=True)

    merged_data = pd.merge(data_out_front[['settle_date','tr_dte','close_price']],data_out_back[['tr_dte','close_price','settle_date','ticker','change_1']],how='inner',on='settle_date')
    merged_data['const_mat']=((merged_data['tr_dte_y']-60)*merged_data['close_price_x']+
                              (60-merged_data['tr_dte_x'])*merged_data['close_price_y'])/\
                             (merged_data['tr_dte_y']-merged_data['tr_dte_x'])

    merged_data['const_mat_change_1'] = merged_data['const_mat']-merged_data['const_mat'].shift(1)
    merged_data['const_mat_change_10'] = merged_data['const_mat']-merged_data['const_mat'].shift(10)
    merged_data['const_mat_change_20'] = merged_data['const_mat']-merged_data['const_mat'].shift(20)

    merged_data['const_mat_change_1_std'] = pd.rolling_std(merged_data['const_mat_change_1'], window=150, min_periods=75)

    merged_data['const_mat_ma5'] = pd.rolling_mean(merged_data['const_mat'], window=5, min_periods=4)
    merged_data['const_mat_ma10'] = pd.rolling_mean(merged_data['const_mat'], window=10, min_periods=8)
    merged_data['const_mat_ma20'] = pd.rolling_mean(merged_data['const_mat'], window=20, min_periods=16)
    merged_data['const_mat_ma40'] = pd.rolling_mean(merged_data['const_mat'], window=40, min_periods=32)
    merged_data['const_mat_ma80'] = pd.rolling_mean(merged_data['const_mat'], window=80, min_periods=64)

    merged_data['const_mat_ma5spread'] = (merged_data['const_mat']-merged_data['const_mat_ma5'])/merged_data['const_mat_change_1_std']
    merged_data['const_mat_ma10spread'] = (merged_data['const_mat']-merged_data['const_mat_ma10'])/merged_data['const_mat_change_1_std']
    merged_data['const_mat_ma20spread'] = (merged_data['const_mat']-merged_data['const_mat_ma20'])/merged_data['const_mat_change_1_std']
    merged_data['const_mat_ma40spread'] = (merged_data['const_mat']-merged_data['const_mat_ma40'])/merged_data['const_mat_change_1_std']
    merged_data['const_mat_ma80spread'] = (merged_data['const_mat']-merged_data['const_mat_ma80'])/merged_data['const_mat_change_1_std']

    merged_data['const_mat_change1norm'] = (merged_data['const_mat'].shift(-1)-merged_data['const_mat'])/merged_data['const_mat_change_1_std']
    merged_data['const_mat_change2norm'] = (merged_data['const_mat'].shift(-2)-merged_data['const_mat'])/merged_data['const_mat_change_1_std']
    merged_data['const_mat_change5norm'] = (merged_data['const_mat'].shift(-5)-merged_data['const_mat'])/merged_data['const_mat_change_1_std']
    merged_data['const_mat_change10norm'] = (merged_data['const_mat'].shift(-10)-merged_data['const_mat'])/merged_data['const_mat_change_1_std']
    merged_data['const_mat_change20norm'] = (merged_data['const_mat'].shift(-20)-merged_data['const_mat'])/merged_data['const_mat_change_1_std']

    merged_data['const_mat_change_10_std'] = pd.rolling_std(merged_data['const_mat_change_10'], window=150, min_periods=75)
    merged_data['const_mat_change_20_std'] = pd.rolling_std(merged_data['const_mat_change_20'], window=150, min_periods=75)

    merged_data['const_mat_change_10_norm'] = merged_data['const_mat_change_10']/merged_data['const_mat_change_10_std']
    merged_data['const_mat_change_20_norm'] = merged_data['const_mat_change_20']/merged_data['const_mat_change_20_std']

    merged_data['const_mat_min5'] = pd.rolling_min(merged_data['const_mat'], window=5, min_periods=4)
    merged_data['const_mat_min10'] = pd.rolling_min(merged_data['const_mat'], window=10, min_periods=8)
    merged_data['const_mat_min20'] = pd.rolling_min(merged_data['const_mat'], window=20, min_periods=16)

    merged_data['const_mat_max5'] = pd.rolling_max(merged_data['const_mat'], window=5, min_periods=4)
    merged_data['const_mat_max10'] = pd.rolling_max(merged_data['const_mat'], window=10, min_periods=8)
    merged_data['const_mat_max20'] = pd.rolling_max(merged_data['const_mat'], window=20, min_periods=16)

    cot_output = cot.get_cot_data(ticker_head=ticker_head,date_to=date_to)

    if ticker_class in ['FX','STIR','Index','Treasury']:

        cot_output['comm_net'] = cot_output['Dealer Longs']-cot_output['Dealer Shorts']
        cot_output['comm_long'] = cot_output['Dealer Longs']
        cot_output['comm_short'] = cot_output['Dealer Shorts']
        cot_output['spec_net'] = cot_output['Asset Manager Longs']-cot_output['Asset Manager Shorts']+cot_output['Leveraged Funds Longs']-cot_output['Leveraged Funds Shorts']
        cot_output['spec_long'] = cot_output['Asset Manager Longs']+cot_output['Leveraged Funds Longs']
        cot_output['spec_short'] = cot_output['Asset Manager Shorts']+cot_output['Leveraged Funds Shorts']
    else:
        cot_output['comm_net'] = cot_output['Producer/Merchant/Processor/User Longs']-cot_output['Producer/Merchant/Processor/User Shorts']
        cot_output['comm_long'] = cot_output['Producer/Merchant/Processor/User Longs']
        cot_output['comm_short'] = cot_output['Producer/Merchant/Processor/User Shorts']
        cot_output['spec_net'] = cot_output['Money Manager Longs']-cot_output['Money Manager Shorts']
        cot_output['spec_long'] = cot_output['Money Manager Longs']
        cot_output['spec_short'] = cot_output['Money Manager Shorts']


    cot_output['spec_long_per_mean'] = pd.rolling_mean(cot_output['spec_long_per'],window=150,min_periods=75)
    cot_output['spec_short_per_mean'] = pd.rolling_mean(cot_output['spec_short_per'],window=150,min_periods=75)




    cot_output['comm_net_change_1'] = cot_output['comm_net']-cot_output['comm_net'].shift(1)
    cot_output['comm_net_change_2'] = cot_output['comm_net']-cot_output['comm_net'].shift(2)
    cot_output['comm_net_change_4'] = cot_output['comm_net']-cot_output['comm_net'].shift(4)

    cot_output['spec_net_change_1'] = cot_output['spec_net']-cot_output['spec_net'].shift(1)
    cot_output['spec_net_change_2'] = cot_output['spec_net']-cot_output['spec_net'].shift(2)
    cot_output['spec_net_change_4'] = cot_output['spec_net']-cot_output['spec_net'].shift(4)

    cot_output['comm_long_change_1'] = cot_output['comm_long']-cot_output['comm_long'].shift(1)
    cot_output['comm_short_change_1'] = cot_output['comm_short']-cot_output['comm_short'].shift(1)

    cot_output['comm_net_change_1_std'] = pd.rolling_std(cot_output['comm_net_change_1'], window=150, min_periods=75)
    cot_output['comm_net_change_2_std'] = pd.rolling_std(cot_output['comm_net_change_2'], window=150, min_periods=75)
    cot_output['comm_net_change_4_std'] = pd.rolling_std(cot_output['comm_net_change_4'], window=150, min_periods=75)

    cot_output['spec_net_change_1_std'] = pd.rolling_std(cot_output['spec_net_change_1'], window=150, min_periods=75)
    cot_output['spec_net_change_2_std'] = pd.rolling_std(cot_output['spec_net_change_2'], window=150, min_periods=75)
    cot_output['spec_net_change_4_std'] = pd.rolling_std(cot_output['spec_net_change_4'], window=150, min_periods=75)

    cot_output['comm_long_change_1_std'] = pd.rolling_std(cot_output['comm_long_change_1'], window=150, min_periods=75)
    cot_output['comm_short_change_1_std'] = pd.rolling_std(cot_output['comm_short_change_1'], window=150, min_periods=75)

    cot_output['comm_net_change_1_norm'] = cot_output['comm_net_change_1']/cot_output['comm_net_change_1_std']
    cot_output['comm_net_change_2_norm'] = cot_output['comm_net_change_2']/cot_output['comm_net_change_2_std']
    cot_output['comm_net_change_4_norm'] = cot_output['comm_net_change_4']/cot_output['comm_net_change_4_std']

    cot_output['spec_net_change_1_norm'] = cot_output['spec_net_change_1']/cot_output['spec_net_change_1_std']
    cot_output['spec_net_change_2_norm'] = cot_output['spec_net_change_2']/cot_output['spec_net_change_2_std']
    cot_output['spec_net_change_4_norm'] = cot_output['spec_net_change_4']/cot_output['spec_net_change_4_std']

    cot_output['comm_long_change_1_norm'] = cot_output['comm_long_change_1']/cot_output['comm_long_change_1_std']
    cot_output['comm_short_change_1_norm'] = cot_output['comm_short_change_1']/cot_output['comm_short_change_1_std']

    cot_output['comm_net_mean'] = pd.rolling_mean(cot_output['comm_net'],window=150,min_periods=75)
    cot_output['spec_net_mean'] = pd.rolling_mean(cot_output['spec_net'],window=150,min_periods=75)

    cot_output['comm_net_std'] = pd.rolling_std(cot_output['comm_net'],window=150,min_periods=75)
    cot_output['spec_net_std'] = pd.rolling_std(cot_output['spec_net'],window=150,min_periods=75)

    cot_output['comm_z'] = (cot_output['comm_net']-cot_output['comm_net_mean'])/cot_output['comm_net_std']
    cot_output['spec_z'] = (cot_output['spec_net']-cot_output['spec_net_mean'])/cot_output['spec_net_std']

    cot_output['settle_date'] = cot_output.index
    cot_output['settle_date'] = [x+dt.timedelta(days=3) for x in cot_output['settle_date']]

    combined_data = pd.merge(merged_data,cot_output,how='left',on='settle_date')

    combined_data['comm_net_change_1_norm'] = combined_data['comm_net_change_1_norm'].fillna(method='pad')
    combined_data['comm_net_change_2_norm'] = combined_data['comm_net_change_2_norm'].fillna(method='pad')
    combined_data['comm_net_change_4_norm'] = combined_data['comm_net_change_4_norm'].fillna(method='pad')

    combined_data['spec_net_change_1_norm'] = combined_data['spec_net_change_1_norm'].fillna(method='pad')
    combined_data['spec_net_change_2_norm'] = combined_data['spec_net_change_2_norm'].fillna(method='pad')
    combined_data['spec_net_change_4_norm'] = combined_data['spec_net_change_4_norm'].fillna(method='pad')

    combined_data['comm_z'] = combined_data['comm_z'].fillna(method='pad')
    combined_data['spec_z'] = combined_data['spec_z'].fillna(method='pad')

    position = 0
    position_list = []
    entry_index_list = []
    exit_index_list = []
    entry_price_list = []
    exit_price_list = []

    for i in range(len(combined_data.index)):

        spec_change1_signal = combined_data['spec_net_change_1_norm'].iloc[i]
        spec_change4_signal = combined_data['spec_net_change_4_norm'].iloc[i]
        spec_z_signal = combined_data['spec_z'].iloc[i]
        price_i = combined_data['const_mat'].iloc[i]

        min_10 = combined_data['const_mat_min10'].iloc[i]
        max_10 = combined_data['const_mat_max10'].iloc[i]
        min_20 = combined_data['const_mat_min20'].iloc[i]
        max_20 = combined_data['const_mat_max20'].iloc[i]

        const_mat_change_10_norm = combined_data['const_mat_change_10_norm'].iloc[i]
        const_mat_change_20_norm = combined_data['const_mat_change_20_norm'].iloc[i]

        if i == len(combined_data.index)-1:
            if position != 0:
                exit_index_list.append(i)
                exit_price_list.append(price_i)
            break

        price_i1 = combined_data['const_mat'].iloc[i+1]

        if (position == 0) and (spec_z_signal>0.5) and (price_i==min_10):
            position = 1
            position_list.append(position)
            entry_index_list.append(i)
            entry_price_list.append(price_i1)
        elif (position == 0) and (spec_z_signal<-0.5) and (price_i==max_10):
            position = -1
            position_list.append(position)
            entry_index_list.append(i)
            entry_price_list.append(price_i1)
        elif (position == 1) and ((price_i==min_20)):
            position = 0
            exit_index_list.append(i)
            exit_price_list.append(price_i1)
        elif (position == -1) and ((price_i==max_20)):
            position = 0
            exit_index_list.append(i)
            exit_price_list.append(price_i1)

    return {'combined_data': combined_data}


def get_contract_summary_stats(**kwargs):
    ticker = kwargs['ticker']
    date_to = kwargs['date_to']
    data_out = gfp.get_futures_price_preloaded(ticker=ticker)
    datetime_to = cu.convert_doubledate_2datetime(date_to)
    data_out = data_out[data_out['settle_date'] <= datetime_to]

    data_out['close_price_daily_diff'] = data_out['close_price'] - data_out['close_price'].shift(1)
    daily_noise = np.std(data_out['close_price_daily_diff'].iloc[-60:])
    average_volume = np.mean(data_out['volume'].iloc[-20:])
    return {'daily_noise': daily_noise, 'average_volume':average_volume}


def get_arma_signals(**kwargs):

    ticker_head = kwargs['ticker_head']
    date_to = kwargs['date_to']

    date2_years_ago = cu.doubledate_shift(date_to, 2*365)

    panel_data = gfp.get_futures_price_preloaded(ticker_head=ticker_head,settle_date_from=date2_years_ago,settle_date_to=date_to)
    panel_data = panel_data[panel_data['tr_dte']>=40]
    panel_data.sort(['settle_date','tr_dte'],ascending=[True,True],inplace=True)
    rolling_data = panel_data.drop_duplicates(subset=['settle_date'], take_last=False)
    rolling_data['percent_change'] = 100*rolling_data['change_1']/rolling_data['close_price']
    rolling_data = rolling_data[rolling_data['percent_change'].notnull()]

    data_input = np.array(rolling_data['percent_change'])

    daily_noise = np.std(data_input)

    param1_list = []
    param2_list = []
    akaike_list = []
    forecast_list = []

    for i in range(0, 3):
        for j in range(0, 3):
            try:
                model_output = sm.tsa.ARMA(data_input, (i,j)).fit()
            except:
                continue

            param1_list.append(i)
            param2_list.append(j)
            akaike_list.append(model_output.aic)
            forecast_list.append(model_output.predict(len(data_input),len(data_input))[0])

    result_frame = pd.DataFrame.from_items([('param1', param1_list),
                             ('param2', param2_list),
                             ('akaike', akaike_list),
                             ('forecast', forecast_list)])

    result_frame.sort('akaike',ascending=True,inplace=True)

    param1 = result_frame['param1'].iloc[0]
    param2 = result_frame['param2'].iloc[0]

    if (param1 == 0)&(param2 == 0):
        forecast = np.nan
    else:
        forecast = result_frame['forecast'].iloc[0]

    return {'success': True, 'forecast':forecast,'normalized_forecast': forecast/daily_noise,
            'param1':param1,'param2':param2,
            'normalized_target': 100*(rolling_data['change1_instant'].iloc[-1]/rolling_data['close_price'].iloc[-1])/daily_noise}







