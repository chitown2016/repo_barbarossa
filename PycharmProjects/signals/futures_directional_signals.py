
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

    print(ticker_head)

    ticker_class = cmi.ticker_class[ticker_head]

    datetime_to = cu.convert_doubledate_2datetime(date_to)

    date5_years_ago = cu.doubledate_shift(date_to,5*365)
    datetime5_years_ago = cu.convert_doubledate_2datetime(date5_years_ago)

    data_out = gfp.get_futures_price_preloaded(ticker_head=ticker_head,settle_date_to=datetime_to)

    data4day = data_out[data_out['settle_date']==datetime_to]
    data4day = data4day[data4day['tr_dte']>=20]

    if len(data4day.index)<2:
        return {'ticker': '',
                'tickerHead': ticker_head,
                'comm_indx_156': np.nan,
                'spec_indx_156': np.nan,
                'small_indx_156': np.nan,
                'willco': np.nan,
                'notes': '',
                'technicalNotes': '',
                's_oi': np.nan, 'trend_direction': np.nan, 'curve_slope': np.nan}



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

    ticker_data = ti.stochastic(data_frame_input=ticker_data, p1=7, p2=4, p3=10)

    technical_note_list = []

    if (ticker_data['D2'].iloc[-1]>ticker_data['D2'].iloc[-2]) and (ticker_data['D2'].iloc[-2]>ticker_data['D2'].iloc[-3]) and (ticker_data['D2'].iloc[-3]>ticker_data['D2'].iloc[-4]) and\
        (ticker_data['D1'].iloc[-1] < ticker_data['D1'].iloc[-2]) and (ticker_data['D1'].iloc[-2] < ticker_data['D1'].iloc[-3]) and (ticker_data['D1'].iloc[-3] < ticker_data['D1'].iloc[-4]):
        technical_note_list.append('bullish anti')
    elif (ticker_data['D2'].iloc[-1]<ticker_data['D2'].iloc[-2]) and (ticker_data['D2'].iloc[-2]<ticker_data['D2'].iloc[-3]) and (ticker_data['D2'].iloc[-3]<ticker_data['D2'].iloc[-4]) and\
        (ticker_data['D1'].iloc[-1] > ticker_data['D1'].iloc[-2]) and (ticker_data['D1'].iloc[-2] > ticker_data['D1'].iloc[-3]) and (ticker_data['D1'].iloc[-3] > ticker_data['D1'].iloc[-4]):
        technical_note_list.append('bearish anti')



    cot_output = cot.get_cot_signals(ticker_head=ticker_head, date_to=date_to)



    daily_noise = np.std(ticker_data['change_1'].iloc[-60:])
    note_list = []

    if len(cot_output.index)>0:

        comm_indx_156 = cot_output['comm_indx_156'].iloc[-1]
        spec_indx_156 = cot_output['spec_indx_156'].iloc[-1]
        small_indx_156 = cot_output['small_indx_156'].iloc[-1]
        willco = cot_output['willco'].iloc[-1]
        s_oi = cot_output['s_oi'].iloc[-1]


        if (trend_direction>0) and willco > 80:
            note_list.append('bullish retracement')
        elif (trend_direction<0) and willco < 20:
            note_list.append('bearish retracement')

        if comm_indx_156 > 80:
            note_list.append('bullish trend reversal')
        elif comm_indx_156 < 20:
            note_list.append('bearish trend reversal')

        if (spec_indx_156<20) and (small_indx_156<20):
            note_list.append('bullish extreme positioning')
        elif (spec_indx_156>80) and (small_indx_156>80):
            note_list.append('bearish extreme positioning')

        if s_oi<10:
            note_list.append('bullish lack of interest')
        elif s_oi>90:
            note_list.append('bearish too much interest')


    else:
        comm_indx_156 = np.nan
        spec_indx_156 = np.nan
        small_indx_156 = np.nan
        willco = np.nan
        s_oi = np.nan

    contract_multiplier = cmi.contract_multiplier[ticker_head]
   #print(ticker_head)

    return {'ticker': ticker2,
            'tickerHead': ticker_head,
            'comm_indx_156': comm_indx_156,
            'spec_indx_156': spec_indx_156,
            'small_indx_156': small_indx_156,
            'willco': willco,
            'notes': ','.join([str(x) for x in note_list]),
            'technicalNotes': ','.join([str(x) for x in technical_note_list]),
            's_oi': s_oi, 'trend_direction': trend_direction,'curve_slope': curve_slope}

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







