
import contract_utilities.contract_lists as cl
import shared.directory_names_aux as dna
import signals.intraday_futures_signals as ifs
import pandas as pd
import numpy as np
import ta.strategy as ts
import os.path

def get_spreads_4date(**kwargs):

    futures_dataframe = cl.generate_futures_list_dataframe(**kwargs)

    if 'volume_filter' in kwargs.keys():
        volume_filter = kwargs['volume_filter']
        futures_dataframe = futures_dataframe[futures_dataframe['volume'] > volume_filter]

    futures_dataframe.reset_index(drop=True, inplace=True)
    futures_dataframe['yearMonth'] = 12*futures_dataframe['ticker_year']+futures_dataframe['ticker_month']
    futures_dataframe['yearMonthMerge'] = futures_dataframe['yearMonth']
    futures_dataframe = futures_dataframe[['ticker','yearMonth','yearMonthMerge','ticker_head','volume']]

    spread_frame = pd.read_excel(dna.get_directory_name(ext='python_file') + '/opportunity_constructs/user_defined_spreads.xlsx')
    output_frame = pd.DataFrame()

    for i in range(len(spread_frame.index)):

        tickerhead1 = spread_frame['tickerHead1'].iloc[i]
        tickerhead2 = spread_frame['tickerHead2'].iloc[i]
        tickerhead3 = spread_frame['tickerHead3'].iloc[i]
        tickerhead4 = spread_frame['tickerHead4'].iloc[i]

        frame1 = futures_dataframe[futures_dataframe['ticker_head'] == tickerhead1]
        frame2 = futures_dataframe[futures_dataframe['ticker_head'] == tickerhead2]
        frame3 = futures_dataframe[futures_dataframe['ticker_head'] == tickerhead3]
        frame4 = futures_dataframe[futures_dataframe['ticker_head'] == tickerhead4]

        merged11 = pd.merge(frame1, frame2, how='inner', on='yearMonthMerge')

        frame2['yearMonthMerge'] = frame2['yearMonth']+1
        merged12 = pd.merge(frame1, frame2, how='inner', on='yearMonthMerge')

        frame2['yearMonthMerge'] = frame2['yearMonth']-1
        merged10 = pd.merge(frame1, frame2, how='inner', on='yearMonthMerge')

        if frame3.empty:

            output_frame2 = pd.concat([merged11, merged12, merged10])

            spread_i = pd.DataFrame()
            spread_i['contract1'] = output_frame2['ticker_x']
            spread_i['contract2'] = output_frame2['ticker_y']
            spread_i['contract3'] = None
            spread_i['ticker_head1'] = tickerhead1
            spread_i['ticker_head2'] = tickerhead2
            spread_i['ticker_head3'] = None
            spread_i['volume1'] = output_frame2['volume_x']
            spread_i['volume2'] = output_frame2['volume_y']
            spread_i['volume3'] = None

            output_frame = pd.concat([output_frame, spread_i])

        elif frame4.empty:

            frame3 = futures_dataframe[futures_dataframe['ticker_head'] == tickerhead3]
            merged111 = pd.merge(merged11, frame3, how='inner', on='yearMonthMerge')
            merged121 = pd.merge(merged12, frame3, how='inner', on='yearMonthMerge')
            merged101 = pd.merge(merged10, frame3, how='inner', on='yearMonthMerge')

            frame3['yearMonthMerge'] = frame3['yearMonth']+1
            merged112 = pd.merge(merged11, frame3, how='inner', on='yearMonthMerge')
            merged122 = pd.merge(merged12, frame3, how='inner', on='yearMonthMerge')

            frame3['yearMonthMerge'] = frame3['yearMonth']-1
            merged110 = pd.merge(merged11, frame3, how='inner', on='yearMonthMerge')
            merged100 = pd.merge(merged10, frame3, how='inner', on='yearMonthMerge')

            output_frame3 = pd.concat([merged111,merged121,merged101,
                                       merged112,merged122,merged110,merged100],sort=True)

            spread_i = pd.DataFrame()
            spread_i['contract1'] = output_frame3['ticker_x']
            spread_i['contract2'] = output_frame3['ticker_y']
            spread_i['contract3'] = output_frame3['ticker']
            spread_i['ticker_head1'] = tickerhead1
            spread_i['ticker_head2'] = tickerhead2
            spread_i['ticker_head3'] = tickerhead3
            spread_i['volume1'] = output_frame3['volume_x']
            spread_i['volume2'] = output_frame3['volume_y']
            spread_i['volume3'] = output_frame3['volume']

            output_frame = pd.concat([output_frame,spread_i])

    output_frame['spread_description'] = output_frame.apply(lambda x: x['ticker_head1']+ '_' +x['ticker_head2'] if x['ticker_head3'] is None else x['ticker_head1']+ '_' +x['ticker_head2'] + '_' + x['ticker_head3'] , axis=1)
    output_frame['min_volume'] = output_frame.apply(lambda x: min(x['volume1'],x['volume2']) if x['ticker_head3'] is None else min(x['volume1'],x['volume2'],x['volume3']),axis=1)
    output_frame.sort_values(['spread_description','min_volume'],ascending=[True, False],inplace=True)
    output_frame.drop_duplicates('spread_description',inplace=True)
    output_frame.reset_index(drop=True,inplace=True)

    return output_frame


def generate_ifs_sheet_4date(**kwargs):

    date_to = kwargs['date_to']

    output_dir = ts.create_strategy_output_dir(strategy_class='ifs', report_date=date_to)

    if os.path.isfile(output_dir + '/summary.pkl'):
        intraday_spreads = pd.read_pickle(output_dir + '/summary.pkl')
        return {'intraday_spreads': intraday_spreads,'success': True}

    if 'volume_filter' not in kwargs.keys():
        kwargs['volume_filter'] = 10000   #2000

    intraday_spreads = get_spreads_4date(**kwargs)

    num_spreads = len(intraday_spreads.index)
    signals_output = [ifs.get_intraday_spread_signals(ticker_list=[intraday_spreads.iloc[x]['contract1'],
                                                   intraday_spreads.iloc[x]['contract2'],
                                                   intraday_spreads.iloc[x]['contract3']],
                                    date_to=date_to) for x in range(num_spreads)]

    intraday_spreads['z'] = [x['z'] for x in signals_output]
    intraday_spreads['recentTrend'] = [x['recent_trend'] for x in signals_output]
    intraday_spreads['mean10'] = [x['intraday_mean10'] for x in signals_output]
    intraday_spreads['std10'] = [x['intraday_std10'] for x in signals_output]

    intraday_spreads['mean5'] = [x['intraday_mean5'] for x in signals_output]
    intraday_spreads['std5'] = [x['intraday_std5'] for x in signals_output]

    intraday_spreads['mean2'] = [x['intraday_mean2'] for x in signals_output]
    intraday_spreads['std2'] = [x['intraday_std2'] for x in signals_output]

    intraday_spreads['mean1'] = [x['intraday_mean1'] for x in signals_output]
    intraday_spreads['std1'] = [x['intraday_std1'] for x in signals_output]

    intraday_spreads['downside'] = [x['downside'] for x in signals_output]
    intraday_spreads['upside'] = [x['upside'] for x in signals_output]

    intraday_spreads['settle'] = [x['spread_settle'] for x in signals_output]
    intraday_spreads['spread_weight'] = [x['spread_weight'] for x in signals_output]
    intraday_spreads['portfolio_weight'] = [x['portfolio_weight'] for x in signals_output]

    intraday_spreads['maSpreadLow'] = [x['ma_spread_low'] for x in signals_output]
    intraday_spreads['maSpreadHigh'] = [x['ma_spread_high'] for x in signals_output]
    intraday_spreads['maSpreadLowL'] = [x['ma_spread_lowL'] for x in signals_output]
    intraday_spreads['maSpreadHighL'] = [x['ma_spread_highL'] for x in signals_output]
    intraday_spreads['is'] = [x['intraday_sharp'] for x in signals_output]

    intraday_spreads['z'] = intraday_spreads['z'].round(2)
    intraday_spreads['upside'] = intraday_spreads['upside'].round(3)
    intraday_spreads['downside'] = intraday_spreads['downside'].round(3)
    intraday_spreads['recentTrend'] = intraday_spreads['recentTrend'].round()

    intraday_spreads['ticker'] = intraday_spreads.apply(lambda x: x['contract1']+ '_' +x['contract2'] if x['contract3'] is None else x['contract1']+ '_' +x['contract2'] + '_' + x['contract3'] , axis=1)

    intraday_spreads.to_pickle(output_dir + '/summary.pkl')

    return {'intraday_spreads': intraday_spreads, 'success': True}






