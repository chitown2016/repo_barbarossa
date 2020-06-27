
import contract_utilities.contract_lists as cl
import contract_utilities.contract_meta_info as cmi
import signals.overnight_calendar_spread_signals as ocss
import shared.calendar_utilities as cu
import get_price.get_futures_price as gfp
import pandas as pd
import os.path
import ta.strategy as ts


def get_overnight_spreads_4date(**kwargs):

    date_to = kwargs['date_to']
    datetime_to = cu.convert_doubledate_2datetime(date_to)
    kwargs['date_from'] = cu.doubledate_shift(date_to,6)
    futures_dataframe = cl.generate_futures_list_dataframe(**kwargs)

    futures_dataframe.sort_values(['ticker','settle_date'],ascending=[True,True],inplace=True)
    grouped = futures_dataframe.groupby(['ticker'])

    mean_frame = pd.DataFrame()
    mean_frame['averege_volume'] = grouped['volume'].mean()
    mean_frame['ticker'] = grouped['ticker'].last()
    mean_frame.index.names = ['ticker_index' if x is 'ticker' else x for x in mean_frame.index.names]

    mean_frame['averege_volume'] = mean_frame['averege_volume'].round()

    futures_dataframe = futures_dataframe[futures_dataframe['settle_date']==datetime_to]
    futures_dataframe = pd.merge(futures_dataframe, mean_frame, on='ticker', how='left')

    if 'volume_filter' in kwargs.keys():
        volume_filter = kwargs['volume_filter']
        futures_dataframe = futures_dataframe[futures_dataframe['averege_volume']>volume_filter]

    futures_dataframe.reset_index(drop=True, inplace=True)

    unique_ticker_heads = cmi.futures_butterfly_strategy_tickerhead_list
    tuples = []

    for ticker_head_i in unique_ticker_heads:
        ticker_head_data = futures_dataframe[futures_dataframe['ticker_head'] == ticker_head_i]

        ticker_head_data.sort_values(['ticker_year','ticker_month'], ascending=[True, True], inplace=True)

        if len(ticker_head_data.index) >= 2:
            tuples = tuples + [(ticker_head_data.index[i], ticker_head_data.index[i+1]) for i in range(len(ticker_head_data.index)-1)]

    return pd.DataFrame([(futures_dataframe['ticker'][indx[0]],
    futures_dataframe['ticker'][indx[1]],
    futures_dataframe['ticker_head'][indx[0]],
    futures_dataframe['ticker_class'][indx[0]],
    futures_dataframe['tr_dte'][indx[0]],
    futures_dataframe['tr_dte'][indx[1]],
    min([futures_dataframe['averege_volume'][indx[0]], futures_dataframe['averege_volume'][indx[1]]]),
    futures_dataframe['multiplier'][indx[0]],
    futures_dataframe['aggregation_method'][indx[0]],
    futures_dataframe['contracts_back'][indx[0]]) for indx in tuples],
                        columns=['ticker1','ticker2','tickerHead','tickerClass','trDte1','trDte2','min_avg_volume','multiplier','agg','cBack'])


def generate_overnight_spreads_sheet_4date(**kwargs):

    date_to = kwargs['date_to']

    output_dir = ts.create_strategy_output_dir(strategy_class='ocs', report_date=date_to)

    if os.path.isfile(output_dir + '/summary.pkl'):
        overnight_calendars = pd.read_pickle(output_dir + '/summary.pkl')
        return {'overnight_calendars': overnight_calendars,'success': True}

    if 'volume_filter' not in kwargs.keys():
        kwargs['volume_filter'] = 50

    overnight_calendars = get_overnight_spreads_4date(**kwargs)

    futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in overnight_calendars['tickerHead']}

    num_spreads = len(overnight_calendars.index)
    signals_output = [ocss.get_overnight_calendar_signals(ticker_list=[overnight_calendars.iloc[x]['ticker1'], overnight_calendars.iloc[x]['ticker2']],
                                                          futures_data_dictionary=futures_data_dictionary, date_to=date_to) for x in range(num_spreads)]

    overnight_calendars['ticker1L'] = [x['ticker1L'] for x in signals_output]
    overnight_calendars['ticker2L'] = [x['ticker2L'] for x in signals_output]
    overnight_calendars['reward_risk'] = [x['reward_risk'] for x in signals_output]
    overnight_calendars['qCarry'] = [x['q_carry'] for x in signals_output]
    overnight_calendars['butterflyQ'] = [x['butterfly_q'] for x in signals_output]
    overnight_calendars['butterflyZ'] = [x['butterfly_z'] for x in signals_output]
    overnight_calendars['spreadPrice'] = [x['spread_price'] for x in signals_output]
    overnight_calendars['butterfly_q10'] = [x['butterfly_q10'] for x in signals_output]
    overnight_calendars['butterfly_q25'] = [x['butterfly_q25'] for x in signals_output]
    overnight_calendars['butterfly_q35'] = [x['butterfly_q35'] for x in signals_output]
    overnight_calendars['butterfly_q50'] = [x['butterfly_q50'] for x in signals_output]
    overnight_calendars['butterfly_q65'] = [x['butterfly_q65'] for x in signals_output]
    overnight_calendars['butterfly_q75'] = [x['butterfly_q75'] for x in signals_output]
    overnight_calendars['butterfly_q90'] = [x['butterfly_q90'] for x in signals_output]
    overnight_calendars['butterflyMean'] = [x['butterfly_mean'] for x in signals_output]
    overnight_calendars['butterflyNoise'] = [x['butterfly_noise'] for x in signals_output]
    overnight_calendars['noise100'] = [x['noise_100'] for x in signals_output]
    overnight_calendars['dollarNoise100'] = [x['dollar_noise_100'] for x in signals_output]

    overnight_calendars['pnl1'] = [x['pnl1'] for x in signals_output]
    overnight_calendars['pnl1_instant'] = [x['pnl1_instant'] for x in signals_output]
    overnight_calendars['pnl2'] = [x['pnl2'] for x in signals_output]
    overnight_calendars['pnl5'] = [x['pnl5'] for x in signals_output]
    overnight_calendars['pnl10'] = [x['pnl10'] for x in signals_output]

    overnight_calendars.to_pickle(output_dir + '/summary.pkl')

    return {'overnight_calendars': overnight_calendars, 'success': True}



