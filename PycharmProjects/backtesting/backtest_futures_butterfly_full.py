from typing import Any

import contract_utilities.expiration as exp
import opportunity_constructs.futures_butterfly as fb
import signals.futures_filters as sf
import signals.futures_signals as fs
import ta.strategy as ts
import contract_utilities.contract_meta_info as cmi
import shared.calendar_utilities as cu
import contract_utilities.contract_meta_info as cmi
import get_price.get_futures_price as gfp
import os.path
import pandas as pd
import numpy as np
import random as rnd

risk = 10000
ticker_head_risk_dict = {x: 0 for x in cmi.futures_butterfly_strategy_tickerhead_list}
ticker_group_risk_dict = {'CL_HO_RB': 0,
                              'CL_B': 0,
                              'W_KW_C': 0,
                              'S_SM_BO': 0,
                              'LC_FC': 0}
max_holding_period = 28

def construct_daily_position(**kwargs):

    trade_date = kwargs['trade_date']
    rule_no = kwargs['rule_no']
    portfolio_id = kwargs['portfolio_id']
    scale_upQ = kwargs['scale_upQ']
    require_positive_pnlQ = kwargs['require_positive_pnlQ']

    if 'futures_data_dictionary' in kwargs.keys():
        futures_data_dictionary = kwargs['futures_data_dictionary']
    else:
        futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in
                                   cmi.futures_butterfly_strategy_tickerhead_list}


    report_date = exp.doubledate_shift_bus_days(double_date=trade_date,shift_in_days=1)
    date40 = exp.doubledate_shift_bus_days(double_date=trade_date, shift_in_days=-42)
    #print(report_date)
    report_date_folder = ts.create_strategy_output_dir(strategy_class='futures_butterfly',report_date=report_date)
    trade_date_folder = ts.create_strategy_output_dir(strategy_class='futures_butterfly', report_date=trade_date)



    global ticker_head_risk_dict
    global ticker_group_risk_dict
    ticker_head_risk_dict = {x: 0 for x in cmi.futures_butterfly_strategy_tickerhead_list}
    ticker_group_risk_dict = {'CL_HO_RB': 0,
                              'CL_B': 0,
                              'W_KW_C': 0,
                              'S_SM_BO': 0,
                              'LC_FC': 0}

    report_date_output = fb.generate_futures_butterfly_sheet_4date(date_to=report_date)
    trade_date_output = fb.generate_futures_butterfly_sheet_4date(date_to=trade_date)

    construct_output = {'success': False}

    if (report_date_output['success']==False)|(trade_date_output['success']==False):
        return construct_output

    report_date_butterflies = report_date_output['butterflies']
    trade_date_butterflies = trade_date_output['butterflies']

    if os.path.isfile(report_date_folder + '/portfolio' + '_' + str(portfolio_id) + '.pkl'):
        position_frame = pd.read_pickle(report_date_folder + '/portfolio' + '_' + str(portfolio_id) + '.pkl')

        for i in range(len(position_frame.index)):
            selected_row = trade_date_butterflies[(trade_date_butterflies['ticker1']==position_frame['ticker1'].iloc[i])&
            (trade_date_butterflies['ticker2'] == position_frame['ticker2'].iloc[i])&
            (trade_date_butterflies['ticker3'] == position_frame['ticker3'].iloc[i])]

            if len(selected_row)==0:
                bf_signals_output = fs.get_futures_butterfly_signals(
                    ticker_list=[position_frame['ticker1'].iloc[i], position_frame['ticker2'].iloc[i],
                                 position_frame['ticker3'].iloc[i]],
                    date_to=trade_date,
                    futures_data_dictionary=futures_data_dictionary)

                if bf_signals_output['success']:
                    new_QF= bf_signals_output['qf3']
                    new_z10 = bf_signals_output['zscore10']
                else:
                    new_QF = np.nan
                    new_z10 = np.nan
                tr_dte1 = np.nan
            else:
                new_QF = selected_row['QF3'].iloc[0]
                new_z10 = selected_row['z10'].iloc[0]
                tr_dte1 = selected_row['trDte1'].iloc[0]

            # Close the Trade?

            if (position_frame['qty1'].iloc[i]>0)&(new_QF-position_frame['QF_Initial'].iloc[i]>20)&(position_frame['QF'].iloc[i]-position_frame['QF_Initial'].iloc[i]>20):
                position_frame['close_Q'].iloc[i] = True
            elif (position_frame['qty1'].iloc[i]<0)&(new_QF-position_frame['QF_Initial'].iloc[i]<-20)&(position_frame['QF'].iloc[i]-position_frame['QF_Initial'].iloc[i]<-20):
                position_frame['close_Q'].iloc[i] = True

            position_frame['QF'].iloc[i] = new_QF
            position_frame['holding_period'].iloc[i] = (cu.convert_doubledate_2datetime(trade_date)-position_frame['entry_date'].iloc[i]).days

            calculate_risk(ticker_head=position_frame['ticker_head'].iloc[i], additional_risk=position_frame['risk'].iloc[i])

            # P&L Calculations:

            ticker1_0_output = gfp.get_futures_price_preloaded(ticker=position_frame['ticker1'].iloc[i],settle_date=report_date,futures_data_dictionary=futures_data_dictionary)
            ticker1_1_output = gfp.get_futures_price_preloaded(ticker=position_frame['ticker1'].iloc[i],settle_date=trade_date,futures_data_dictionary=futures_data_dictionary)

            ticker2_0_output = gfp.get_futures_price_preloaded(ticker=position_frame['ticker2'].iloc[i],settle_date=report_date,futures_data_dictionary=futures_data_dictionary)
            ticker2_1_output = gfp.get_futures_price_preloaded(ticker=position_frame['ticker2'].iloc[i],settle_date=trade_date,futures_data_dictionary=futures_data_dictionary)

            ticker3_0_output = gfp.get_futures_price_preloaded(ticker=position_frame['ticker3'].iloc[i],settle_date=report_date,futures_data_dictionary=futures_data_dictionary)
            ticker3_1_output = gfp.get_futures_price_preloaded(ticker=position_frame['ticker3'].iloc[i],settle_date=trade_date,futures_data_dictionary=futures_data_dictionary)

            #print(position_frame['ticker1'].iloc[i])
            #print(position_frame['ticker2'].iloc[i])
            #print(position_frame['ticker3'].iloc[i])
            #display(position_frame.iloc[i])

            position_frame['pnl'].iloc[i] = position_frame['multiplier'].iloc[i]*(position_frame['qty1'].iloc[i]*(ticker1_1_output['close_price'].iloc[0]-ticker1_0_output['close_price'].iloc[0])+
            position_frame['qty2'].iloc[i]*(ticker2_1_output['close_price'].iloc[0]-ticker2_0_output['close_price'].iloc[0])+
            position_frame['qty3'].iloc[i]*(ticker3_1_output['close_price'].iloc[0]-ticker3_0_output['close_price'].iloc[0]))

            if (position_frame['pnl_total'].iloc[i]<-position_frame['risk'].iloc[i]) and (max_holding_period-position_frame['holding_period'].iloc[i]>15) and scale_upQ and (not position_frame['scaled_upQ'].iloc[i]):
                if  ((position_frame['qty1'].iloc[i]<0)&(new_QF>=85)&(new_z10>=0.38)&(tr_dte1>=65)) or ((position_frame['qty1'].iloc[i]>0)&(new_QF<=13)&(new_z10<=-0.28)&(tr_dte1>=65)):
                    position_frame['qty1'].iloc[i] = round(1.66*position_frame['qty1'].iloc[i])
                    position_frame['qty2'].iloc[i] = round(1.66 * position_frame['qty2'].iloc[i])
                    position_frame['qty3'].iloc[i] = round(1.66 * position_frame['qty3'].iloc[i])
                    position_frame['scaled_upQ'].iloc[i] = True

            new_pnl_total = position_frame['pnl_total'].iloc[i] + position_frame['pnl'].iloc[i]

            # Don't close the position until it makes money
            if require_positive_pnlQ and (position_frame['pnl_total'].iloc[i]<0) and (new_pnl_total<0):
                position_frame['close_Q'].iloc[i] = False

            position_frame['pnl_total'].iloc[i] = new_pnl_total

            # Close every position at max holding period
            if position_frame['holding_period'].iloc[i]>max_holding_period:
                position_frame['close_Q'].iloc[i] = True

    else:
        position_frame = pd.DataFrame()

    report_date_long = sf.get_futures_butterfly_filters(data_frame_input=report_date_butterflies,
                                                          filter_list=['long' + str(rule_no)])
    report_date_short = sf.get_futures_butterfly_filters(data_frame_input=report_date_butterflies,
                                                           filter_list=['short' + str(rule_no)])

    trade_date_long = sf.get_futures_butterfly_filters(data_frame_input=trade_date_butterflies,
                                                        filter_list=['long' + str(rule_no)])
    trade_date_short = sf.get_futures_butterfly_filters(data_frame_input=trade_date_butterflies,
                                                         filter_list=['short' + str(rule_no)])

    report_frame_long = report_date_long['selected_frame']
    report_frame_short = report_date_short['selected_frame']

    trade_frame_long = trade_date_long['selected_frame']
    trade_frame_short = trade_date_short['selected_frame']

    merged_long = report_frame_long.merge(trade_frame_long, how='inner', on=['ticker1', 'ticker2', 'ticker3'])
    merged_short = report_frame_short.merge(trade_frame_short, how='inner', on=['ticker1', 'ticker2', 'ticker3'])

    if merged_long.empty:
        merged_frame = merged_short
    elif merged_short.empty:
        merged_frame = merged_long
    else:
        merged_frame = pd.concat([merged_long, merged_short])

    unique_ticker_head_list = merged_frame['tickerHead_x'].unique()
    rnd.shuffle(unique_ticker_head_list)


    for i in range(len(unique_ticker_head_list)):
        ticker_head_frame = merged_frame[merged_frame['tickerHead_x'] == unique_ticker_head_list[i]]
        ticker_head_frame.sort_values(by=['trDte3_x','QF3_y'], ascending=[True,rnd.choice([True, False])], inplace=True)

        # only take the trade if there's no data gap going forward
        row_indx = -1
        for j in range(len(ticker_head_frame.index)):
            data1 = gfp.get_futures_price_preloaded(ticker=ticker_head_frame['ticker1'].iloc[j], settle_date_from=trade_date,
                                                     settle_date_to=date40,futures_data_dictionary=futures_data_dictionary)
            if data1['close_price'].isnull().any():
                continue

            data2 = gfp.get_futures_price_preloaded(ticker=ticker_head_frame['ticker2'].iloc[j],
                                                    settle_date_from=trade_date,
                                                    settle_date_to=date40,futures_data_dictionary=futures_data_dictionary)
            if data2['close_price'].isnull().any():
                continue

            data3 = gfp.get_futures_price_preloaded(ticker=ticker_head_frame['ticker3'].iloc[j],
                                                    settle_date_from=trade_date,
                                                    settle_date_to=date40,futures_data_dictionary=futures_data_dictionary)
            if data3['close_price'].isnull().any():
                continue

            row_indx = j
            break

        if row_indx<0:
            continue


        slack = calculate_slack_per_tickerhead(ticker_head=unique_ticker_head_list[i],
                                               ticker_head_risk_dict=ticker_head_risk_dict,
                                               ticker_group_risk_dict=ticker_group_risk_dict)
        #print(slack)

        if slack<(risk/4):
            continue

        calculate_risk(ticker_head=unique_ticker_head_list[i],additional_risk=slack)

        if ticker_head_frame['QF3_y'].iloc[row_indx] < 50:
            first_qty = round(slack / abs(ticker_head_frame['downside32_y'].iloc[row_indx]))
        elif ticker_head_frame['QF3_y'].iloc[row_indx] > 50:
            first_qty = -round(slack / abs(ticker_head_frame['upside32_y'].iloc[row_indx]))

        second_qty = -round(first_qty * ticker_head_frame['second_spread_weight_32_y'].iloc[row_indx])



        position_frame = position_frame.append(pd.DataFrame({'ticker1': [ticker_head_frame['ticker1'].iloc[row_indx]],
                                                             'ticker2': [ticker_head_frame['ticker2'].iloc[row_indx]],
                                                             'ticker3': [ticker_head_frame['ticker3'].iloc[row_indx]],
                                                             'ticker_head':[ticker_head_frame['tickerHead_x'].iloc[row_indx]],
                                                             'multiplier': [ticker_head_frame['multiplier_x'].iloc[row_indx]],
                                                             'entry_date':[cu.convert_doubledate_2datetime(trade_date)],
                                                             'holding_period':[0],
                                                             'QF_Initial': [ticker_head_frame['QF3_y'].iloc[row_indx]],
                                                             'QF': [ticker_head_frame['QF3_y'].iloc[row_indx]],
                                                             'Z4_Initial': [ticker_head_frame['z10_y'].iloc[row_indx]],
                                                             'rr_initial': [ticker_head_frame['rr3_y'].iloc[row_indx]],
                                                             'second_spread_weight': [ticker_head_frame['second_spread_weight_32_y'].iloc[row_indx]],
                                                             'trDte1': [ticker_head_frame['trDte1_y'].iloc[row_indx]],
                                                             'qty1': [first_qty],
                                                             'qty2': [-first_qty + second_qty],
                                                             'qty3': [-second_qty],
                                                             'price1': [ticker_head_frame['price1_y'].iloc[row_indx]],
                                                             'price2': [ticker_head_frame['price2_y'].iloc[row_indx]],
                                                             'price3': [ticker_head_frame['price3_y'].iloc[row_indx]],
                                                             'pnl': [0],
                                                             'pnl_total': [0],
                                                             'risk': [slack],
                                                             'scaled_upQ': [False],
                                                             'close_Q':[False]
                                                             }))

    if position_frame['close_Q'].any():
        closed_positions = position_frame[position_frame['close_Q']]
        closed_positions['exit_date'] = cu.convert_doubledate_2datetime(trade_date)
        position_frame = position_frame[~position_frame['close_Q']]
    else:
        closed_positions = pd.DataFrame()


    position_frame.to_pickle(trade_date_folder + '/portfolio' + '_' + str(portfolio_id) + '.pkl')
    closed_positions.to_pickle(trade_date_folder + '/closed_positions' + '_' + str(portfolio_id) + '.pkl')

    return {'merged_long': merged_long, 'merged_short': merged_short,
            'position_frame': position_frame[['ticker1','ticker2','ticker3','ticker_head','entry_date',
                                              'QF_Initial','QF','qty1','qty2','qty3','pnl','pnl_total','risk',
                'scaled_upQ','close_Q','price1','price2','price3']],
            'closed_positions': closed_positions}

def calculate_risk(**kwargs):

    ticker_head = kwargs['ticker_head']
    additional_risk = kwargs['additional_risk']

    global ticker_head_risk_dict
    global ticker_group_risk_dict

    ticker_head_risk_dict[ticker_head] = ticker_head_risk_dict[ticker_head] + additional_risk

    if ticker_head in ['CL', 'HO', 'RB']:
        ticker_group_risk_dict['CL_HO_RB'] = ticker_group_risk_dict['CL_HO_RB'] + additional_risk

    if ticker_head in ['CL', 'B']:
        ticker_group_risk_dict['CL_B'] = ticker_group_risk_dict['CL_B'] + additional_risk

    if ticker_head in ['W', 'KW', 'C']:
        ticker_group_risk_dict['W_KW_C'] = ticker_group_risk_dict['W_KW_C'] + additional_risk

    if ticker_head in ['S', 'SM', 'BO']:
        ticker_group_risk_dict['S_SM_BO'] = ticker_group_risk_dict['S_SM_BO'] + additional_risk

def calculate_slack_per_tickerhead(**kwargs):

    ticker_head = kwargs['ticker_head']

    ticker_head_slack = risk-ticker_head_risk_dict[ticker_head]
    ticker_group_slack = 1.5*risk


    if ticker_head in ['CL', 'HO', 'RB']:
        ticker_group_slack = min(1.5*risk-ticker_group_risk_dict['CL_HO_RB'],ticker_group_slack)

    if ticker_head in ['CL', 'B']:
        ticker_group_slack = min(1.5*risk-ticker_group_risk_dict['CL_B'], ticker_group_slack)

    if ticker_head in ['W', 'KW', 'C']:
        ticker_group_slack = min(1.5 * risk - ticker_group_risk_dict['W_KW_C'], ticker_group_slack)

    if ticker_head in ['S', 'SM', 'BO']:
        ticker_group_slack = min(1.5 * risk - ticker_group_risk_dict['S_SM_BO'], ticker_group_slack)

    return min(ticker_head_slack,ticker_group_slack)




