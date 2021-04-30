__author__ = 'kocat_000'

import opportunity_constructs.spread_carry as sc
import contract_utilities.expiration as exp
import contract_utilities.contract_meta_info as cmi
import shared.calendar_utilities as cu
import signals.futures_filters as sf
import get_price.get_futures_price as gfp
import ta.strategy as ts
import os.path
import pandas as pd
import numpy as np

def backtest_spread_carry(**kwargs):

    date_list = kwargs['date_list']
    risk = 1000

    futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in sc.max_tr_dte_limits.keys()}

    ticker_head_list = list(sc.max_tr_dte_limits.keys())

    total_pnl_frame = pd.DataFrame({'report_date': date_list})
    total_pnl_frame['portfolio'] = 0
    backtest_output = []

    for i in range(len(ticker_head_list)):
        total_pnl_frame[ticker_head_list[i]] = 0

    for i in range(len(date_list)):
        spread_carry_output = sc.generate_spread_carry_sheet_4date(report_date=date_list[i],futures_data_dictionary=futures_data_dictionary)

        if spread_carry_output['success']:
            daily_sheet = spread_carry_output['spread_report']
        else:
            continue

        backtest_output.append(daily_sheet)

        daily_sheet['q_carry_abs'] = abs(daily_sheet['q_carry'])
        pnl_tickerhead_frame = pd.DataFrame({'ticker_head': ticker_head_list})
        pnl_tickerhead_frame['total_pnl'] = 0

        for j in range(len(ticker_head_list)):
            ticker_head_results = daily_sheet[daily_sheet['tickerHead'] == ticker_head_list[j]]

            if len(ticker_head_results.index)<=1:
                continue

            max_q_carry_abs = ticker_head_results['q_carry_abs'].max()

            if np.isnan(max_q_carry_abs):
                continue

            selected_spread = ticker_head_results.iloc[ticker_head_results['q_carry_abs'].idxmax()]

            if selected_spread['q_carry']>0:
                total_pnl_frame[ticker_head_list[j]][i] = selected_spread['change5']*risk/abs(selected_spread['downside'])
                pnl_tickerhead_frame['total_pnl'][j] = total_pnl_frame[ticker_head_list[j]][i]
            elif selected_spread['q_carry']<0:
                total_pnl_frame[ticker_head_list[j]][i] = -selected_spread['change5']*risk/abs(selected_spread['upside'])
                pnl_tickerhead_frame['total_pnl'][j] = total_pnl_frame[ticker_head_list[j]][i]

        total_pnl_frame['portfolio'][i] = pnl_tickerhead_frame['total_pnl'].sum()

    big_data = pd.concat(backtest_output)
    big_data['pnl_long5'] = big_data['change5']*risk/abs(big_data['downside'])
    big_data['pnl_short5'] = -big_data['change5']*risk/abs(big_data['upside'])
    big_data['pnl_final'] = big_data['pnl_long5']
    big_data.loc[big_data['q_carry'] < 0, 'pnl_final'] = big_data.loc[big_data['q_carry'] <0, 'pnl_short5']

    return {'total_pnl_frame': total_pnl_frame, 'big_data': big_data}

def construct_spread_portfolio(**kwargs):

    date_list = kwargs['date_list']
    risk = 1000

    futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in sc.max_tr_dte_limits.keys()}

    ticker_head_list = list(sc.max_tr_dte_limits.keys())

    total_pnl_frame = pd.DataFrame({'report_date': date_list})
    total_pnl_frame['portfolio'] = 0

    for i in range(len(ticker_head_list)):
        total_pnl_frame[ticker_head_list[i]] = 0

    for i in range(len(date_list)):
        spread_carry_output = sc.generate_spread_carry_sheet_4date(report_date=date_list[i],futures_data_dictionary=futures_data_dictionary)

        if spread_carry_output['success']:
            daily_sheet = spread_carry_output['spread_report']
        else:
            continue

        pnl_tickerhead_frame = pd.DataFrame({'ticker_head': ticker_head_list})
        pnl_tickerhead_frame['buy_mean_pnl'] = 0
        pnl_tickerhead_frame['sell_mean_pnl'] = 0
        pnl_tickerhead_frame['total_pnl'] = 0

        daily_sheet = \
                daily_sheet[(np.isfinite(daily_sheet['change5']))&
                            (np.isfinite(daily_sheet['upside']))&
                            (np.isfinite(daily_sheet['downside']))]

        for j in range(len(ticker_head_list)):
            ticker_head_results = daily_sheet[daily_sheet['tickerHead'] == ticker_head_list[j]]
            filter_output_long = sf.get_spread_carry_filters(data_frame_input=ticker_head_results, filter_list=['long1'])
            filter_output_short = sf.get_spread_carry_filters(data_frame_input=ticker_head_results, filter_list=['short1'])

            selected_short_trades = ticker_head_results[filter_output_short['selection_indx']]
            selected_long_trades = ticker_head_results[filter_output_long['selection_indx']]

            if len(selected_short_trades.index) > 0:

                short_pnl = -selected_short_trades['change5']*risk/abs(selected_short_trades['upside'])
                pnl_tickerhead_frame['sell_mean_pnl'][j] = short_pnl.mean()

            if len(selected_long_trades.index) > 0:
                long_pnl = selected_long_trades['change5']*risk/abs(selected_long_trades['downside'])
                pnl_tickerhead_frame['buy_mean_pnl'][j] = long_pnl.mean()

            pnl_tickerhead_frame['total_pnl'][j] = pnl_tickerhead_frame['buy_mean_pnl'][j] + pnl_tickerhead_frame['sell_mean_pnl'][j]
            total_pnl_frame[ticker_head_list[j]][i] = pnl_tickerhead_frame['total_pnl'][j]

        total_pnl_frame['portfolio'][i] = pnl_tickerhead_frame['total_pnl'].sum()

    return total_pnl_frame


ticker_head_risk_dict = {x+'_long': 0 for x in cmi.futures_butterfly_strategy_tickerhead_list}
ticker_head_risk_dict_short = {x+'_short': 0 for x in cmi.futures_butterfly_strategy_tickerhead_list}
ticker_head_risk_dict.update(ticker_head_risk_dict_short)
ticker_group_risk_dict = {'CL_B_short': 0,
                              'W_KW_short': 0,
                              'W_KW_long': 0,
                              'W_KW_C_short': 0,
                              'W_KW_C_long': 0,
                              'S_SM_BO_short': 0,
                              'S_SM_BO_long': 0}
risk = 1000


def construct_daily_position(**kwargs):

    trade_date = kwargs['trade_date']
    rule_no = kwargs['rule_no']
    portfolio_id = kwargs['portfolio_id']
    con = kwargs['con']
    risk_managementQ = kwargs['risk_managementQ']
    position_managementQ = kwargs['position_managementQ']
    max_holding_period = kwargs['max_holding_period']

    global ticker_head_risk_dict
    global ticker_group_risk_dict
    ticker_head_risk_dict = {x + '_long': 0 for x in cmi.futures_butterfly_strategy_tickerhead_list}
    ticker_head_risk_dict_short = {x + '_short': 0 for x in cmi.futures_butterfly_strategy_tickerhead_list}
    ticker_head_risk_dict.update(ticker_head_risk_dict_short)
    ticker_group_risk_dict = {'CL_B_short': 0,
                              'W_KW_short': 0,
                              'W_KW_long': 0,
                              'W_KW_C_short': 0,
                              'W_KW_C_long': 0,
                              'S_SM_BO_short': 0,
                              'S_SM_BO_long': 0}

    if 'futures_data_dictionary' in kwargs.keys():
        futures_data_dictionary = kwargs['futures_data_dictionary']
    else:
        futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in
                                   cmi.futures_butterfly_strategy_tickerhead_list}

    report_date = exp.doubledate_shift_bus_days(double_date=trade_date, shift_in_days=1)
    date25 = exp.doubledate_shift_bus_days(double_date=trade_date, shift_in_days=-25)

    report_date_folder = ts.create_strategy_output_dir(strategy_class='spread_carry', report_date=report_date)
    trade_date_folder = ts.create_strategy_output_dir(strategy_class='spread_carry', report_date=trade_date)

    report_date_output = sc.generate_spread_carry_sheet_4date(report_date=report_date,
                                                              futures_data_dictionary=futures_data_dictionary)

    trade_date_output = sc.generate_spread_carry_sheet_4date(report_date=trade_date,
                                                             futures_data_dictionary=futures_data_dictionary)

    report_date_sheet = report_date_output['spread_report']
    trade_date_sheet = trade_date_output['spread_report']

    report_date_sheet.reset_index(drop=True, inplace=True)
    trade_date_sheet.reset_index(drop=True, inplace=True)

    report_date_sheet = \
        report_date_sheet[(np.isfinite(report_date_sheet['change5'])) &
                          (np.isfinite(report_date_sheet['upside'])) &
                          (np.isfinite(report_date_sheet['downside']))]

    trade_date_sheet = \
        trade_date_sheet[(np.isfinite(trade_date_sheet['change5'])) &
                          (np.isfinite(trade_date_sheet['upside'])) &
                          (np.isfinite(trade_date_sheet['downside']))]

    if os.path.isfile(report_date_folder + '/portfolio' + '_' + str(portfolio_id) + '.pkl'):
        position_frame = pd.read_pickle(report_date_folder + '/portfolio' + '_' + str(portfolio_id) + '.pkl')

        for i in range(len(position_frame)):
            selected_row = trade_date_sheet[(trade_date_sheet['ticker1']==position_frame['ticker1'].iloc[i])&
                                            (trade_date_sheet['ticker2']==position_frame['ticker2'].iloc[i])]

            if len(selected_row)==0:
                new_q_carry = np.nan
                new_reward_risk = np.nan
            else:
                new_q_carry = selected_row['q_carry']
                new_reward_risk = selected_row['reward_risk']

            position_frame['holding_period'].iloc[i] = (cu.convert_doubledate_2datetime(trade_date) - position_frame['entry_date'].iloc[i]).days

            if (position_frame['holding_period'].iloc[i] > 7) and position_managementQ:
                if position_frame['ticker_head'].iloc[i] in ['CL', 'B']:
                    if ((position_frame['reward_risk'].iloc[i]>-0.06) and (new_reward_risk>-0.06)) or \
                            ((position_frame['q_carry'].iloc[i]>4) and (new_q_carry>4)):
                        position_frame['close_Q'].iloc[i] = True
            #else:
            #    if (position_frame['qty'].iloc[i]>0) and (position_frame['q_carry'].iloc[i]<13) and (new_q_carry<13):
            #        position_frame['close_Q'].iloc[i] = True
            #    elif (position_frame['qty'].iloc[i]<0) and (position_frame['q_carry'].iloc[i]>-7) and (new_q_carry>-7):
            #        position_frame['close_Q'].iloc[i] = True

            position_frame['reward_risk'] = new_reward_risk
            position_frame['q_carry'] = new_q_carry
            position_frame['holding_period'].iloc[i] = (cu.convert_doubledate_2datetime(trade_date) - position_frame['entry_date'].iloc[i]).days

            calculate_risk(ticker_head=position_frame['ticker_head'].iloc[i],additional_risk=position_frame['risk'].iloc[i], direction=np.sign(position_frame['qty'].iloc[i]))

            ticker1_0_output = gfp.get_futures_price_preloaded(ticker=position_frame['ticker1'].iloc[i],
                                                               settle_date=report_date,
                                                               futures_data_dictionary=futures_data_dictionary)
            ticker1_1_output = gfp.get_futures_price_preloaded(ticker=position_frame['ticker1'].iloc[i],
                                                               settle_date=trade_date,
                                                               futures_data_dictionary=futures_data_dictionary)

            ticker2_0_output = gfp.get_futures_price_preloaded(ticker=position_frame['ticker2'].iloc[i],
                                                               settle_date=report_date,
                                                               futures_data_dictionary=futures_data_dictionary)
            ticker2_1_output = gfp.get_futures_price_preloaded(ticker=position_frame['ticker2'].iloc[i],
                                                               settle_date=trade_date,
                                                               futures_data_dictionary=futures_data_dictionary)

            position_frame['pnl'].iloc[i] = position_frame['multiplier'].iloc[i] *\
            (position_frame['qty'].iloc[i] * (ticker1_1_output['close_price'].iloc[0] - ticker1_0_output['close_price'].iloc[0])
             -position_frame['qty'].iloc[i] * (ticker2_1_output['close_price'].iloc[0] - ticker2_0_output['close_price'].iloc[0]))

            new_pnl_total = position_frame['pnl_total'].iloc[i] + position_frame['pnl'].iloc[i]
            position_frame['pnl_total'].iloc[i] = new_pnl_total

            exp_output = exp.get_days2_expiration(ticker=position_frame['ticker1'].iloc[i], date_to=trade_date, instrument='futures',con=con)
            position_frame['front_tr_dte'].iloc[i] = exp_output['tr_dte']

            # Close the position less than 30 dte
            if position_frame['front_tr_dte'].iloc[i] < 30:
                position_frame['close_Q'].iloc[i] = True

            if position_frame['holding_period'].iloc[i]>max_holding_period:
                position_frame['close_Q'].iloc[i] = True

    else:
        position_frame = pd.DataFrame()

    report_date_long = sf.get_spread_carry_filters(data_frame_input=report_date_sheet, filter_list=['long' + str(rule_no)])
    report_date_short = sf.get_spread_carry_filters(data_frame_input=report_date_sheet, filter_list=['short' + str(rule_no)])

    trade_date_long = sf.get_spread_carry_filters(data_frame_input=trade_date_sheet, filter_list=['long' + str(rule_no)])
    trade_date_short = sf.get_spread_carry_filters(data_frame_input=trade_date_sheet, filter_list=['short' + str(rule_no)])

    report_frame_long = report_date_long['selected_frame']
    report_frame_short = report_date_short['selected_frame']

    trade_frame_long = trade_date_long['selected_frame']
    trade_frame_short = trade_date_short['selected_frame']

    merged_long = report_frame_long.merge(trade_frame_long, how='inner', on=['ticker1', 'ticker2','ticker1L','ticker2L'])
    merged_short = report_frame_short.merge(trade_frame_short, how='inner', on=['ticker1', 'ticker2','ticker1L','ticker2L'])

    if merged_long.empty and not merged_short.empty:
        merged_short['direction'] = -1
        merged_short['theme'] = [x+'_short' for x in merged_short['tickerHead_x']]
        merged_frame = merged_short
    elif merged_short.empty and not merged_long.empty:
        merged_long['direction'] = 1
        merged_long['theme'] = [x + '_long' for x in merged_long['tickerHead_x']]
        merged_frame = merged_long
    else:
        merged_short['direction'] = -1
        merged_long['direction'] = 1
        merged_short['theme'] = [x + '_short' for x in merged_short['tickerHead_x']]
        merged_long['theme'] = [x + '_long' for x in merged_long['tickerHead_x']]
        merged_frame = pd.concat([merged_long, merged_short])

    theme_list = merged_frame['theme'].unique()

    if risk_managementQ:
        for i in range(len(theme_list)):

            theme_frame = merged_frame[merged_frame['theme'] == theme_list[i]]
            randomized_frame = theme_frame.sample(frac=1)

            # only take the trade if there's no data gap going forward

            row_indx = -1

            for j in range(len(randomized_frame)):

                data1 = gfp.get_futures_price_preloaded(ticker=randomized_frame['ticker1'].iloc[j],
                                                        settle_date_from=trade_date,
                                                        settle_date_to=date25,
                                                        futures_data_dictionary=futures_data_dictionary)
                if data1['close_price'].isnull().any():
                    continue

                data2 = gfp.get_futures_price_preloaded(ticker=randomized_frame['ticker2'].iloc[j],
                                                        settle_date_from=trade_date,
                                                        settle_date_to=date25,
                                                        futures_data_dictionary=futures_data_dictionary)
                if data2['close_price'].isnull().any():
                    continue

                if not bool(merged_frame['ticker1L'].iloc[j].strip()):
                    continue

                data3 = gfp.get_futures_price_preloaded(ticker=randomized_frame['ticker1L'].iloc[j],
                                                        settle_date_from=trade_date,
                                                        settle_date_to=date25,
                                                        futures_data_dictionary=futures_data_dictionary)
                if data3['close_price'].isnull().any():
                    continue

                if not bool(merged_frame['ticker2L'].iloc[j].strip()):
                    continue

                data4 = gfp.get_futures_price_preloaded(ticker=randomized_frame['ticker2L'].iloc[j],
                                                        settle_date_from=trade_date,
                                                        settle_date_to=date25,
                                                        futures_data_dictionary=futures_data_dictionary)
                if data4['close_price'].isnull().any():
                    continue

                row_indx = j
                break

            if row_indx < 0:
                continue

            slack = calculate_slack_per_tickerhead(ticker_head=randomized_frame['tickerHead_x'].iloc[row_indx],
                                                   direction=randomized_frame['direction'].iloc[row_indx],
                                                   ticker_head_risk_dict=ticker_head_risk_dict,
                                                   ticker_group_risk_dict=ticker_group_risk_dict)

            if randomized_frame['tickerHead_x'].iloc[row_indx] in ['CL','B']:
                if slack > 3*risk/4:
                    slack = risk*0.6

            if slack<(risk/4):
                continue

            calculate_risk(ticker_head=randomized_frame['tickerHead_x'].iloc[row_indx], additional_risk=slack, direction=randomized_frame['direction'].iloc[row_indx])

            if randomized_frame['direction'].iloc[row_indx]>0:
                qty = round(abs(slack/randomized_frame['downside_y'].iloc[row_indx]))
            else:
                qty = -round(slack / randomized_frame['upside_y'].iloc[row_indx])

            position_frame = position_frame.append(pd.DataFrame({'ticker1': [randomized_frame['ticker1'].iloc[row_indx]],
                                                                 'ticker2': [randomized_frame['ticker2'].iloc[row_indx]],
                                                                 'ticker_head': [randomized_frame['tickerHead_x'].iloc[row_indx]],
                                                                 'front_tr_dte_initial': [randomized_frame['front_tr_dte_y'].iloc[row_indx]],
                                                                 'front_tr_dteL_initial': [randomized_frame['front_tr_dteL_y'].iloc[row_indx]],
                                                                 'front_tr_dte': [randomized_frame['front_tr_dte_y'].iloc[row_indx]],
                                                                 'qty': [qty],
                                                                 'pnl': [0],
                                                                 'pnl_total': [0],
                                                                 'risk': [slack],
                                                                 'multiplier': [cmi.contract_multiplier[randomized_frame['tickerHead_x'].iloc[row_indx]]],
                                                                 'q_carry_initial': [randomized_frame['q_carry_y'].iloc[row_indx]],
                                                                 'reward_risk_initial': [randomized_frame['reward_risk_y'].iloc[row_indx]],
                                                                 'q_inital':  [randomized_frame['q_y'].iloc[row_indx]],
                                                                 'q1_inital': [randomized_frame['q1_y'].iloc[row_indx]],
                                                                 'q5_inital': [randomized_frame['q5_y'].iloc[row_indx]],
                                                                 'q_carry_max_initial':  [randomized_frame['q_carry_max_y'].iloc[row_indx]],
                                                                 'q_carry_min_initial': [randomized_frame['q_carry_min_y'].iloc[row_indx]],
                                                                 'q_carry_average_initial': [ randomized_frame['q_carry_average_y'].iloc[row_indx]],
                                                                 'q_carry': [randomized_frame['q_carry_y'].iloc[row_indx]],
                                                                 'reward_risk': [randomized_frame['reward_risk_y'].iloc[row_indx]],
                                                                 'entry_date': [cu.convert_doubledate_2datetime(trade_date)],
                                                                 'holding_period': [0],
                                                                 'close_Q': [False]}))
    else:
        for i in range(len(merged_frame)):

            if len(position_frame)>0:

                existing_position = position_frame[(position_frame['ticker1'] == merged_frame['ticker1'].iloc[i]) & (
                        position_frame['ticker2'] == merged_frame['ticker2'].iloc[i])]

                if len(existing_position) > 0:
                    continue

            data1 = gfp.get_futures_price_preloaded(ticker=merged_frame['ticker1'].iloc[i],
                                                    settle_date_from=trade_date,
                                                    settle_date_to=date25,
                                                    futures_data_dictionary=futures_data_dictionary)
            if data1['close_price'].isnull().any():
                continue

            data2 = gfp.get_futures_price_preloaded(ticker=merged_frame['ticker2'].iloc[i],
                                                    settle_date_from=trade_date,
                                                    settle_date_to=date25,
                                                    futures_data_dictionary=futures_data_dictionary)
            if data2['close_price'].isnull().any():
                continue

            if not bool(merged_frame['ticker1L'].iloc[i].strip()):
                continue

            data3 = gfp.get_futures_price_preloaded(ticker=merged_frame['ticker1L'].iloc[i],
                                                    settle_date_from=trade_date,
                                                    settle_date_to=date25,
                                                    futures_data_dictionary=futures_data_dictionary)
            if data3['close_price'].isnull().any():
                continue

            if not bool(merged_frame['ticker2L'].iloc[i].strip()):
                continue

            data4 = gfp.get_futures_price_preloaded(ticker=merged_frame['ticker2L'].iloc[i],
                                                    settle_date_from=trade_date,
                                                    settle_date_to=date25,
                                                    futures_data_dictionary=futures_data_dictionary)
            if data4['close_price'].isnull().any():
                continue

            if merged_frame['direction'].iloc[i] > 0:
                qty = round(abs(risk / merged_frame['downside_y'].iloc[i]))
            else:
                qty = -round(risk / merged_frame['upside_y'].iloc[i])

            position_frame = position_frame.append(
                pd.DataFrame({'ticker1': [merged_frame['ticker1'].iloc[i]],
                              'ticker2': [merged_frame['ticker2'].iloc[i]],
                              'ticker_head': [merged_frame['tickerHead_x'].iloc[i]],
                              'front_tr_dte_initial': [merged_frame['front_tr_dte_y'].iloc[i]],
                              'front_tr_dteL_initial': [merged_frame['front_tr_dteL_y'].iloc[i]],
                              'front_tr_dte': [merged_frame['front_tr_dte_y'].iloc[i]],
                              'qty': [qty],
                              'pnl': [0],
                              'pnl_total': [0],
                              'risk': [risk],
                              'multiplier': [cmi.contract_multiplier[merged_frame['tickerHead_x'].iloc[i]]],
                              'q_carry_initial': [merged_frame['q_carry_y'].iloc[i]],
                              'reward_risk_initial': [merged_frame['reward_risk_y'].iloc[i]],
                              'q_inital': [merged_frame['q_y'].iloc[i]],
                              'q1_inital': [merged_frame['q1_y'].iloc[i]],
                              'q5_inital': [merged_frame['q5_y'].iloc[i]],
                              'q_carry_max_initial': [merged_frame['q_carry_max_y'].iloc[i]],
                              'q_carry_min_initial': [merged_frame['q_carry_min_y'].iloc[i]],
                              'q_carry_average_initial': [merged_frame['q_carry_average_y'].iloc[i]],
                              'q_carry': [merged_frame['q_carry_y'].iloc[i]],
                              'reward_risk': [merged_frame['reward_risk_y'].iloc[i]],
                              'entry_date': [cu.convert_doubledate_2datetime(trade_date)],
                              'holding_period': [0],
                              'close_Q': [False]}))

    if position_frame['close_Q'].any():
        closed_positions = position_frame[position_frame['close_Q']]
        closed_positions['exit_date'] = cu.convert_doubledate_2datetime(trade_date)
        position_frame = position_frame[~position_frame['close_Q']]
    else:
        closed_positions = pd.DataFrame()

    position_frame.to_pickle(trade_date_folder + '/portfolio' + '_' + str(portfolio_id) + '.pkl')
    closed_positions.to_pickle(trade_date_folder + '/closed_positions' + '_' + str(portfolio_id) + '.pkl')

    return position_frame


def calculate_risk(**kwargs):

    ticker_head = kwargs['ticker_head']
    additional_risk = kwargs['additional_risk']
    direction = kwargs['direction']

    global ticker_head_risk_dict
    global ticker_group_risk_dict

    if direction > 0:
        direction_str = '_long'
    else:
        direction_str = '_short'

    ticker_head_risk_dict[ticker_head + direction_str] = ticker_head_risk_dict[ticker_head + direction_str] + additional_risk

    if ticker_head in ['CL', 'B']:
        ticker_group_risk_dict['CL_B' + direction_str] = ticker_group_risk_dict['CL_B' + direction_str] + additional_risk

    if ticker_head in ['W', 'KW']:
        ticker_group_risk_dict['W_KW' + direction_str] = ticker_group_risk_dict['W_KW' + direction_str] + additional_risk

    if ticker_head in ['W', 'KW', 'C']:
        ticker_group_risk_dict['W_KW_C' + direction_str] = ticker_group_risk_dict['W_KW_C' + direction_str] + additional_risk

    if ticker_head in ['S', 'SM', 'BO']:
        ticker_group_risk_dict['S_SM_BO' + direction_str] = ticker_group_risk_dict['S_SM_BO' + direction_str] + additional_risk


def calculate_slack_per_tickerhead(**kwargs):

    ticker_head = kwargs['ticker_head']
    direction = kwargs['direction']

    if direction > 0:
        direction_str = '_long'
    else:
        direction_str = '_short'

    ticker_head_slack = risk-ticker_head_risk_dict[ticker_head + direction_str]
    ticker_group_slack = ticker_head_slack

    if ticker_head in ['CL', 'B']:
        ticker_group_slack = min(1.2*risk-ticker_group_risk_dict['CL_B' + direction_str], ticker_group_slack)

    if ticker_head in ['W', 'KW']:
        ticker_group_slack = min(1.2*risk-ticker_group_risk_dict['W_KW' + direction_str], ticker_group_slack)

    if ticker_head in ['W', 'KW', 'C']:
        ticker_group_slack = min(1.6 * risk - ticker_group_risk_dict['W_KW_C' + direction_str], ticker_group_slack)

    if ticker_head in ['S', 'SM', 'BO']:
        ticker_group_slack = min(2 * risk - ticker_group_risk_dict['S_SM_BO' + direction_str], ticker_group_slack)

    return min(ticker_head_slack, ticker_group_slack)







