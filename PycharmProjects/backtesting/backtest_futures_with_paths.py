__author__ = 'kocat_000'

import opportunity_constructs.futures_butterfly as opfb
import pandas as pd
import numpy as np
import backtesting.utilities as bu
import ta.strategy as ts
import os.path
import get_price.get_futures_price as gfp
import contract_utilities.contract_meta_info as cmi

def get_backtest_summary_4_date(**kwargs):

    report_date = kwargs['report_date']
    print(report_date)
    futures_data_dictionary = kwargs['futures_data_dictionary']

    if 'use_existing_filesQ' in kwargs.keys():
        use_existing_filesQ = kwargs['use_existing_filesQ']
    else:
        use_existing_filesQ = True

    output_dir = ts.create_strategy_output_dir(strategy_class='futures_butterfly', report_date=report_date)

    if os.path.isfile(output_dir + '/backtest_results.pkl') and use_existing_filesQ:
        return pd.read_pickle(output_dir + '/backtest_results.pkl')

    butt_out = opfb.generate_futures_butterfly_sheet_4date(date_to=report_date)

    strategy_sheet = butt_out['butterflies']
    num_trades = len(strategy_sheet.index)

    holding_period5 = [np.NAN]*num_trades
    holding_period10 = [np.NAN]*num_trades
    holding_period15 = [np.NAN]*num_trades
    holding_period20 = [np.NAN]*num_trades
    holding_period25 = [np.NAN]*num_trades

    path_pnl5 = [np.NAN]*num_trades
    path_pnl10 = [np.NAN]*num_trades
    path_pnl15 = [np.NAN]*num_trades
    path_pnl20 = [np.NAN]*num_trades
    path_pnl25 = [np.NAN]*num_trades

    hold_pnl1long = [np.NAN]*num_trades
    hold_pnl2long = [np.NAN]*num_trades
    hold_pnl5long = [np.NAN]*num_trades
    hold_pnl10long = [np.NAN]*num_trades
    hold_pnl20long = [np.NAN]*num_trades

    hold_pnl1short = [np.NAN]*num_trades
    hold_pnl2short = [np.NAN]*num_trades
    hold_pnl5short = [np.NAN]*num_trades
    hold_pnl10short = [np.NAN]*num_trades
    hold_pnl20short = [np.NAN]*num_trades

    path_pnl5_per_contract = [np.NAN]*num_trades
    path_pnl10_per_contract = [np.NAN]*num_trades
    path_pnl15_per_contract = [np.NAN]*num_trades
    path_pnl20_per_contract = [np.NAN]*num_trades
    path_pnl25_per_contract = [np.NAN]*num_trades

    hold_pnl1long_per_contract = [np.NAN]*num_trades
    hold_pnl2long_per_contract = [np.NAN]*num_trades
    hold_pnl5long_per_contract = [np.NAN]*num_trades
    hold_pnl10long_per_contract = [np.NAN]*num_trades
    hold_pnl20long_per_contract = [np.NAN]*num_trades

    hold_pnl1short_per_contract = [np.NAN]*num_trades
    hold_pnl2short_per_contract = [np.NAN]*num_trades
    hold_pnl5short_per_contract = [np.NAN]*num_trades
    hold_pnl10short_per_contract = [np.NAN]*num_trades
    hold_pnl20short_per_contract = [np.NAN]*num_trades

    hold_pnl1long12 = [np.NAN] * num_trades
    hold_pnl2long12 = [np.NAN] * num_trades
    hold_pnl5long12 = [np.NAN] * num_trades
    hold_pnl10long12 = [np.NAN] * num_trades
    hold_pnl20long12 = [np.NAN] * num_trades

    hold_pnl1short12 = [np.NAN] * num_trades
    hold_pnl2short12 = [np.NAN] * num_trades
    hold_pnl5short12 = [np.NAN] * num_trades
    hold_pnl10short12 = [np.NAN] * num_trades
    hold_pnl20short12 = [np.NAN] * num_trades

    hold_pnl1long_per_contract12 = [np.NAN] * num_trades
    hold_pnl2long_per_contract12 = [np.NAN] * num_trades
    hold_pnl5long_per_contract12 = [np.NAN] * num_trades
    hold_pnl10long_per_contract12 = [np.NAN] * num_trades
    hold_pnl20long_per_contract12 = [np.NAN] * num_trades

    hold_pnl1short_per_contract12 = [np.NAN] * num_trades
    hold_pnl2short_per_contract12 = [np.NAN] * num_trades
    hold_pnl5short_per_contract12 = [np.NAN] * num_trades
    hold_pnl10short_per_contract12 = [np.NAN] * num_trades
    hold_pnl20short_per_contract12 = [np.NAN] * num_trades

    hold_pnl1long3 = [np.NAN] * num_trades
    hold_pnl2long3 = [np.NAN] * num_trades
    hold_pnl5long3 = [np.NAN] * num_trades
    hold_pnl10long3 = [np.NAN] * num_trades
    hold_pnl20long3 = [np.NAN] * num_trades

    hold_pnl1short3 = [np.NAN] * num_trades
    hold_pnl2short3 = [np.NAN] * num_trades
    hold_pnl5short3 = [np.NAN] * num_trades
    hold_pnl10short3 = [np.NAN] * num_trades
    hold_pnl20short3 = [np.NAN] * num_trades

    hold_pnl1long_per_contract3 = [np.NAN] * num_trades
    hold_pnl2long_per_contract3 = [np.NAN] * num_trades
    hold_pnl5long_per_contract3 = [np.NAN] * num_trades
    hold_pnl10long_per_contract3 = [np.NAN] * num_trades
    hold_pnl20long_per_contract3 = [np.NAN] * num_trades

    hold_pnl1short_per_contract3 = [np.NAN] * num_trades
    hold_pnl2short_per_contract3 = [np.NAN] * num_trades
    hold_pnl5short_per_contract3 = [np.NAN] * num_trades
    hold_pnl10short_per_contract3 = [np.NAN] * num_trades
    hold_pnl20short_per_contract3 = [np.NAN] * num_trades

    hold_pnl1long32 = [np.NAN] * num_trades
    hold_pnl2long32 = [np.NAN] * num_trades
    hold_pnl5long32 = [np.NAN] * num_trades
    hold_pnl10long32 = [np.NAN] * num_trades
    hold_pnl20long32 = [np.NAN] * num_trades

    hold_pnl1short32 = [np.NAN] * num_trades
    hold_pnl2short32 = [np.NAN] * num_trades
    hold_pnl5short32 = [np.NAN] * num_trades
    hold_pnl10short32 = [np.NAN] * num_trades
    hold_pnl20short32 = [np.NAN] * num_trades

    hold_pnl1long_per_contract32 = [np.NAN] * num_trades
    hold_pnl2long_per_contract32 = [np.NAN] * num_trades
    hold_pnl5long_per_contract32 = [np.NAN] * num_trades
    hold_pnl10long_per_contract32 = [np.NAN] * num_trades
    hold_pnl20long_per_contract32 = [np.NAN] * num_trades

    hold_pnl1short_per_contract32 = [np.NAN] * num_trades
    hold_pnl2short_per_contract32 = [np.NAN] * num_trades
    hold_pnl5short_per_contract32 = [np.NAN] * num_trades
    hold_pnl10short_per_contract32 = [np.NAN] * num_trades
    hold_pnl20short_per_contract32 = [np.NAN] * num_trades

    hold_pnl1long4 = [np.NAN] * num_trades
    hold_pnl2long4 = [np.NAN] * num_trades
    hold_pnl5long4 = [np.NAN] * num_trades
    hold_pnl10long4 = [np.NAN] * num_trades
    hold_pnl20long4 = [np.NAN] * num_trades

    hold_pnl1short4 = [np.NAN] * num_trades
    hold_pnl2short4 = [np.NAN] * num_trades
    hold_pnl5short4 = [np.NAN] * num_trades
    hold_pnl10short4 = [np.NAN] * num_trades
    hold_pnl20short4 = [np.NAN] * num_trades

    hold_pnl1long_per_contract4 = [np.NAN] * num_trades
    hold_pnl2long_per_contract4 = [np.NAN] * num_trades
    hold_pnl5long_per_contract4 = [np.NAN] * num_trades
    hold_pnl10long_per_contract4 = [np.NAN] * num_trades
    hold_pnl20long_per_contract4 = [np.NAN] * num_trades

    hold_pnl1short_per_contract4 = [np.NAN] * num_trades
    hold_pnl2short_per_contract4 = [np.NAN] * num_trades
    hold_pnl5short_per_contract4 = [np.NAN] * num_trades
    hold_pnl10short_per_contract4 = [np.NAN] * num_trades
    hold_pnl20short_per_contract4 = [np.NAN] * num_trades



    hold_pnl1long5 = [np.NAN] * num_trades
    hold_pnl2long5 = [np.NAN] * num_trades
    hold_pnl5long5 = [np.NAN] * num_trades
    hold_pnl10long5 = [np.NAN] * num_trades
    hold_pnl20long5 = [np.NAN] * num_trades

    hold_pnl1short5 = [np.NAN] * num_trades
    hold_pnl2short5 = [np.NAN] * num_trades
    hold_pnl5short5 = [np.NAN] * num_trades
    hold_pnl10short5 = [np.NAN] * num_trades
    hold_pnl20short5 = [np.NAN] * num_trades

    hold_pnl1long_per_contract5 = [np.NAN] * num_trades
    hold_pnl2long_per_contract5 = [np.NAN] * num_trades
    hold_pnl5long_per_contract5 = [np.NAN] * num_trades
    hold_pnl10long_per_contract5 = [np.NAN] * num_trades
    hold_pnl20long_per_contract5 = [np.NAN] * num_trades

    hold_pnl1short_per_contract5 = [np.NAN] * num_trades
    hold_pnl2short_per_contract5 = [np.NAN] * num_trades
    hold_pnl5short_per_contract5 = [np.NAN] * num_trades
    hold_pnl10short_per_contract5 = [np.NAN] * num_trades
    hold_pnl20short_per_contract5 = [np.NAN] * num_trades

    hold_pnl1long6 = [np.NAN] * num_trades
    hold_pnl2long6 = [np.NAN] * num_trades
    hold_pnl5long6 = [np.NAN] * num_trades
    hold_pnl10long6 = [np.NAN] * num_trades
    hold_pnl20long6 = [np.NAN] * num_trades

    hold_pnl1short6 = [np.NAN] * num_trades
    hold_pnl2short6 = [np.NAN] * num_trades
    hold_pnl5short6 = [np.NAN] * num_trades
    hold_pnl10short6 = [np.NAN] * num_trades
    hold_pnl20short6 = [np.NAN] * num_trades

    hold_pnl1long_per_contract6 = [np.NAN] * num_trades
    hold_pnl2long_per_contract6 = [np.NAN] * num_trades
    hold_pnl5long_per_contract6 = [np.NAN] * num_trades
    hold_pnl10long_per_contract6 = [np.NAN] * num_trades
    hold_pnl20long_per_contract6 = [np.NAN] * num_trades

    hold_pnl1short_per_contract6 = [np.NAN] * num_trades
    hold_pnl2short_per_contract6 = [np.NAN] * num_trades
    hold_pnl5short_per_contract6 = [np.NAN] * num_trades
    hold_pnl10short_per_contract6 = [np.NAN] * num_trades
    hold_pnl20short_per_contract6 = [np.NAN] * num_trades

    for i in range(num_trades):
        sheet_entry = strategy_sheet.iloc[i]

        data_list = []

        for j in range(3):

            ticker_frame = gfp.get_futures_price_preloaded(ticker=sheet_entry['ticker'+str(j+1)],
                                    futures_data_dictionary=futures_data_dictionary,
                                    settle_date_from_exclusive=report_date)

            ticker_frame.set_index('settle_date', drop=False, inplace=True)
            data_list.append(ticker_frame)


        merged_data = pd.concat(data_list, axis=1, join='inner')

        mid_price = (merged_data['close_price'].iloc[:,0]*sheet_entry['weight1']+
                     merged_data['close_price'].iloc[:,2]*sheet_entry['weight3'])
        ratio_path = mid_price/(-sheet_entry['weight2']*merged_data['close_price'].iloc[:,1])

        if len(ratio_path.index) < 6:
            continue

        #if sheet_entry['second_spread_weight_1'] < 0:
        #    continue

        quantity_long = round(10000/abs(sheet_entry['downside']))
        quantity_short = -round(10000/abs(sheet_entry['upside']))

        quantity_long12 = round(10000 / abs(sheet_entry['downside12']))
        quantity_short12 = -round(10000 / abs(sheet_entry['upside12']))

        quantity_long3 = round(10000 / abs(sheet_entry['downside3']))
        quantity_short3 = -round(10000 / abs(sheet_entry['upside3']))

        quantity_long32 = round(10000 / abs(sheet_entry['downside32']))
        quantity_short32 = -round(10000 / abs(sheet_entry['upside32']))

        quantity_long4 = round(10000 / abs(sheet_entry['downside4']))
        quantity_short4 = -round(10000 / abs(sheet_entry['upside4']))

        quantity_long5 = round(10000 / abs(sheet_entry['downside5']))
        quantity_short5 = -round(10000 / abs(sheet_entry['upside5']))

        quantity_long6 = round(10000 / abs(sheet_entry['downside6']))
        quantity_short6 = -round(10000 / abs(sheet_entry['upside6']))

        contracts_traded_per_unit = 4*(1+sheet_entry['second_spread_weight_1'])
        contracts_traded_per_unit12 = 4 * (1 + sheet_entry['second_spread_weight_12'])
        contracts_traded_per_unit3 = 4 * (1 + sheet_entry['second_spread_weight_3'])
        contracts_traded_per_unit32 = 4 * (1 + sheet_entry['second_spread_weight_32'])

        contracts_traded_per_unit4 = 4 * (1 + sheet_entry['second_spread_weight_4'])
        contracts_traded_per_unit5 = 4 * (1 + sheet_entry['second_spread_weight_5'])
        contracts_traded_per_unit6 = 4 * (1 + sheet_entry['second_spread_weight_6'])

        if sheet_entry['QF'] > 50:
            trigger_direction = 'going_down'
            quantity = quantity_short
            quantity12 = quantity_short12
            quantity3 = quantity_short3
            quantity32 = quantity_short32
        elif sheet_entry['QF'] < 50:
            trigger_direction = 'going_up'
            quantity = quantity_long
            quantity12 = quantity_long12
            quantity3 = quantity_long3
            quantity32 = quantity_long32
        else:
            quantity = np.NAN
            quantity12 = np.NAN
            quantity3 = np.NAN
            quantity32 = np.NAN

        total_contracts_traded_per_unit = abs(quantity)*contracts_traded_per_unit

        exit5 = bu.find_exit_point(time_series=ratio_path,trigger_value=sheet_entry['ratio_target5'],
                                   trigger_direction=trigger_direction,max_exit_point=min(20, len(ratio_path.index)-1))

        exit10 = bu.find_exit_point(time_series=ratio_path,trigger_value=sheet_entry['ratio_target10'],
                                    trigger_direction=trigger_direction,max_exit_point=min(20, len(ratio_path.index)-1))

        exit15 = bu.find_exit_point(time_series=ratio_path,trigger_value=sheet_entry['ratio_target15'],
                                    trigger_direction=trigger_direction,max_exit_point=min(20, len(ratio_path.index)-1))

        exit20 = bu.find_exit_point(time_series=ratio_path,trigger_value=sheet_entry['ratio_target20'],
                                    trigger_direction=trigger_direction,max_exit_point=min(20, len(ratio_path.index)-1))

        exit25 = bu.find_exit_point(time_series=ratio_path,trigger_value=sheet_entry['ratio_target25'],
                                    trigger_direction=trigger_direction,max_exit_point=min(20, len(ratio_path.index)-1))

        holding_period5[i] = exit5
        holding_period10[i] = exit10
        holding_period15[i] = exit15
        holding_period20[i] = exit20
        holding_period25[i] = exit25

        path_path_list = []
        hold_pnl_list = []
        hold_pnl12_list = []
        hold_pnl3_list = []
        hold_pnl32_list = []
        hold_pnl4_list = []
        hold_pnl5_list = []
        hold_pnl6_list = []

        for exit_indx in [exit5, exit10, exit15, exit20, exit25]:

            exit_indx_robust = min(len(ratio_path.index)-1, exit_indx)

            raw_pnl = (merged_data['close_price'].iloc[exit_indx_robust,0]-merged_data['close_price'].iloc[0,0])\
                       -(1+sheet_entry['second_spread_weight_1'])*(merged_data['close_price'].iloc[exit_indx_robust,1]-merged_data['close_price'].iloc[0,1])\
                       +(sheet_entry['second_spread_weight_1'])*(merged_data['close_price'].iloc[exit_indx_robust,2]-merged_data['close_price'].iloc[0,2])

            path_path_list.append(raw_pnl*sheet_entry['multiplier']*quantity)

        for hold_indx in [1, 2, 5, 10, 20]:

            hold_indx_robust = min(len(ratio_path.index)-1, hold_indx)

            hold_pnl = (merged_data['close_price'].iloc[hold_indx_robust, 0]-merged_data['close_price'].iloc[0, 0])\
                       -(1+sheet_entry['second_spread_weight_1'])*(merged_data['close_price'].iloc[hold_indx_robust, 1]-merged_data['close_price'].iloc[0, 1])\
                       +(sheet_entry['second_spread_weight_1'])*(merged_data['close_price'].iloc[hold_indx_robust, 2]-merged_data['close_price'].iloc[0, 2])

            hold_pnl12 = (merged_data['close_price'].iloc[hold_indx_robust, 0] - merged_data['close_price'].iloc[0, 0]) \
                       - (1 + sheet_entry['second_spread_weight_12']) * (merged_data['close_price'].iloc[hold_indx_robust, 1] - merged_data['close_price'].iloc[0, 1]) \
                       + (sheet_entry['second_spread_weight_12']) * (merged_data['close_price'].iloc[hold_indx_robust, 2] - merged_data['close_price'].iloc[0, 2])

            hold_pnl3 = (merged_data['close_price'].iloc[hold_indx_robust, 0] - merged_data['close_price'].iloc[0, 0]) \
                         - (1 + sheet_entry['second_spread_weight_3']) * (merged_data['close_price'].iloc[hold_indx_robust, 1] -merged_data['close_price'].iloc[0, 1]) \
                         + (sheet_entry['second_spread_weight_3']) * (merged_data['close_price'].iloc[hold_indx_robust, 2] - merged_data['close_price'].iloc[0, 2])

            hold_pnl32 = (merged_data['close_price'].iloc[hold_indx_robust, 0] - merged_data['close_price'].iloc[0, 0]) \
                        - (1 + sheet_entry['second_spread_weight_32']) * (merged_data['close_price'].iloc[hold_indx_robust, 1] - merged_data['close_price'].iloc[0, 1]) \
                        + (sheet_entry['second_spread_weight_32']) * (merged_data['close_price'].iloc[hold_indx_robust, 2] - merged_data['close_price'].iloc[0, 2])

            hold_pnl4 = (merged_data['close_price'].iloc[hold_indx_robust, 0] - merged_data['close_price'].iloc[0, 0]) \
                        - (1 + sheet_entry['second_spread_weight_4']) * (
                                    merged_data['close_price'].iloc[hold_indx_robust, 1] -
                                    merged_data['close_price'].iloc[0, 1]) \
                        + (sheet_entry['second_spread_weight_4']) * (
                                    merged_data['close_price'].iloc[hold_indx_robust, 2] -
                                    merged_data['close_price'].iloc[0, 2])

            hold_pnl5 = (merged_data['close_price'].iloc[hold_indx_robust, 0] - merged_data['close_price'].iloc[0, 0]) \
                        - (1 + sheet_entry['second_spread_weight_5']) * (
                                merged_data['close_price'].iloc[hold_indx_robust, 1] -
                                merged_data['close_price'].iloc[0, 1]) \
                        + (sheet_entry['second_spread_weight_5']) * (
                                merged_data['close_price'].iloc[hold_indx_robust, 2] -
                                merged_data['close_price'].iloc[0, 2])

            hold_pnl6 = (merged_data['close_price'].iloc[hold_indx_robust, 0] - merged_data['close_price'].iloc[0, 0]) \
                        - (1 + sheet_entry['second_spread_weight_6']) * (
                                merged_data['close_price'].iloc[hold_indx_robust, 1] -
                                merged_data['close_price'].iloc[0, 1]) \
                        + (sheet_entry['second_spread_weight_6']) * (
                                merged_data['close_price'].iloc[hold_indx_robust, 2] -
                                merged_data['close_price'].iloc[0, 2])

            hold_pnl_list.append(hold_pnl*sheet_entry['multiplier'])
            hold_pnl12_list.append(hold_pnl12 * sheet_entry['multiplier'])
            hold_pnl3_list.append(hold_pnl3 * sheet_entry['multiplier'])
            hold_pnl32_list.append(hold_pnl32 * sheet_entry['multiplier'])
            hold_pnl4_list.append(hold_pnl4 * sheet_entry['multiplier'])
            hold_pnl5_list.append(hold_pnl5 * sheet_entry['multiplier'])
            hold_pnl6_list.append(hold_pnl6 * sheet_entry['multiplier'])

        path_pnl5[i] = path_path_list[0]
        path_pnl10[i] = path_path_list[1]
        path_pnl15[i] = path_path_list[2]
        path_pnl20[i] = path_path_list[3]
        path_pnl25[i] = path_path_list[4]

        path_pnl5_per_contract[i] = path_path_list[0]/total_contracts_traded_per_unit
        path_pnl10_per_contract[i] = path_path_list[1]/total_contracts_traded_per_unit
        path_pnl15_per_contract[i] = path_path_list[2]/total_contracts_traded_per_unit
        path_pnl20_per_contract[i] = path_path_list[3]/total_contracts_traded_per_unit
        path_pnl25_per_contract[i] = path_path_list[4]/total_contracts_traded_per_unit

        hold_pnl1long[i] = hold_pnl_list[0]*quantity_long
        hold_pnl2long[i] = hold_pnl_list[1]*quantity_long
        hold_pnl5long[i] = hold_pnl_list[2]*quantity_long
        hold_pnl10long[i] = hold_pnl_list[3]*quantity_long
        hold_pnl20long[i] = hold_pnl_list[4]*quantity_long

        hold_pnl1long_per_contract[i] = hold_pnl_list[0]/contracts_traded_per_unit
        hold_pnl2long_per_contract[i] = hold_pnl_list[1]/contracts_traded_per_unit
        hold_pnl5long_per_contract[i] = hold_pnl_list[2]/contracts_traded_per_unit
        hold_pnl10long_per_contract[i] = hold_pnl_list[3]/contracts_traded_per_unit
        hold_pnl20long_per_contract[i] = hold_pnl_list[4]/contracts_traded_per_unit

        hold_pnl1short[i] = hold_pnl_list[0]*quantity_short
        hold_pnl2short[i] = hold_pnl_list[1]*quantity_short
        hold_pnl5short[i] = hold_pnl_list[2]*quantity_short
        hold_pnl10short[i] = hold_pnl_list[3]*quantity_short
        hold_pnl20short[i] = hold_pnl_list[4]*quantity_short

        hold_pnl1short_per_contract[i] = -hold_pnl_list[0]/contracts_traded_per_unit
        hold_pnl2short_per_contract[i] = -hold_pnl_list[1]/contracts_traded_per_unit
        hold_pnl5short_per_contract[i] = -hold_pnl_list[2]/contracts_traded_per_unit
        hold_pnl10short_per_contract[i] = -hold_pnl_list[3]/contracts_traded_per_unit
        hold_pnl20short_per_contract[i] = -hold_pnl_list[4]/contracts_traded_per_unit

        #############################################

        hold_pnl1long12[i] = hold_pnl12_list[0] * quantity_long12
        hold_pnl2long12[i] = hold_pnl12_list[1] * quantity_long12
        hold_pnl5long12[i] = hold_pnl12_list[2] * quantity_long12
        hold_pnl10long12[i] = hold_pnl12_list[3] * quantity_long12
        hold_pnl20long12[i] = hold_pnl12_list[4] * quantity_long12

        hold_pnl1long_per_contract12[i] = hold_pnl12_list[0] / contracts_traded_per_unit12
        hold_pnl2long_per_contract12[i] = hold_pnl12_list[1] / contracts_traded_per_unit12
        hold_pnl5long_per_contract12[i] = hold_pnl12_list[2] / contracts_traded_per_unit12
        hold_pnl10long_per_contract12[i] = hold_pnl12_list[3] / contracts_traded_per_unit12
        hold_pnl20long_per_contract12[i] = hold_pnl12_list[4] / contracts_traded_per_unit12

        hold_pnl1short12[i] = hold_pnl12_list[0] * quantity_short12
        hold_pnl2short12[i] = hold_pnl12_list[1] * quantity_short12
        hold_pnl5short12[i] = hold_pnl12_list[2] * quantity_short12
        hold_pnl10short12[i] = hold_pnl12_list[3] * quantity_short12
        hold_pnl20short12[i] = hold_pnl12_list[4] * quantity_short12

        hold_pnl1short_per_contract12[i] = -hold_pnl12_list[0] / contracts_traded_per_unit12
        hold_pnl2short_per_contract12[i] = -hold_pnl12_list[1] / contracts_traded_per_unit12
        hold_pnl5short_per_contract12[i] = -hold_pnl12_list[2] / contracts_traded_per_unit12
        hold_pnl10short_per_contract12[i] = -hold_pnl12_list[3] / contracts_traded_per_unit12
        hold_pnl20short_per_contract12[i] = -hold_pnl12_list[4] / contracts_traded_per_unit12

        #############################################

        hold_pnl1long3[i] = hold_pnl3_list[0] * quantity_long3
        hold_pnl2long3[i] = hold_pnl3_list[1] * quantity_long3
        hold_pnl5long3[i] = hold_pnl3_list[2] * quantity_long3
        hold_pnl10long3[i] = hold_pnl3_list[3] * quantity_long3
        hold_pnl20long3[i] = hold_pnl3_list[4] * quantity_long3

        hold_pnl1long_per_contract3[i] = hold_pnl3_list[0] / contracts_traded_per_unit3
        hold_pnl2long_per_contract3[i] = hold_pnl3_list[1] / contracts_traded_per_unit3
        hold_pnl5long_per_contract3[i] = hold_pnl3_list[2] / contracts_traded_per_unit3
        hold_pnl10long_per_contract3[i] = hold_pnl3_list[3] / contracts_traded_per_unit3
        hold_pnl20long_per_contract3[i] = hold_pnl3_list[4] / contracts_traded_per_unit3

        hold_pnl1short3[i] = hold_pnl3_list[0] * quantity_short3
        hold_pnl2short3[i] = hold_pnl3_list[1] * quantity_short3
        hold_pnl5short3[i] = hold_pnl3_list[2] * quantity_short3
        hold_pnl10short3[i] = hold_pnl3_list[3] * quantity_short3
        hold_pnl20short3[i] = hold_pnl3_list[4] * quantity_short3

        hold_pnl1short_per_contract3[i] = -hold_pnl3_list[0] / contracts_traded_per_unit3
        hold_pnl2short_per_contract3[i] = -hold_pnl3_list[1] / contracts_traded_per_unit3
        hold_pnl5short_per_contract3[i] = -hold_pnl3_list[2] / contracts_traded_per_unit3
        hold_pnl10short_per_contract3[i] = -hold_pnl3_list[3] / contracts_traded_per_unit3
        hold_pnl20short_per_contract3[i] = -hold_pnl3_list[4] / contracts_traded_per_unit3

        #############################################

        hold_pnl1long32[i] = hold_pnl32_list[0] * quantity_long32
        hold_pnl2long32[i] = hold_pnl32_list[1] * quantity_long32
        hold_pnl5long32[i] = hold_pnl32_list[2] * quantity_long32
        hold_pnl10long32[i] = hold_pnl32_list[3] * quantity_long32
        hold_pnl20long32[i] = hold_pnl32_list[4] * quantity_long32

        hold_pnl1long_per_contract32[i] = hold_pnl32_list[0] / contracts_traded_per_unit32
        hold_pnl2long_per_contract32[i] = hold_pnl32_list[1] / contracts_traded_per_unit32
        hold_pnl5long_per_contract32[i] = hold_pnl32_list[2] / contracts_traded_per_unit32
        hold_pnl10long_per_contract32[i] = hold_pnl32_list[3] / contracts_traded_per_unit32
        hold_pnl20long_per_contract32[i] = hold_pnl32_list[4] / contracts_traded_per_unit32

        hold_pnl1short32[i] = hold_pnl32_list[0] * quantity_short32
        hold_pnl2short32[i] = hold_pnl32_list[1] * quantity_short32
        hold_pnl5short32[i] = hold_pnl32_list[2] * quantity_short32
        hold_pnl10short32[i] = hold_pnl32_list[3] * quantity_short32
        hold_pnl20short32[i] = hold_pnl32_list[4] * quantity_short32

        hold_pnl1short_per_contract32[i] = -hold_pnl32_list[0] / contracts_traded_per_unit32
        hold_pnl2short_per_contract32[i] = -hold_pnl32_list[1] / contracts_traded_per_unit32
        hold_pnl5short_per_contract32[i] = -hold_pnl32_list[2] / contracts_traded_per_unit32
        hold_pnl10short_per_contract32[i] = -hold_pnl32_list[3] / contracts_traded_per_unit32
        hold_pnl20short_per_contract32[i] = -hold_pnl32_list[4] / contracts_traded_per_unit32

        #############################################

        hold_pnl1long4[i] = hold_pnl4_list[0] * quantity_long4
        hold_pnl2long4[i] = hold_pnl4_list[1] * quantity_long4
        hold_pnl5long4[i] = hold_pnl4_list[2] * quantity_long4
        hold_pnl10long4[i] = hold_pnl4_list[3] * quantity_long4
        hold_pnl20long4[i] = hold_pnl4_list[4] * quantity_long4

        hold_pnl1long_per_contract4[i] = hold_pnl4_list[0] / contracts_traded_per_unit4
        hold_pnl2long_per_contract4[i] = hold_pnl4_list[1] / contracts_traded_per_unit4
        hold_pnl5long_per_contract4[i] = hold_pnl4_list[2] / contracts_traded_per_unit4
        hold_pnl10long_per_contract4[i] = hold_pnl4_list[3] / contracts_traded_per_unit4
        hold_pnl20long_per_contract4[i] = hold_pnl4_list[4] / contracts_traded_per_unit4

        hold_pnl1short4[i] = hold_pnl4_list[0] * quantity_short4
        hold_pnl2short4[i] = hold_pnl4_list[1] * quantity_short4
        hold_pnl5short4[i] = hold_pnl4_list[2] * quantity_short4
        hold_pnl10short4[i] = hold_pnl4_list[3] * quantity_short4
        hold_pnl20short4[i] = hold_pnl4_list[4] * quantity_short4

        hold_pnl1short_per_contract4[i] = -hold_pnl4_list[0] / contracts_traded_per_unit4
        hold_pnl2short_per_contract4[i] = -hold_pnl4_list[1] / contracts_traded_per_unit4
        hold_pnl5short_per_contract4[i] = -hold_pnl4_list[2] / contracts_traded_per_unit4
        hold_pnl10short_per_contract4[i] = -hold_pnl4_list[3] / contracts_traded_per_unit4
        hold_pnl20short_per_contract4[i] = -hold_pnl4_list[4] / contracts_traded_per_unit4

        #############################################

        hold_pnl1long5[i] = hold_pnl5_list[0] * quantity_long5
        hold_pnl2long5[i] = hold_pnl5_list[1] * quantity_long5
        hold_pnl5long5[i] = hold_pnl5_list[2] * quantity_long5
        hold_pnl10long5[i] = hold_pnl5_list[3] * quantity_long5
        hold_pnl20long5[i] = hold_pnl5_list[4] * quantity_long5

        hold_pnl1long_per_contract5[i] = hold_pnl5_list[0] / contracts_traded_per_unit5
        hold_pnl2long_per_contract5[i] = hold_pnl5_list[1] / contracts_traded_per_unit5
        hold_pnl5long_per_contract5[i] = hold_pnl5_list[2] / contracts_traded_per_unit5
        hold_pnl10long_per_contract5[i] = hold_pnl5_list[3] / contracts_traded_per_unit5
        hold_pnl20long_per_contract5[i] = hold_pnl5_list[4] / contracts_traded_per_unit5

        hold_pnl1short5[i] = hold_pnl5_list[0] * quantity_short5
        hold_pnl2short5[i] = hold_pnl5_list[1] * quantity_short5
        hold_pnl5short5[i] = hold_pnl5_list[2] * quantity_short5
        hold_pnl10short5[i] = hold_pnl5_list[3] * quantity_short5
        hold_pnl20short5[i] = hold_pnl5_list[4] * quantity_short5

        hold_pnl1short_per_contract5[i] = -hold_pnl5_list[0] / contracts_traded_per_unit5
        hold_pnl2short_per_contract5[i] = -hold_pnl5_list[1] / contracts_traded_per_unit5
        hold_pnl5short_per_contract5[i] = -hold_pnl5_list[2] / contracts_traded_per_unit5
        hold_pnl10short_per_contract5[i] = -hold_pnl5_list[3] / contracts_traded_per_unit5
        hold_pnl20short_per_contract5[i] = -hold_pnl5_list[4] / contracts_traded_per_unit5

        #############################################

        hold_pnl1long6[i] = hold_pnl6_list[0] * quantity_long6
        hold_pnl2long6[i] = hold_pnl6_list[1] * quantity_long6
        hold_pnl5long6[i] = hold_pnl6_list[2] * quantity_long6
        hold_pnl10long6[i] = hold_pnl6_list[3] * quantity_long6
        hold_pnl20long6[i] = hold_pnl6_list[4] * quantity_long6

        hold_pnl1long_per_contract6[i] = hold_pnl6_list[0] / contracts_traded_per_unit6
        hold_pnl2long_per_contract6[i] = hold_pnl6_list[1] / contracts_traded_per_unit6
        hold_pnl5long_per_contract6[i] = hold_pnl6_list[2] / contracts_traded_per_unit6
        hold_pnl10long_per_contract6[i] = hold_pnl6_list[3] / contracts_traded_per_unit6
        hold_pnl20long_per_contract6[i] = hold_pnl6_list[4] / contracts_traded_per_unit6

        hold_pnl1short6[i] = hold_pnl6_list[0] * quantity_short6
        hold_pnl2short6[i] = hold_pnl6_list[1] * quantity_short6
        hold_pnl5short6[i] = hold_pnl6_list[2] * quantity_short6
        hold_pnl10short6[i] = hold_pnl6_list[3] * quantity_short6
        hold_pnl20short6[i] = hold_pnl6_list[4] * quantity_short6

        hold_pnl1short_per_contract6[i] = -hold_pnl6_list[0] / contracts_traded_per_unit6
        hold_pnl2short_per_contract6[i] = -hold_pnl6_list[1] / contracts_traded_per_unit6
        hold_pnl5short_per_contract6[i] = -hold_pnl6_list[2] / contracts_traded_per_unit6
        hold_pnl10short_per_contract6[i] = -hold_pnl6_list[3] / contracts_traded_per_unit6
        hold_pnl20short_per_contract6[i] = -hold_pnl6_list[4] / contracts_traded_per_unit6

    strategy_sheet['holding_period5'] = holding_period5
    strategy_sheet['holding_period10'] = holding_period10
    strategy_sheet['holding_period15'] = holding_period15
    strategy_sheet['holding_period20'] = holding_period20
    strategy_sheet['holding_period25'] = holding_period25

    strategy_sheet['path_pnl5'] = path_pnl5
    strategy_sheet['path_pnl10'] = path_pnl10
    strategy_sheet['path_pnl15'] = path_pnl15
    strategy_sheet['path_pnl20'] = path_pnl20
    strategy_sheet['path_pnl25'] = path_pnl25

    strategy_sheet['path_pnl5_per_contract'] = path_pnl5_per_contract
    strategy_sheet['path_pnl10_per_contract'] = path_pnl10_per_contract
    strategy_sheet['path_pnl15_per_contract'] = path_pnl15_per_contract
    strategy_sheet['path_pnl20_per_contract'] = path_pnl20_per_contract
    strategy_sheet['path_pnl25_per_contract'] = path_pnl25_per_contract

    strategy_sheet['hold_pnl1long'] = hold_pnl1long
    strategy_sheet['hold_pnl2long'] = hold_pnl2long
    strategy_sheet['hold_pnl5long'] = hold_pnl5long
    strategy_sheet['hold_pnl10long'] = hold_pnl10long
    strategy_sheet['hold_pnl20long'] = hold_pnl20long

    strategy_sheet['hold_pnl1long_per_contract'] = hold_pnl1long_per_contract
    strategy_sheet['hold_pnl2long_per_contract'] = hold_pnl2long_per_contract
    strategy_sheet['hold_pnl5long_per_contract'] = hold_pnl5long_per_contract
    strategy_sheet['hold_pnl10long_per_contract'] = hold_pnl10long_per_contract
    strategy_sheet['hold_pnl20long_per_contract'] = hold_pnl20long_per_contract

    strategy_sheet['hold_pnl1short'] = hold_pnl1short
    strategy_sheet['hold_pnl2short'] = hold_pnl2short
    strategy_sheet['hold_pnl5short'] = hold_pnl5short
    strategy_sheet['hold_pnl10short'] = hold_pnl10short
    strategy_sheet['hold_pnl20short'] = hold_pnl20short

    strategy_sheet['hold_pnl1short_per_contract'] = hold_pnl1short_per_contract
    strategy_sheet['hold_pnl2short_per_contract'] = hold_pnl2short_per_contract
    strategy_sheet['hold_pnl5short_per_contract'] = hold_pnl5short_per_contract
    strategy_sheet['hold_pnl10short_per_contract'] = hold_pnl10short_per_contract
    strategy_sheet['hold_pnl20short_per_contract'] = hold_pnl20short_per_contract

    strategy_sheet['report_date'] = report_date

    strategy_sheet['hold_pnl1'] = [np.NAN]*num_trades
    strategy_sheet['hold_pnl2'] = [np.NAN]*num_trades
    strategy_sheet['hold_pnl5'] = [np.NAN]*num_trades
    strategy_sheet['hold_pnl10'] = [np.NAN]*num_trades
    strategy_sheet['hold_pnl20'] = [np.NAN]*num_trades

    strategy_sheet['hold_pnl1_per_contract'] = [np.NAN]*num_trades
    strategy_sheet['hold_pnl2_per_contract'] = [np.NAN]*num_trades
    strategy_sheet['hold_pnl5_per_contract'] = [np.NAN]*num_trades
    strategy_sheet['hold_pnl10_per_contract'] = [np.NAN]*num_trades
    strategy_sheet['hold_pnl20_per_contract'] = [np.NAN]*num_trades

    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl1'] = strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl1short']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl2'] = strategy_sheet.loc[strategy_sheet['QF'] > 50,'hold_pnl2short']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl5'] = strategy_sheet.loc[strategy_sheet['QF'] > 50,'hold_pnl5short']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl10'] = strategy_sheet.loc[strategy_sheet['QF'] > 50,'hold_pnl10short']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl20'] = strategy_sheet.loc[strategy_sheet['QF'] > 50,'hold_pnl20short']

    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl1'] = strategy_sheet.loc[strategy_sheet['QF'] < 50,'hold_pnl1long']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl2'] = strategy_sheet.loc[strategy_sheet['QF'] < 50,'hold_pnl2long']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl5'] = strategy_sheet.loc[strategy_sheet['QF'] < 50,'hold_pnl5long']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl10'] = strategy_sheet.loc[strategy_sheet['QF'] < 50,'hold_pnl10long']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl20'] = strategy_sheet.loc[strategy_sheet['QF'] < 50,'hold_pnl20long']

    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl1_per_contract'] = strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl1short_per_contract']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl2_per_contract'] = strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl2short_per_contract']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl5_per_contract'] = strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl5short_per_contract']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl10_per_contract'] = strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl10short_per_contract']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl20_per_contract'] = strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl20short_per_contract']

    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl1_per_contract'] = strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl1long_per_contract']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl2_per_contract'] = strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl2long_per_contract']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl5_per_contract'] = strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl5long_per_contract']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl10_per_contract'] = strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl10long_per_contract']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl20_per_contract'] = strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl20long_per_contract']

    ################################################

    strategy_sheet['hold_pnl1long12'] = hold_pnl1long12
    strategy_sheet['hold_pnl2long12'] = hold_pnl2long12
    strategy_sheet['hold_pnl5long12'] = hold_pnl5long12
    strategy_sheet['hold_pnl10long12'] = hold_pnl10long12
    strategy_sheet['hold_pnl20long12'] = hold_pnl20long12

    strategy_sheet['hold_pnl1long_per_contract12'] = hold_pnl1long_per_contract12
    strategy_sheet['hold_pnl2long_per_contract12'] = hold_pnl2long_per_contract12
    strategy_sheet['hold_pnl5long_per_contract12'] = hold_pnl5long_per_contract12
    strategy_sheet['hold_pnl10long_per_contract12'] = hold_pnl10long_per_contract12
    strategy_sheet['hold_pnl20long_per_contract12'] = hold_pnl20long_per_contract12

    strategy_sheet['hold_pnl1short12'] = hold_pnl1short12
    strategy_sheet['hold_pnl2short12'] = hold_pnl2short12
    strategy_sheet['hold_pnl5short12'] = hold_pnl5short12
    strategy_sheet['hold_pnl10short12'] = hold_pnl10short12
    strategy_sheet['hold_pnl20short12'] = hold_pnl20short12

    strategy_sheet['hold_pnl1short_per_contract12'] = hold_pnl1short_per_contract12
    strategy_sheet['hold_pnl2short_per_contract12'] = hold_pnl2short_per_contract12
    strategy_sheet['hold_pnl5short_per_contract12'] = hold_pnl5short_per_contract12
    strategy_sheet['hold_pnl10short_per_contract12'] = hold_pnl10short_per_contract12
    strategy_sheet['hold_pnl20short_per_contract12'] = hold_pnl20short_per_contract12

    strategy_sheet['hold_pnl1_12'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl2_12'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl5_12'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl10_12'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl20_12'] = [np.NAN] * num_trades

    strategy_sheet['hold_pnl1_per_contract12'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl2_per_contract12'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl5_per_contract12'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl10_per_contract12'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl20_per_contract12'] = [np.NAN] * num_trades

    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl1_12'] = strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl1short12']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl2_12'] = strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl2short12']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl5_12'] = strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl5short12']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl10_12'] = strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl10short12']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl20_12'] = strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl20short12']

    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl1_12'] = strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl1long12']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl2_12'] = strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl2long12']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl5_12'] = strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl5long12']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl10_12'] = strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl10long12']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl20_12'] = strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl20long12']

    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl1_per_contract12'] = strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl1short_per_contract12']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl2_per_contract12'] = strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl2short_per_contract12']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl5_per_contract12'] = strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl5short_per_contract12']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl10_per_contract12'] = strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl10short_per_contract12']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl20_per_contract12'] = strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl20short_per_contract12']

    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl1_per_contract12'] = strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl1long_per_contract12']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl2_per_contract12'] = strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl2long_per_contract12']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl5_per_contract12'] = strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl5long_per_contract12']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl10_per_contract12'] = strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl10long_per_contract12']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl20_per_contract12'] = strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl20long_per_contract12']

    ################################################

    strategy_sheet['hold_pnl1long3'] = hold_pnl1long3
    strategy_sheet['hold_pnl2long3'] = hold_pnl2long3
    strategy_sheet['hold_pnl5long3'] = hold_pnl5long3
    strategy_sheet['hold_pnl10long3'] = hold_pnl10long3
    strategy_sheet['hold_pnl20long3'] = hold_pnl20long3

    strategy_sheet['hold_pnl1long_per_contract3'] = hold_pnl1long_per_contract3
    strategy_sheet['hold_pnl2long_per_contract3'] = hold_pnl2long_per_contract3
    strategy_sheet['hold_pnl5long_per_contract3'] = hold_pnl5long_per_contract3
    strategy_sheet['hold_pnl10long_per_contract3'] = hold_pnl10long_per_contract3
    strategy_sheet['hold_pnl20long_per_contract3'] = hold_pnl20long_per_contract3

    strategy_sheet['hold_pnl1short3'] = hold_pnl1short3
    strategy_sheet['hold_pnl2short3'] = hold_pnl2short3
    strategy_sheet['hold_pnl5short3'] = hold_pnl5short3
    strategy_sheet['hold_pnl10short3'] = hold_pnl10short3
    strategy_sheet['hold_pnl20short3'] = hold_pnl20short3

    strategy_sheet['hold_pnl1short_per_contract3'] = hold_pnl1short_per_contract3
    strategy_sheet['hold_pnl2short_per_contract3'] = hold_pnl2short_per_contract3
    strategy_sheet['hold_pnl5short_per_contract3'] = hold_pnl5short_per_contract3
    strategy_sheet['hold_pnl10short_per_contract3'] = hold_pnl10short_per_contract3
    strategy_sheet['hold_pnl20short_per_contract3'] = hold_pnl20short_per_contract3

    strategy_sheet['hold_pnl1_3'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl2_3'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl5_3'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl10_3'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl20_3'] = [np.NAN] * num_trades

    strategy_sheet['hold_pnl1_per_contract3'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl2_per_contract3'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl5_per_contract3'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl10_per_contract3'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl20_per_contract3'] = [np.NAN] * num_trades

    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl1_3'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl1short3']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl2_3'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl2short3']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl5_3'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl5short3']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl10_3'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl10short3']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl20_3'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl20short3']

    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl1_3'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl1long3']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl2_3'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl2long3']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl5_3'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl5long3']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl10_3'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl10long3']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl20_3'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl20long3']

    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl1_per_contract3'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl1short_per_contract3']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl2_per_contract3'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl2short_per_contract3']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl5_per_contract3'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl5short_per_contract3']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl10_per_contract3'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl10short_per_contract3']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl20_per_contract3'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl20short_per_contract3']

    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl1_per_contract3'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl1long_per_contract3']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl2_per_contract3'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl2long_per_contract3']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl5_per_contract3'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl5long_per_contract3']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl10_per_contract3'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl10long_per_contract3']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl20_per_contract3'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl20long_per_contract3']

    ################################################

    strategy_sheet['hold_pnl1long32'] = hold_pnl1long32
    strategy_sheet['hold_pnl2long32'] = hold_pnl2long32
    strategy_sheet['hold_pnl5long32'] = hold_pnl5long32
    strategy_sheet['hold_pnl10long32'] = hold_pnl10long32
    strategy_sheet['hold_pnl20long32'] = hold_pnl20long32

    strategy_sheet['hold_pnl1long_per_contract32'] = hold_pnl1long_per_contract32
    strategy_sheet['hold_pnl2long_per_contract32'] = hold_pnl2long_per_contract32
    strategy_sheet['hold_pnl5long_per_contract32'] = hold_pnl5long_per_contract32
    strategy_sheet['hold_pnl10long_per_contract32'] = hold_pnl10long_per_contract32
    strategy_sheet['hold_pnl20long_per_contract32'] = hold_pnl20long_per_contract32

    strategy_sheet['hold_pnl1short32'] = hold_pnl1short32
    strategy_sheet['hold_pnl2short32'] = hold_pnl2short32
    strategy_sheet['hold_pnl5short32'] = hold_pnl5short32
    strategy_sheet['hold_pnl10short32'] = hold_pnl10short32
    strategy_sheet['hold_pnl20short32'] = hold_pnl20short32

    strategy_sheet['hold_pnl1short_per_contract32'] = hold_pnl1short_per_contract32
    strategy_sheet['hold_pnl2short_per_contract32'] = hold_pnl2short_per_contract32
    strategy_sheet['hold_pnl5short_per_contract32'] = hold_pnl5short_per_contract32
    strategy_sheet['hold_pnl10short_per_contract32'] = hold_pnl10short_per_contract32
    strategy_sheet['hold_pnl20short_per_contract32'] = hold_pnl20short_per_contract32

    strategy_sheet['hold_pnl1_32'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl2_32'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl5_32'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl10_32'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl20_32'] = [np.NAN] * num_trades

    strategy_sheet['hold_pnl1_per_contract32'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl2_per_contract32'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl5_per_contract32'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl10_per_contract32'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl20_per_contract32'] = [np.NAN] * num_trades

    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl1_32'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl1short32']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl2_32'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl2short32']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl5_32'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl5short32']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl10_32'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl10short32']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl20_32'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl20short32']

    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl1_32'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl1long32']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl2_32'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl2long32']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl5_32'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl5long32']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl10_32'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl10long32']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl20_32'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl20long32']

    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl1_per_contract32'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl1short_per_contract32']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl2_per_contract32'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl2short_per_contract32']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl5_per_contract32'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl5short_per_contract32']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl10_per_contract32'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl10short_per_contract32']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl20_per_contract32'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl20short_per_contract32']

    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl1_per_contract32'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl1long_per_contract32']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl2_per_contract32'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl2long_per_contract32']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl5_per_contract32'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl5long_per_contract32']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl10_per_contract32'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl10long_per_contract32']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl20_per_contract32'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl20long_per_contract32']


    ################################################

    strategy_sheet['hold_pnl1long4'] = hold_pnl1long4
    strategy_sheet['hold_pnl2long4'] = hold_pnl2long4
    strategy_sheet['hold_pnl5long4'] = hold_pnl5long4
    strategy_sheet['hold_pnl10long4'] = hold_pnl10long4
    strategy_sheet['hold_pnl20long4'] = hold_pnl20long4

    strategy_sheet['hold_pnl1long_per_contract4'] = hold_pnl1long_per_contract4
    strategy_sheet['hold_pnl2long_per_contract4'] = hold_pnl2long_per_contract4
    strategy_sheet['hold_pnl5long_per_contract4'] = hold_pnl5long_per_contract4
    strategy_sheet['hold_pnl10long_per_contract4'] = hold_pnl10long_per_contract4
    strategy_sheet['hold_pnl20long_per_contract4'] = hold_pnl20long_per_contract4

    strategy_sheet['hold_pnl1short4'] = hold_pnl1short4
    strategy_sheet['hold_pnl2short4'] = hold_pnl2short4
    strategy_sheet['hold_pnl5short4'] = hold_pnl5short4
    strategy_sheet['hold_pnl10short4'] = hold_pnl10short4
    strategy_sheet['hold_pnl20short4'] = hold_pnl20short4

    strategy_sheet['hold_pnl1short_per_contract4'] = hold_pnl1short_per_contract4
    strategy_sheet['hold_pnl2short_per_contract4'] = hold_pnl2short_per_contract4
    strategy_sheet['hold_pnl5short_per_contract4'] = hold_pnl5short_per_contract4
    strategy_sheet['hold_pnl10short_per_contract4'] = hold_pnl10short_per_contract4
    strategy_sheet['hold_pnl20short_per_contract4'] = hold_pnl20short_per_contract4

    strategy_sheet['hold_pnl1_4'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl2_4'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl5_4'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl10_4'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl20_4'] = [np.NAN] * num_trades

    strategy_sheet['hold_pnl1_per_contract4'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl2_per_contract4'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl5_per_contract4'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl10_per_contract4'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl20_per_contract4'] = [np.NAN] * num_trades

    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl1_4'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl1short4']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl2_4'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl2short4']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl5_4'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl5short4']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl10_4'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl10short4']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl20_4'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl20short4']

    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl1_4'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl1long4']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl2_4'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl2long4']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl5_4'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl5long4']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl10_4'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl10long4']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl20_4'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl20long4']

    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl1_per_contract4'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl1short_per_contract4']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl2_per_contract4'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl2short_per_contract4']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl5_per_contract4'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl5short_per_contract4']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl10_per_contract4'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl10short_per_contract4']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl20_per_contract4'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl20short_per_contract4']

    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl1_per_contract4'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl1long_per_contract4']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl2_per_contract4'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl2long_per_contract4']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl5_per_contract4'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl5long_per_contract4']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl10_per_contract4'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl10long_per_contract4']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl20_per_contract4'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl20long_per_contract4']

    ################################################

    strategy_sheet['hold_pnl1long5'] = hold_pnl1long5
    strategy_sheet['hold_pnl2long5'] = hold_pnl2long5
    strategy_sheet['hold_pnl5long5'] = hold_pnl5long5
    strategy_sheet['hold_pnl10long5'] = hold_pnl10long5
    strategy_sheet['hold_pnl20long5'] = hold_pnl20long5

    strategy_sheet['hold_pnl1long_per_contract5'] = hold_pnl1long_per_contract5
    strategy_sheet['hold_pnl2long_per_contract5'] = hold_pnl2long_per_contract5
    strategy_sheet['hold_pnl5long_per_contract5'] = hold_pnl5long_per_contract5
    strategy_sheet['hold_pnl10long_per_contract5'] = hold_pnl10long_per_contract5
    strategy_sheet['hold_pnl20long_per_contract5'] = hold_pnl20long_per_contract5

    strategy_sheet['hold_pnl1short5'] = hold_pnl1short5
    strategy_sheet['hold_pnl2short5'] = hold_pnl2short5
    strategy_sheet['hold_pnl5short5'] = hold_pnl5short5
    strategy_sheet['hold_pnl10short5'] = hold_pnl10short5
    strategy_sheet['hold_pnl20short5'] = hold_pnl20short5

    strategy_sheet['hold_pnl1short_per_contract5'] = hold_pnl1short_per_contract5
    strategy_sheet['hold_pnl2short_per_contract5'] = hold_pnl2short_per_contract5
    strategy_sheet['hold_pnl5short_per_contract5'] = hold_pnl5short_per_contract5
    strategy_sheet['hold_pnl10short_per_contract5'] = hold_pnl10short_per_contract5
    strategy_sheet['hold_pnl20short_per_contract5'] = hold_pnl20short_per_contract5

    strategy_sheet['hold_pnl1_5'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl2_5'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl5_5'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl10_5'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl20_5'] = [np.NAN] * num_trades

    strategy_sheet['hold_pnl1_per_contract5'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl2_per_contract5'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl5_per_contract5'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl10_per_contract5'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl20_per_contract5'] = [np.NAN] * num_trades

    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl1_5'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl1short5']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl2_5'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl2short5']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl5_5'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl5short5']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl10_5'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl10short5']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl20_5'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl20short5']

    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl1_5'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl1long5']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl2_5'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl2long5']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl5_5'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl5long5']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl10_5'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl10long5']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl20_5'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl20long5']

    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl1_per_contract5'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl1short_per_contract5']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl2_per_contract5'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl2short_per_contract5']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl5_per_contract5'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl5short_per_contract5']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl10_per_contract5'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl10short_per_contract5']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl20_per_contract5'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl20short_per_contract5']

    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl1_per_contract5'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl1long_per_contract5']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl2_per_contract5'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl2long_per_contract5']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl5_per_contract5'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl5long_per_contract5']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl10_per_contract5'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl10long_per_contract5']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl20_per_contract5'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl20long_per_contract5']

    ################################################

    strategy_sheet['hold_pnl1long6'] = hold_pnl1long6
    strategy_sheet['hold_pnl2long6'] = hold_pnl2long6
    strategy_sheet['hold_pnl5long6'] = hold_pnl5long6
    strategy_sheet['hold_pnl10long6'] = hold_pnl10long6
    strategy_sheet['hold_pnl20long6'] = hold_pnl20long6

    strategy_sheet['hold_pnl1long_per_contract6'] = hold_pnl1long_per_contract6
    strategy_sheet['hold_pnl2long_per_contract6'] = hold_pnl2long_per_contract6
    strategy_sheet['hold_pnl5long_per_contract6'] = hold_pnl5long_per_contract6
    strategy_sheet['hold_pnl10long_per_contract6'] = hold_pnl10long_per_contract6
    strategy_sheet['hold_pnl20long_per_contract6'] = hold_pnl20long_per_contract6

    strategy_sheet['hold_pnl1short6'] = hold_pnl1short6
    strategy_sheet['hold_pnl2short6'] = hold_pnl2short6
    strategy_sheet['hold_pnl5short6'] = hold_pnl5short6
    strategy_sheet['hold_pnl10short6'] = hold_pnl10short6
    strategy_sheet['hold_pnl20short6'] = hold_pnl20short6

    strategy_sheet['hold_pnl1short_per_contract6'] = hold_pnl1short_per_contract6
    strategy_sheet['hold_pnl2short_per_contract6'] = hold_pnl2short_per_contract6
    strategy_sheet['hold_pnl5short_per_contract6'] = hold_pnl5short_per_contract6
    strategy_sheet['hold_pnl10short_per_contract6'] = hold_pnl10short_per_contract6
    strategy_sheet['hold_pnl20short_per_contract6'] = hold_pnl20short_per_contract6

    strategy_sheet['hold_pnl1_6'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl2_6'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl5_6'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl10_6'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl20_6'] = [np.NAN] * num_trades

    strategy_sheet['hold_pnl1_per_contract6'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl2_per_contract6'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl5_per_contract6'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl10_per_contract6'] = [np.NAN] * num_trades
    strategy_sheet['hold_pnl20_per_contract6'] = [np.NAN] * num_trades

    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl1_6'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl1short6']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl2_6'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl2short6']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl5_6'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl5short6']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl10_6'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl10short6']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl20_6'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl20short6']

    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl1_6'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl1long6']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl2_6'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl2long6']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl5_6'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl5long6']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl10_6'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl10long6']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl20_6'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl20long6']

    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl1_per_contract6'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl1short_per_contract6']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl2_per_contract6'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl2short_per_contract6']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl5_per_contract6'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl5short_per_contract6']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl10_per_contract6'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl10short_per_contract6']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl20_per_contract6'] = strategy_sheet.loc[
        strategy_sheet['QF'] > 50, 'hold_pnl20short_per_contract6']

    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl1_per_contract6'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl1long_per_contract6']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl2_per_contract6'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl2long_per_contract6']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl5_per_contract6'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl5long_per_contract6']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl10_per_contract6'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl10long_per_contract6']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl20_per_contract6'] = strategy_sheet.loc[
        strategy_sheet['QF'] < 50, 'hold_pnl20long_per_contract6']

    strategy_sheet.to_pickle(output_dir + '/backtest_results.pkl')

    return strategy_sheet


def get_backtest_summary(**kwargs):

    futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in cmi.futures_butterfly_strategy_tickerhead_list}
    date_list = kwargs['date_list']

    if 'use_existing_filesQ' in kwargs.keys():
        use_existing_filesQ = kwargs['use_existing_filesQ']
    else:
        use_existing_filesQ = True

    backtest_output = []

    for report_date in date_list:
        backtest_output.append(get_backtest_summary_4_date(report_date=report_date,
                                                                futures_data_dictionary=futures_data_dictionary,
                                                                use_existing_filesQ=use_existing_filesQ))

    return {'big_data' : pd.concat(backtest_output), 'backtest_output': backtest_output}




