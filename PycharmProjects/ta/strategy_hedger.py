
import ta.strategy as tas
import shared.calendar_utilities as cu
import my_sql_routines.my_sql_utilities as msu
import contract_utilities.expiration as exp
import get_price.get_options_price as gop
import option_models.utils as omu
import pandas as pd
import numpy as np
import math as m
import ta.get_intraday_prices as gip
import contract_utilities.contract_meta_info as cmi
import ta.trade_fill_loader as tfl
import datetime as dt
pd.options.mode.chained_assignment = None  # default='warn'
import shared.converters as sc

flat_curve_ticker_class_list = ['Index', 'FX', 'Metal', 'Treasury']

def get_hedge_4strategy(**kwargs):

    con = msu.get_my_sql_connection(**kwargs)
    current_date = cu.get_doubledate()
    settle_price_date = exp.doubledate_shift_bus_days(double_date=current_date, shift_in_days=1)

    position_frame = tas.get_net_position_4strategy_alias(alias=kwargs['alias'],as_of_date=current_date,con=con)

    if 'intraday_price_frame' in kwargs.keys():
        intraday_price_frame = kwargs['intraday_price_frame']
    else:
        intraday_price_frame = gip.get_cme_direct_prices()
        intraday_price_frame.rename(columns={'ticker': 'underlying_ticker'},inplace=True)

        intraday_price_frame['ticker_head'] = [cmi.get_contract_specs(x)['ticker_head'] for x in intraday_price_frame['underlying_ticker']]
        intraday_price_frame['mid_price'] = (intraday_price_frame['bid_price'] + intraday_price_frame['ask_price'])/2

        intraday_price_frame['mid_price'] = [tfl.convert_trade_price_from_cme_direct(ticker_head=intraday_price_frame['ticker_head'].iloc[x],
                                            price=intraday_price_frame['mid_price'].iloc[x]) for x in range(len(intraday_price_frame.index))]

    options_frame = position_frame[position_frame['instrument'] == 'O']
    futures_frame = position_frame[position_frame['instrument'] == 'F']

    if options_frame.empty:
        futures_frame.rename(columns={'ticker': 'underlying_ticker', 'qty': 'underlying_delta'},inplace=True)
        futures_frame = futures_frame[['underlying_ticker', 'underlying_delta']]
        net_position = pd.merge(futures_frame, intraday_price_frame, how='left', on='underlying_ticker')
        net_position['hedge_price'] = net_position['mid_price']
        net_position['hedge'] = -net_position['underlying_delta']
        return net_position

    imp_vol_list = [gop.get_options_price_from_db(ticker=options_frame['ticker'].iloc[x],
                                  settle_date=settle_price_date,
                                  strike=options_frame['strike_price'].iloc[x],
                                  column_names=['imp_vol'],
                                  con=con)['imp_vol'] for x in range(len(options_frame.index))]

    options_frame['imp_vol'] = [imp_vol_list[x][1] if (np.isnan(imp_vol_list[x][0]) and len(imp_vol_list[x]) > 1) else imp_vol_list[x][0]
                                for x in range(len(options_frame.index))]

    options_frame['underlying_ticker'] = [omu.get_option_underlying(ticker=x) for x in options_frame['ticker']]
    #print(intraday_price_frame)

    options_frame = pd.merge(options_frame, intraday_price_frame, how='left', on='underlying_ticker')

    options_frame['ticker_head'] = [cmi.get_contract_specs(x)['ticker_head'] for x in options_frame['ticker']]
    options_frame['exercise_type'] = [cmi.get_option_exercise_type(ticker_head=x) for x in options_frame['ticker_head']]
    options_frame['strike_price'] = options_frame['strike_price'].astype('float64')

    options_frame['delta'] = [omu.option_model_wrapper(ticker=options_frame['ticker'].iloc[x],
                             calculation_date=current_date,
                             interest_rate_date=settle_price_date,
                             underlying=options_frame['mid_price'].iloc[x],
                             strike=options_frame['strike_price'].iloc[x],
                             implied_vol=options_frame['imp_vol'].iloc[x],
                             option_type=options_frame['option_type'].iloc[x],
                             exercise_type=options_frame['exercise_type'].iloc[x],
                             con=con)['delta'] for x in range(len(options_frame.index))]

    options_frame['total_delta'] = options_frame['qty']*options_frame['delta']

    grouped = options_frame.groupby('underlying_ticker')

    net_position = pd.DataFrame()

    net_position['underlying_ticker'] = (grouped['underlying_ticker'].first()).values
    net_position['hedge_price'] = (grouped['mid_price'].first()).values
    net_position['option_delta'] = (grouped['total_delta'].sum()).values
    net_position['option_delta'] = net_position['option_delta'].round(2)

    if futures_frame.empty:
        net_position['total_delta'] = net_position['option_delta']
    else:
        futures_frame.rename(columns={'ticker': 'underlying_ticker', 'qty': 'underlying_delta'},inplace=True)
        futures_frame = futures_frame[['underlying_ticker', 'underlying_delta']]

        isinOptions = futures_frame['underlying_ticker'].isin(net_position['underlying_ticker'])
        futures_frame_w_options = futures_frame[isinOptions]
        futures_frame_wo_options = futures_frame[~isinOptions]

        if futures_frame_w_options.empty:
            net_position['underlying_delta'] = 0
            net_position['total_delta'] = net_position['option_delta']
        else:
            net_position = pd.merge(net_position, futures_frame_w_options, how='outer', on='underlying_ticker')
            net_position[['underlying_delta']] = net_position[['underlying_delta']].fillna(value=0,inplace=False)
            net_position['total_delta'] = net_position['option_delta']+net_position['underlying_delta']

        if not futures_frame_wo_options.empty:
            net_position_futures = pd.merge(futures_frame_wo_options, intraday_price_frame, how='left', on='underlying_ticker')
            net_position_futures['hedge_price'] = net_position_futures['mid_price']
            net_position_futures['option_delta'] = 0
            net_position_futures['total_delta'] = net_position_futures['underlying_delta']
            net_position = pd.concat([net_position,net_position_futures[['underlying_ticker','hedge_price','option_delta','underlying_delta','total_delta']]])

    net_position['hedge'] = -net_position['total_delta']

    if 'con' not in kwargs.keys():
        con.close()

    return net_position

def hedge_strategy_against_delta(**kwargs):

    con = msu.get_my_sql_connection(**kwargs)
    print(kwargs['alias'])

    if 'intraday_price_frame' in kwargs.keys():
        hedge_results = get_hedge_4strategy(alias=kwargs['alias'], intraday_price_frame=kwargs['intraday_price_frame'], con=con)
    else:
        hedge_results = get_hedge_4strategy(alias=kwargs['alias'], con=con)

    trade_frame = pd.DataFrame()
    trade_frame['ticker'] = hedge_results['underlying_ticker']
    trade_frame['option_type'] = None
    trade_frame['strike_price'] = np.NaN
    trade_frame['trade_price'] = hedge_results['hedge_price']
    trade_frame['trade_quantity'] = hedge_results['hedge']
    trade_frame['instrument'] = 'F'
    trade_frame['real_tradeQ'] = True
    trade_frame['alias'] = kwargs['alias']

    tas.load_trades_2strategy(trade_frame=trade_frame,con=con)

    trade_frame['trade_quantity'] = -trade_frame['trade_quantity']
    trade_frame['alias'] = kwargs['delta_alias']
    tas.load_trades_2strategy(trade_frame=trade_frame,con=con)

    if 'con' not in kwargs.keys():
        con.close()


def strategy_hedge_report(**kwargs):

    current_date = cu.get_doubledate()

    con = msu.get_my_sql_connection(**kwargs)

    strategy_frame = tas.get_open_strategies(as_of_date=current_date,con=con)

    strategy_class_list = [sc.convert_from_string_to_dictionary(string_input=strategy_frame['description_string'][x])['strategy_class']
                           for x in range(len(strategy_frame.index))]

    hedge_indx = [x in ['vcs', 'scv','optionInventory'] for x in strategy_class_list]
    hedge_frame = strategy_frame[hedge_indx]

    #hedge_frame = hedge_frame[(hedge_frame['alias'] == 'BPX2018_short_scv')]
    #hedge_frame = hedge_frame[(hedge_frame['alias'] != 'CLZ17H18VCS')]

    if 'intraday_price_frame' in kwargs.keys():
        [hedge_strategy_against_delta(alias=x, intraday_price_frame=kwargs['intraday_price_frame'], delta_alias=kwargs['delta_alias'], con=con) for x in hedge_frame['alias']]
    else:
        [hedge_strategy_against_delta(alias=x, delta_alias=kwargs['delta_alias'], con=con) for x in hedge_frame['alias']]

    if 'con' not in kwargs.keys():
        con.close()


def get_delta_strategy_alias(**kwargs):

    # inputs:
    # as_of_date

    strategy_frame = tas.get_open_strategies(**kwargs)
    strategy_frame.sort_values('open_date', ascending=True, inplace=True)

    strategy_class_list = [sc.convert_from_string_to_dictionary(string_input=strategy_frame['description_string'].iloc[x])['strategy_class'] for x in range(len(strategy_frame.index))]

    if ('delta' not in strategy_class_list):
        datetime_now = dt.datetime.now()
        delta_strategy_alias = 'delta_' + datetime_now.strftime('%b%y')
        generate_strategy_output = tas.generate_db_strategy_from_alias(alias=delta_strategy_alias, description_string='strategy_class=delta')
        delta_alias_final =  generate_strategy_output['alias']
    else:
        strategy_frame['class'] = strategy_class_list
        delta_frame = strategy_frame[strategy_frame['class'] == 'delta']
        delta_alias_final = delta_frame['alias'].iloc[-1]



    return delta_alias_final

def get_intraday_data_contract_frame(**kwargs):

    current_date = cu.get_doubledate()
    con = msu.get_my_sql_connection(**kwargs)

    strategy_frame = tas.get_open_strategies(as_of_date=current_date, con=con)

    strategy_class_list = [sc.convert_from_string_to_dictionary(string_input=strategy_frame['description_string'][x])['strategy_class'] for x in range(len(strategy_frame.index))]

    hedge_indx = [x in ['vcs', 'scv', 'optionInventory'] for x in strategy_class_list]
    hedge_frame = strategy_frame[hedge_indx]

    options_frame_list = []

    for i in range(len(hedge_frame.index)):

        position_frame = tas.get_net_position_4strategy_alias(alias=hedge_frame['alias'].iloc[i], as_of_date=current_date, con=con)
        options_frame = position_frame[position_frame['instrument'] == 'O']
        options_frame['underlying_ticker'] = [omu.get_option_underlying(ticker=x) for x in options_frame['ticker']]
        options_frame_list.append(options_frame)

    merged_frame = pd.concat(options_frame_list)
    underlying_ticker_list = list(merged_frame['underlying_ticker'].unique())

    delta_alias = get_delta_strategy_alias(con=con)
    delta_position_frame = tas.get_net_position_4strategy_alias(alias=delta_alias, as_of_date=current_date, con=con)

    contract_frame = pd.DataFrame()
    contract_frame['ticker'] = list(set(delta_position_frame['ticker'].unique()) | set(underlying_ticker_list))

    contract_specs_output_list = [cmi.get_contract_specs(x) for x in contract_frame['ticker']]

    contract_frame['ticker_head'] = [x['ticker_head'] for x in contract_specs_output_list]
    contract_frame['ticker_class'] = [x['ticker_class'] for x in contract_specs_output_list]

    contract_frame['cont_indx'] = [x['cont_indx'] for x in contract_specs_output_list]

    contract_frame['is_spread_q'] = False

    contract_frame.sort_values(['ticker_head', 'cont_indx'], ascending=[True, True], inplace=True)
    contract_frame.reset_index(drop=True, inplace=True)

    non_flat_frame = contract_frame[~contract_frame['ticker_class'].isin(flat_curve_ticker_class_list)]

    unique_ticker_head_list = non_flat_frame['ticker_head'].unique()

    for i in range(len(unique_ticker_head_list)):
        ticker_head_frame = non_flat_frame[non_flat_frame['ticker_head'] == unique_ticker_head_list[i]]
        if len(ticker_head_frame.index) > 1:
            for j in range(len(ticker_head_frame.index) - 1):
                for k in range(j + 1, len(ticker_head_frame.index)):
                    spread_ticker = ticker_head_frame['ticker'].iloc[j] + '-' + ticker_head_frame['ticker'].iloc[k]
                    contract_frame.loc[len(contract_frame.index)] = [spread_ticker,
                                                                                     ticker_head_frame[
                                                                                         'ticker_head'].iloc[j],
                                                                                     ticker_head_frame[
                                                                                         'ticker_class'].iloc[j],
                                                                                     ticker_head_frame[
                                                                                         'cont_indx'].iloc[j], True]

    if 'con' not in kwargs.keys():
        con.close()

    return contract_frame

def calc_hedge_quantity(**kwargs):
    qty = kwargs['qty']
    return -np.sign(qty) * m.floor(abs(qty))



def get_hedge_frame(**kwargs):

    con = msu.get_my_sql_connection(**kwargs)
    delta_alias = get_delta_strategy_alias(con=con)
    current_date = cu.get_doubledate()

    delta_position_frame = tas.get_net_position_4strategy_alias(alias=delta_alias, as_of_date=current_date, con=con)

    contract_specs_output_list = [cmi.get_contract_specs(x) for x in delta_position_frame['ticker']]

    delta_position_frame['ticker_head'] = [x['ticker_head'] for x in contract_specs_output_list]
    delta_position_frame['ticker_class'] = [x['ticker_class'] for x in contract_specs_output_list]
    delta_position_frame['cont_indx'] = [x['cont_indx'] for x in contract_specs_output_list]

    #delta_position_frame['qty'].iloc[0]  = 2
    #delta_position_frame['qty'].iloc[1] =  -1
    #delta_position_frame['qty'].iloc[2] = 1

    non_flat_frame = delta_position_frame[~delta_position_frame['ticker_class'].isin(flat_curve_ticker_class_list)]

    unique_ticker_head_list = non_flat_frame['ticker_head'].unique()

    non_flat_curve_hedge_frame = pd.DataFrame(columns=['ticker', 'ticker_head', 'is_spread_q', 'is_expiration_roll_q', 'tr_days_2_roll', 'hedge'])

    for i in range(len(unique_ticker_head_list)):
        ticker_head_frame = non_flat_frame[non_flat_frame['ticker_head'] == unique_ticker_head_list[i]]
        ticker_head_frame.reset_index(drop=True, inplace=True)

        if len(ticker_head_frame.index) == 1:
            non_flat_curve_hedge_frame.loc[len(non_flat_curve_hedge_frame.index)] = \
                [ticker_head_frame['ticker'].iloc[0], ticker_head_frame['ticker_head'].iloc[0],
                 False, False, 100, calc_hedge_quantity(qty=ticker_head_frame['qty'].iloc[0])]
            continue


        raw_sum = ticker_head_frame['qty'].sum()
        outright_hedge = calc_hedge_quantity(qty=raw_sum)

        max_indx = ticker_head_frame['qty'].idxmax()
        min_indx = ticker_head_frame['qty'].idxmin()

        if (outright_hedge > 0):
            non_flat_curve_hedge_frame.loc[len(non_flat_curve_hedge_frame.index)] = \
                [ticker_head_frame['ticker'].loc[min_indx], ticker_head_frame['ticker_head'].loc[min_indx],
                 False, False, 100, outright_hedge]
            ticker_head_frame['qty'].loc[min_indx] = ticker_head_frame['qty'].loc[min_indx] + outright_hedge

        if (outright_hedge < 0):
            non_flat_curve_hedge_frame.loc[len(non_flat_curve_hedge_frame.index)] = \
                [ticker_head_frame['ticker'].loc[max_indx], ticker_head_frame['ticker_head'].loc[max_indx],
                 False, False, 100, outright_hedge]
            ticker_head_frame['qty'].loc[max_indx] = ticker_head_frame['qty'].loc[max_indx] + outright_hedge

        position_cleaned_q = False

        while (not position_cleaned_q):
            max_indx2 = ticker_head_frame['qty'].idxmax()
            min_indx2 = ticker_head_frame['qty'].idxmin()

            position_cleaned_q = not ((ticker_head_frame['qty'].loc[max_indx2] >= 1) and
                                      (ticker_head_frame['qty'].loc[min_indx2] <= -1))

            if position_cleaned_q:
                break

            spread_hedge = calc_hedge_quantity(qty=min(ticker_head_frame['qty'].loc[max_indx2],
                                                           -ticker_head_frame['qty'].loc[min_indx2]))

            if ticker_head_frame['cont_indx'].loc[max_indx2] < ticker_head_frame['cont_indx'].loc[min_indx2]:
                spread_ticker = ticker_head_frame['ticker'].loc[max_indx2] + '-' + ticker_head_frame['ticker'].loc[
                    min_indx2]
                hedge_qty = spread_hedge
            else:
                spread_ticker = ticker_head_frame['ticker'].loc[min_indx2] + '-' + ticker_head_frame['ticker'].loc[
                    max_indx2]
                hedge_qty = -spread_hedge

            non_flat_curve_hedge_frame.loc[len(non_flat_curve_hedge_frame.index)] = \
                [spread_ticker, unique_ticker_head_list[i], True, False, 100, hedge_qty]
            ticker_head_frame['qty'].loc[max_indx2] = ticker_head_frame['qty'].loc[max_indx2] + spread_hedge
            ticker_head_frame['qty'].loc[min_indx2] = ticker_head_frame['qty'].loc[min_indx2] - spread_hedge

    if 'con' not in kwargs.keys():
        con.close()


    return non_flat_curve_hedge_frame


















