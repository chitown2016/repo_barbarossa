
import ta.strategy as ts
import contract_utilities.contract_meta_info as cmi
import stock_utilities.stock_meta_info as smi
import get_price.get_futures_price as gfp
import get_price.get_stock_price as gsp
import get_price.get_options_price as gop
import contract_utilities.expiration as exp
import shared.calendar_utilities as cu
import shared.converters as sc
import my_sql_routines.my_sql_utilities as msu
import pandas as pd
import numpy as np
import datetime as dt
pd.options.mode.chained_assignment = None  # default='warn'

def get_stock_strategy_pnl_4day(**kwargs):

    alias = kwargs['alias']
    pnl_date = kwargs['pnl_date']

    if 'shift_in_days' in kwargs.keys():
        shift_in_days = kwargs['shift_in_days']
    else:
        shift_in_days = 1

    pnl_datetime = cu.convert_doubledate_2datetime(pnl_date)

    con = msu.get_my_sql_connection(**kwargs)

    if 'trades_frame' in kwargs.keys():
        trades_frame = kwargs['trades_frame']
    else:
        trades_frame = ts.get_trades_4strategy_alias(alias=alias,con=con)
        trades_frame['t_cost'] = [smi.get_ib_t_cost(price=trades_frame['trade_price'].iloc[x],quantity=trades_frame['trade_quantity'].iloc[x]) for x in range(len(trades_frame.index))]

    position_frame_aux = trades_frame[trades_frame['trade_date'] < pnl_datetime]
    intraday_frame_aux = trades_frame[trades_frame['trade_date'] == pnl_datetime]

    grouped = position_frame_aux.groupby(['ticker'])
    net_position = pd.DataFrame()
    net_position['qty'] = grouped['trade_quantity'].sum()
    net_position['ticker'] = grouped['ticker'].first()
    net_position = net_position[abs(net_position['qty']) > 0.1]

    useful_generalized_ticker_list = list(set(net_position['ticker'].values) | set(intraday_frame_aux['ticker'].unique()))
    trades_frame = trades_frame[trades_frame['ticker'].isin(useful_generalized_ticker_list)]

    if 'stock_data_dictionary' in kwargs.keys():
        stock_data_dictionary = kwargs['stock_data_dictionary']
    else:
        unique_ticker_list = trades_frame['ticker'].unique()
        stock_data_dictionary = {x: gsp.get_stock_price_preloaded(ticker=x) for x in unique_ticker_list}

    pnl_date_1 = exp.doubledate_shift_bus_days(double_date=pnl_date, shift_in_days=shift_in_days)

    stock_price_out_1 = [gsp.get_stock_price_preloaded(ticker=x,
                                                           stock_data_dictionary=stock_data_dictionary,
                                                           settle_date=pnl_date_1) for x in trades_frame['ticker']]

    stock_price_out = [gsp.get_stock_price_preloaded(ticker=x,
                                                         stock_data_dictionary=stock_data_dictionary,
                                                         settle_date=pnl_date) for x in trades_frame['ticker']]

    trades_frame['price_1'] = [np.NaN if x.empty else x['close'].values[0] for x in stock_price_out_1]
    trades_frame['price'] = [np.NaN if x.empty else x['close'].values[0] for x in stock_price_out]

    position_frame = trades_frame[trades_frame['trade_date'] < pnl_datetime]

    nan_price_q = position_frame['price'].isnull().values.any()

    intraday_frame = trades_frame[trades_frame['trade_date'] == pnl_datetime]

    position_pnl_per_ticker = pd.DataFrame(columns=['ticker', 'pnl_position'])
    intraday_pnl_per_ticker = pd.DataFrame(columns=['ticker', 'pnl_intraday'])

    if len(position_frame) == 0:
        position_pnl = 0
    else:
        position_frame['pnl'] = position_frame['trade_quantity'] * \
                                (position_frame['price'] - position_frame['price_1'])
        position_pnl = position_frame['pnl'].sum()

        position_grouped_per_ticker = position_frame.groupby('ticker')
        position_pnl_per_ticker['pnl_position'] = (position_grouped_per_ticker['pnl'].sum()).values
        position_pnl_per_ticker['ticker'] = (position_grouped_per_ticker['ticker'].first()).values

    if len(intraday_frame) == 0:
        intraday_pnl = 0
        t_cost = 0
    else:
        intraday_frame['pnl'] = intraday_frame['trade_quantity'] * \
                                (intraday_frame['price'] - intraday_frame['trade_price'])
        intraday_frame['pnl_wtcost'] = intraday_frame['pnl'] - intraday_frame['t_cost']
        intraday_pnl = intraday_frame['pnl'].sum()
        t_cost = intraday_frame['t_cost'].sum()

        intraday_grouped_per_ticker = intraday_frame.groupby('ticker')
        intraday_pnl_per_ticker['pnl_intraday'] = (intraday_grouped_per_ticker['pnl_wtcost'].sum()).values
        intraday_pnl_per_ticker['ticker'] = (intraday_grouped_per_ticker['ticker'].first()).values

    pnl_per_ticker = pd.merge(position_pnl_per_ticker, intraday_pnl_per_ticker, how='outer', on='ticker')
    intraday_zero_indx = [x not in intraday_pnl_per_ticker['ticker'].values for x in pnl_per_ticker['ticker']]
    position_zero_indx = [x not in position_pnl_per_ticker['ticker'].values for x in pnl_per_ticker['ticker']]
    pnl_per_ticker['pnl_position'][position_zero_indx] = 0
    pnl_per_ticker['pnl_intraday'][intraday_zero_indx] = 0
    pnl_per_ticker['pnl_total'] = pnl_per_ticker['pnl_position'] + pnl_per_ticker['pnl_intraday']
    pnl_per_ticker.set_index('ticker', drop=True, inplace=True)

    if 'con' not in kwargs.keys():
        con.close()

    return {'total_pnl': int(position_pnl + intraday_pnl - t_cost),
            'position_pnl': int(position_pnl),
            'intraday_pnl': int(intraday_pnl),
            't_cost': int(t_cost),
            'nan_price_q': nan_price_q,
            'pnl_per_ticker': pnl_per_ticker}



def get_strategy_pnl_4day(**kwargs):

    alias = kwargs['alias']
    pnl_date = kwargs['pnl_date']

    if 'shift_in_days' in kwargs.keys():
        shift_in_days = kwargs['shift_in_days']
    else:
        shift_in_days = 1

    if 'broker' in kwargs.keys():
        broker = kwargs['broker']
    else:
        broker = 'abn'

    #print(pnl_date)

    pnl_datetime = cu.convert_doubledate_2datetime(pnl_date)

    con = msu.get_my_sql_connection(**kwargs)

    if 'trades_frame' in kwargs.keys():
        trades_frame = kwargs['trades_frame']
        ticker_head_list = [cmi.get_contract_specs(x)['ticker_head'] for x in trades_frame['ticker']]
    else:
        trades_frame = ts.get_trades_4strategy_alias(alias=alias,con=con)
        ticker_head_list = [cmi.get_contract_specs(x)['ticker_head'] for x in trades_frame['ticker']]
        trades_frame['contract_multiplier'] = [cmi.contract_multiplier[x] for x in ticker_head_list]
        trades_frame['t_cost'] = [cmi.get_t_cost(ticker_head=x,broker=broker) for x in ticker_head_list]

    trades_frame['ticker_head'] = ticker_head_list

    option_indx = trades_frame['instrument'] == 'O'
    trades_frame['generalized_ticker'] = trades_frame['ticker']
    trades_frame['generalized_ticker'][option_indx] = trades_frame['ticker'][option_indx] + '-' + \
                                                         trades_frame['option_type'][option_indx] + '-' + \
                                                         trades_frame['strike_price'][option_indx].astype(str)

    position_frame_aux = trades_frame[trades_frame['trade_date'] < pnl_datetime]
    intraday_frame_aux = trades_frame[trades_frame['trade_date'] == pnl_datetime]

    grouped = position_frame_aux.groupby(['generalized_ticker'])
    net_position = pd.DataFrame()
    net_position['qty'] = grouped['trade_quantity'].sum()
    net_position['generalized_ticker'] = grouped['generalized_ticker'].first()
    net_position = net_position[abs(net_position['qty'])>0.1]

    useful_generalized_ticker_list = list(set(net_position['generalized_ticker'].values) | set(intraday_frame_aux['generalized_ticker'].unique()))
    trades_frame = trades_frame[trades_frame['generalized_ticker'].isin(useful_generalized_ticker_list)]

    if 'futures_data_dictionary' in kwargs.keys():
        futures_data_dictionary = kwargs['futures_data_dictionary']
    else:
        unique_ticker_head_list = list(set(ticker_head_list))
        futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in unique_ticker_head_list}

    pnl_date_1 = exp.doubledate_shift_bus_days(double_date=pnl_date,shift_in_days=shift_in_days)

    underlying_frame = trades_frame[trades_frame['instrument'] == 'F']
    option_frame = trades_frame[trades_frame['instrument'] == 'O']

    futures_price_out_1 = [gfp.get_futures_price_preloaded(ticker=x,
                                futures_data_dictionary=futures_data_dictionary,
                                settle_date=pnl_date_1) for x in underlying_frame['ticker']]

    futures_price_out = [gfp.get_futures_price_preloaded(ticker=x,
                                futures_data_dictionary=futures_data_dictionary,
                                settle_date=pnl_date) for x in underlying_frame['ticker']]

    underlying_frame['price_1'] = [np.NaN if x.empty else x['close_price'].values[0] for x in futures_price_out_1]

    underlying_frame['price'] = [np.NaN if x.empty else x['close_price'].values[0] for x in futures_price_out]

    #underlying_frame['price_1'] = [gfp.get_futures_price_preloaded(ticker=x,
    #                            futures_data_dictionary=futures_data_dictionary,
    #                            settle_date=pnl_date_1)['close_price'].values[0] for x in underlying_frame['ticker']]

    #underlying_frame['price'] = [gfp.get_futures_price_preloaded(ticker=x,
    #                            futures_data_dictionary=futures_data_dictionary,
    #                            settle_date=pnl_date)['close_price'].values[0] for x in underlying_frame['ticker']]

    option_frame['price_1'] = [gop.get_options_price_from_db(ticker=option_frame['ticker'].iloc[x],
                                                             strike=option_frame['strike_price'].iloc[x],
                                                             option_type=option_frame['option_type'].iloc[x],
                                                             con=con,settle_date=pnl_date_1,
                                                             return_nan_if_emptyQ = True)['close_price'].values[0] for x in range(len(option_frame.index))]

    option_frame['price'] = [gop.get_options_price_from_db(ticker=option_frame['ticker'].iloc[x],
                                                             strike=option_frame['strike_price'].iloc[x],
                                                             option_type=option_frame['option_type'].iloc[x],
                                                             con=con,settle_date=pnl_date,
                                                             return_nan_if_emptyQ = True)['close_price'].values[0] for x in range(len(option_frame.index))]

    trades_frame = pd.concat([option_frame, underlying_frame])

    position_frame = trades_frame[trades_frame['trade_date'] < pnl_datetime]

    nan_price_q = position_frame['price'].isnull().values.any()

    intraday_frame = trades_frame[trades_frame['trade_date'] == pnl_datetime]

    position_pnl_per_ticker = pd.DataFrame(columns=['ticker','pnl_position'])
    intraday_pnl_per_ticker = pd.DataFrame(columns=['ticker','pnl_intraday'])

    position_pnl_per_tickerhead = pd.DataFrame(columns=['ticker_head','pnl_position'])
    intraday_pnl_per_tickerhead = pd.DataFrame(columns=['ticker_head','pnl_intraday'])

    if len(position_frame) == 0:
        position_pnl = 0
    else:
        position_frame['pnl'] = position_frame['contract_multiplier']*\
                                position_frame['trade_quantity']*\
                                (position_frame['price']-position_frame['price_1'])
        position_pnl = position_frame['pnl'].sum()

        position_grouped_per_ticker = position_frame.groupby('ticker')
        position_grouped_per_tickerhead = position_frame.groupby('ticker_head')
        position_pnl_per_ticker['pnl_position'] = (position_grouped_per_ticker['pnl'].sum()).values
        position_pnl_per_ticker['ticker'] = (position_grouped_per_ticker['ticker'].first()).values

        position_pnl_per_tickerhead['pnl_position'] = (position_grouped_per_tickerhead['pnl'].sum()).values
        position_pnl_per_tickerhead['ticker_head'] = (position_grouped_per_tickerhead['ticker_head'].first()).values

    if len(intraday_frame) == 0:
        intraday_pnl = 0
        t_cost = 0
    else:
        intraday_frame['pnl'] = intraday_frame['contract_multiplier']*\
                                intraday_frame['trade_quantity']*\
                                (intraday_frame['price']-intraday_frame['trade_price'])
        intraday_frame['pnl_wtcost'] = intraday_frame['pnl']-abs(intraday_frame['trade_quantity']*intraday_frame['t_cost'])
        intraday_pnl = intraday_frame['pnl'].sum()
        t_cost = (abs(intraday_frame['trade_quantity']*intraday_frame['t_cost'])).sum()

        intraday_grouped_per_ticker = intraday_frame.groupby('ticker')
        intraday_grouped_per_tickerhead = intraday_frame.groupby('ticker_head')
        intraday_pnl_per_ticker['pnl_intraday'] = (intraday_grouped_per_ticker['pnl_wtcost'].sum()).values
        intraday_pnl_per_ticker['ticker'] = (intraday_grouped_per_ticker['ticker'].first()).values

        intraday_pnl_per_tickerhead['pnl_intraday'] = (intraday_grouped_per_tickerhead['pnl_wtcost'].sum()).values
        intraday_pnl_per_tickerhead['ticker_head'] = (intraday_grouped_per_tickerhead['ticker_head'].first()).values

    pnl_per_ticker = pd.merge(position_pnl_per_ticker,intraday_pnl_per_ticker,how='outer',on='ticker')
    intraday_zero_indx = [x not in intraday_pnl_per_ticker['ticker'].values for x in pnl_per_ticker['ticker']]
    position_zero_indx = [x not in position_pnl_per_ticker['ticker'].values for x in pnl_per_ticker['ticker']]
    pnl_per_ticker['pnl_position'][position_zero_indx] = 0
    pnl_per_ticker['pnl_intraday'][intraday_zero_indx] = 0
    pnl_per_ticker['pnl_total'] = pnl_per_ticker['pnl_position']+pnl_per_ticker['pnl_intraday']
    pnl_per_ticker.set_index('ticker', drop=True, inplace=True)

    pnl_per_tickerhead = pd.merge(position_pnl_per_tickerhead,intraday_pnl_per_tickerhead,how='outer',on='ticker_head')
    intraday_zero_indx = [x not in intraday_pnl_per_tickerhead['ticker_head'].values for x in pnl_per_tickerhead['ticker_head']]
    position_zero_indx = [x not in position_pnl_per_tickerhead['ticker_head'].values for x in pnl_per_tickerhead['ticker_head']]
    pnl_per_tickerhead['pnl_position'][position_zero_indx] = 0
    pnl_per_tickerhead['pnl_intraday'][intraday_zero_indx] = 0
    pnl_per_tickerhead['pnl_total'] = pnl_per_tickerhead['pnl_position']+pnl_per_tickerhead['pnl_intraday']
    pnl_per_tickerhead.set_index('ticker_head', drop=True, inplace=True)

    if 'con' not in kwargs.keys():
        con.close()

    return {'total_pnl': int(position_pnl+intraday_pnl - t_cost),
            'position_pnl': int(position_pnl),
            'intraday_pnl': int(intraday_pnl),
            't_cost': int(t_cost),
            'nan_price_q':nan_price_q,
            'pnl_per_ticker': pnl_per_ticker,
            'pnl_per_tickerhead':pnl_per_tickerhead}


def get_strategy_pnl(**kwargs):

    alias = kwargs['alias']
    con = msu.get_my_sql_connection(**kwargs)
    #print(alias)

    strategy_info = ts.get_strategy_info_from_alias(alias=alias, con=con)

    if 'as_of_date' in kwargs.keys():
        as_of_date = kwargs['as_of_date']
    else:
        as_of_date = exp.doubledate_shift_bus_days()

    if 'broker' in kwargs.keys():
        broker = kwargs['broker']
    else:
        broker = 'abn'

    open_date = int(strategy_info['open_date'].strftime('%Y%m%d'))
    #open_date = 20160920
    close_date = int(strategy_info['close_date'].strftime('%Y%m%d'))

    if close_date>as_of_date:
        close_date = as_of_date

    bus_day_list = exp.get_bus_day_list(date_from=open_date,date_to=close_date)

    trades_frame = ts.get_trades_4strategy_alias(alias=alias,con=con)

    if sum(trades_frame['instrument']=='S')>0:
        stock_strategy_Q = True
    else:
        stock_strategy_Q = False

    if stock_strategy_Q:

        return  {'pnl_frame': pd.DataFrame(), 'daily_pnl': np.nan, 'total_pnl': np.nan}

        unique_ticker_list = trades_frame['ticker'].unique()
        stock_data_dictionary = {x: gsp.get_stock_price_preloaded(ticker=x) for x in unique_ticker_list}

        trades_frame['t_cost'] = [smi.get_ib_t_cost(price=trades_frame['trade_price'].iloc[x],quantity=trades_frame['trade_quantity'].iloc[x]) for x in range(len(trades_frame.index))]
        pnl_path = [get_stock_strategy_pnl_4day(alias=alias, pnl_date=x, con=con, trades_frame=trades_frame, stock_data_dictionary=stock_data_dictionary) for x in bus_day_list]

        nan_price_q_list = [x['nan_price_q'] for x in pnl_path]
        good_price_q_list = [not i for i in nan_price_q_list]

        bus_day_after_nan_list = [bus_day_list[x + 1] for x in range(len(bus_day_list) - 1) if nan_price_q_list[x]]

        pnl_path = [pnl_path[x] for x in range(len(pnl_path)) if good_price_q_list[x]]
        bus_day_list = [bus_day_list[x] for x in range(len(bus_day_list)) if good_price_q_list[x]]

        # print(bus_day_list)
        # print(bus_day_after_nan_list)

        if len(bus_day_after_nan_list) > 0:
            pnl_path_after_nan = [get_stock_strategy_pnl_4day(alias=alias, pnl_date=x, con=con,
                                                        trades_frame=trades_frame, broker=broker,
                                                        shift_in_days=2,
                                                              stock_data_dictionary=stock_data_dictionary) for x in
                                  bus_day_after_nan_list]
            for i in range(len(bus_day_after_nan_list)):

                if bus_day_after_nan_list[i] in bus_day_list:
                    index_val = bus_day_list.index(bus_day_after_nan_list[i])
                    pnl_path[index_val] = pnl_path_after_nan[i]

        pnl_frame = pd.DataFrame(pnl_path)
        pnl_frame['settle_date'] = bus_day_list

        output_dictionary = {
            'pnl_frame': pnl_frame[['settle_date', 'position_pnl', 'intraday_pnl', 't_cost', 'total_pnl']],
            'daily_pnl': pnl_frame['total_pnl'].values[-1],
            'total_pnl': pnl_frame['total_pnl'].sum()}


    else:

        ticker_head_list = [cmi.get_contract_specs(x)['ticker_head'] for x in trades_frame['ticker']]
        unique_ticker_head_list = list(set(ticker_head_list))

        if 'futures_data_dictionary' in kwargs.keys():
            futures_data_dictionary = kwargs['futures_data_dictionary']
        else:
            futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in unique_ticker_head_list}

        trades_frame['contract_multiplier'] = [cmi.contract_multiplier[x] for x in ticker_head_list]
        trades_frame['t_cost'] = [cmi.get_t_cost(ticker_head=x,broker=broker) for x in ticker_head_list]

        pnl_path = [get_strategy_pnl_4day(alias=alias,pnl_date=x,con=con,
                                          trades_frame=trades_frame,broker=broker,
                                          futures_data_dictionary=futures_data_dictionary) for x in bus_day_list]

        nan_price_q_list = [x['nan_price_q'] for x in pnl_path]
        good_price_q_list = [not i for i in nan_price_q_list]

        bus_day_after_nan_list = [bus_day_list[x+1] for x in range(len(bus_day_list)-1) if nan_price_q_list[x] and not nan_price_q_list[x+1]]

        pnl_path = [pnl_path[x] for x in range(len(pnl_path)) if good_price_q_list[x]]
        bus_day_list = [bus_day_list[x] for x in range(len(bus_day_list)) if good_price_q_list[x]]

        #print(bus_day_list)
        #print(bus_day_after_nan_list)

        if len(bus_day_after_nan_list) > 0:
             pnl_path_after_nan = [get_strategy_pnl_4day(alias=alias,pnl_date=x,con=con,
                                          trades_frame=trades_frame,broker=broker,
                                                         shift_in_days=2,
                                          futures_data_dictionary=futures_data_dictionary) for x in bus_day_after_nan_list]
             for i in range(len(bus_day_after_nan_list)):
                 index_val = bus_day_list.index(bus_day_after_nan_list[i])
                 pnl_path[index_val] = pnl_path_after_nan[i]

        pnl_per_tickerhead_list = [x['pnl_per_tickerhead'] for x in pnl_path]

        pnl_per_tickerhead = pd.concat(pnl_per_tickerhead_list, axis=1, sort=True)
        pnl_per_tickerhead = pnl_per_tickerhead[['pnl_total']]
        pnl_per_tickerhead = pnl_per_tickerhead.transpose()

        if len(unique_ticker_head_list)>1:
            zero_indx = [[x not in y.index for y in pnl_per_tickerhead_list] for x in pnl_per_tickerhead.columns]

            for i in range(len(pnl_per_tickerhead.columns)):
                pnl_per_tickerhead.iloc[:, i][zero_indx[i]] = 0

        pnl_per_tickerhead['settle_date'] = bus_day_list
        pnl_per_tickerhead.reset_index(inplace=True,drop=True)

        pnl_frame = pd.DataFrame(pnl_path)
        pnl_frame['settle_date'] = bus_day_list

        daily_index = pnl_frame['settle_date'] == as_of_date

        if sum(daily_index) == 0:
            daily_pnl = np.nan
        else:
            daily_pnl = pnl_frame['total_pnl'].values[-1]

        output_dictionary = {'pnl_frame': pnl_frame[['settle_date','position_pnl','intraday_pnl','t_cost','total_pnl']],
            'pnl_per_tickerhead': pnl_per_tickerhead,
            'daily_pnl': daily_pnl,
            'total_pnl': pnl_frame['total_pnl'].sum()}

    if 'con' not in kwargs.keys():
        con.close()

    return output_dictionary


def close_strategy(**kwargs):

    alias = kwargs['alias']
    broker = kwargs['broker']

    if 'close_date' in kwargs.keys():
        close_date = kwargs['close_date']
    else:
        close_date = exp.doubledate_shift_bus_days()

    now_time = dt.datetime.now()
    now_date = now_time.date()

    con = msu.get_my_sql_connection(**kwargs)

    net_position = ts.get_net_position_4strategy_alias(alias=alias, con=con, as_of_date=close_date)

    if net_position.empty:

        pnl_output = get_strategy_pnl(alias=alias, broker=broker, con=con, as_of_date=close_date)
        total_pnl = pnl_output['total_pnl']
        cur = con.cursor()

        query_str = 'UPDATE strategy SET pnl=' + str(total_pnl) + \
                  ', close_date=' + str(close_date) + \
                  ', last_updated_date=' + now_date.strftime('%Y%m%d') + \
                  ' WHERE alias=\'' + alias + '\''
        cur.execute(query_str)
        con.commit()

    else:
        print(alias + ' is not empty ! ')
        return

    if 'con' not in kwargs.keys():
        con.close()

