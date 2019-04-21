__author__ = 'kocat_000'

import shared.directory_names as dn
import shared.calendar_utilities as cu
import os.path
import pandas as pd
import numpy as np
import shared.converters as conv
import datetime as dt
import my_sql_routines.my_sql_utilities as msu
import contract_utilities.expiration as exp
import get_price.get_futures_price as gfp
import get_price.get_options_price as gop
import time as tm
pd.options.mode.chained_assignment = None

def generate_db_strategy_from_strategy_sheet(**kwargs):

    id = kwargs['id']
    strategy_class = kwargs['strategy_class']

    strategy_output = load_strategy_file(**kwargs)

    del strategy_output['z5']
    del strategy_output['z6']
    del strategy_output['z7']

    strategy = strategy_output.loc[id]

    if strategy_class == 'futures_butterfly':
        if strategy['Q'] <= 49:
            strategy_alias = strategy['tickerHead'] + strategy['ticker1'][-5:] + strategy['ticker2'][-5:] + \
                             strategy['ticker2'][-5:] + strategy['ticker3'][-5:]
        elif strategy['Q'] >= 51:
            strategy_alias = strategy['tickerHead'] + strategy['ticker2'][-5:] + strategy['ticker3'][-5:] + \
                       strategy['ticker1'][-5:] + strategy['ticker2'][-5:]

        strategy = strategy[['ticker1','ticker2','ticker3','trDte1','trDte2','trDte3','agg','cBack','Q','QF','z1']]

    description_string = 'strategy_class=' + strategy_class + '&' + \
                          conv.convert_from_dictionary_to_string(dictionary_input=strategy)

    return generate_db_strategy_from_alias(alias=strategy_alias, description_string=description_string)


def get_net_position_4strategy_alias(**kwargs):

    alias = kwargs['alias']

    if 'as_of_date' in kwargs.keys():
        as_of_date = kwargs['as_of_date']
    else:
        as_of_date = exp.doubledate_shift_bus_days()

    con = msu.get_my_sql_connection(**kwargs)
    trades_frame = get_trades_4strategy_alias(alias=alias,con=con)

    as_of_datetime = cu.convert_doubledate_2datetime(as_of_date)

    trades_frame = trades_frame[trades_frame['trade_date'] <= as_of_datetime]

    trades_frame['full_ticker'] = [trades_frame['ticker'].iloc[x] if trades_frame['instrument'].iloc[x] in ['F','S'] else
                                   trades_frame['ticker'].iloc[x] + '_' + trades_frame['option_type'].iloc[x] + str(trades_frame['strike_price'].iloc[x])
                                   for x in range(len(trades_frame.index))]

    grouped = trades_frame.groupby('full_ticker')

    net_position = pd.DataFrame()

    net_position['ticker'] = (grouped['ticker'].first()).values
    net_position['option_type'] = (grouped['option_type'].first()).values
    net_position['strike_price'] = (grouped['strike_price'].first()).values
    net_position['instrument'] = (grouped['instrument'].first()).values
    net_position['qty'] = (grouped['trade_quantity'].sum()).values

    net_position['qty'] = net_position['qty'].round(2)

    if 'con' not in kwargs.keys():
        con.close()

    return net_position[net_position['qty'] != 0]


def generate_db_strategy_from_alias(**kwargs):

    alias = kwargs['alias']
    description_string = kwargs['description_string']

    con = msu.get_my_sql_connection(**kwargs)
    cur = con.cursor()

    for i in range(1, 50):

        if i > 1:
            alias_modified = alias + '_' + str(i)
        else:
            alias_modified = alias

        strategy_id = get_strategy_id_from_alias(alias=alias_modified, con=con)

        if not strategy_id:
            break

    now_date = dt.datetime.now().date()

    column_str = "alias, open_date, close_date, created_date, last_updated_date, description_string"
    insert_str = ("%s, " * 6)[:-2]

    tuple_to_load = (alias_modified, now_date, cu.convert_doubledate_2datetime(30000101),
                     now_date, now_date, description_string)

    final_str = "INSERT INTO strategy (%s) VALUES (%s)" % (column_str, insert_str)
    cur.execute(final_str,tuple_to_load)
    con.commit()

    strategy_id = get_strategy_id_from_alias(alias=alias_modified, con=con)

    if 'con' not in kwargs.keys():
        con.close()

    return {'alias': alias_modified, 'strategy_id': strategy_id}


def get_trades_4strategy_alias(**kwargs):

    con = msu.get_my_sql_connection(**kwargs)

    sql_query = 'SELECT tr.id, tr.ticker, tr.option_type, tr.strike_price, tr.trade_price, tr.trade_quantity, tr.trade_date, tr.instrument, tr.real_tradeQ ' + \
                 'FROM strategy as str INNER JOIN trades as tr ON tr.strategy_id=str.id ' + \
                 'WHERE str.alias=\'' + kwargs['alias'] + '\''
    cur = con.cursor()
    cur.execute(sql_query)
    data = cur.fetchall()

    if 'con' not in kwargs.keys():
        con.close()

    trade_frame = pd.DataFrame(data, columns=['id', 'ticker', 'option_type', 'strike_price', 'trade_price', 'trade_quantity', 'trade_date', 'instrument', 'real_tradeQ'])
    trade_frame['trade_price'] = [float(x) if x is not None else float('NaN') for x in trade_frame['trade_price'].values]
    trade_frame['trade_quantity'] = trade_frame['trade_quantity'].astype('float64')
    trade_frame['strike_price'] = trade_frame['strike_price'].astype('float64')
    return trade_frame


def get_strategy_id_from_alias(**kwargs):
    alias = kwargs['alias']
    con = msu.get_my_sql_connection(**kwargs)
    cur = con.cursor()

    query_string = 'SELECT id FROM strategy WHERE alias=\'' + alias + '\''
    cur.execute(query_string)
    data = cur.fetchall()

    if not data:
        strategy_id = None
    else:
        strategy_id = data[0][0]

    if 'con' not in kwargs.keys():
        con.close()

    return strategy_id


def get_strategy_info_from_alias(**kwargs):
    alias = kwargs['alias']
    con = msu.get_my_sql_connection(**kwargs)
    cur = con.cursor()

    query_string = 'SELECT * FROM strategy WHERE alias=\'' + alias + '\''
    cur.execute(query_string)
    data = cur.fetchall()

    if 'con' not in kwargs.keys():
        con.close()

    return {'id': data[0][0],
            'alias': data[0][1],
            'open_date': data[0][2],
            'close_date': data[0][3],
            'pnl': data[0][4],
            'created_date': data[0][5],
            'last_updated_date': data[0][6],
            'description_string': data[0][7]}


def load_trades_2strategy(**kwargs):

    trade_frame = kwargs['trade_frame']
    con = msu.get_my_sql_connection(**kwargs)

    trade_frame['strategy_id'] = [get_strategy_id_from_alias(alias=trade_frame['alias'][x],con=con) for x in range(len(trade_frame.index))]
    trade_frame['strike_price'] = trade_frame['strike_price'].astype('float64')

    now_time = dt.datetime.now()
    now_date = now_time.date()

    if 'trade_date' in kwargs.keys():
        trade_date = cu.convert_doubledate_2datetime(kwargs['trade_date'])
    else:
        trade_date = now_date

    column_str = "ticker, option_type, strike_price, strategy_id, trade_price, trade_quantity, trade_date, instrument, real_tradeQ, created_date, last_updated_date"
    insert_str = ("%s, " * 11)[:-2]

    final_str = "INSERT INTO trades (%s) VALUES (%s)" % (column_str, insert_str)

    column_names = trade_frame.columns.tolist()

    ticker_indx = column_names.index('ticker')
    option_type_indx = column_names.index('option_type')
    strike_price_indx = column_names.index('strike_price')
    trade_price_indx = column_names.index('trade_price')
    trade_quantity_indx = column_names.index('trade_quantity')
    instrument_indx = column_names.index('instrument')
    real_tradeQ_indx = column_names.index('real_tradeQ')
    strategy_id_indx = column_names.index('strategy_id')

    tuples = [tuple([x[ticker_indx],
                     None if pd.isnull(x[option_type_indx]) else x[option_type_indx],
                     None if pd.isnull(x[strike_price_indx]) else x[strike_price_indx],
                      x[strategy_id_indx],x[trade_price_indx], x[trade_quantity_indx],
              trade_date,x[instrument_indx], x[real_tradeQ_indx],now_time,now_time]) for x in trade_frame.values]

    msu.sql_execute_many_wrapper(final_str=final_str, tuples=tuples, con=con)

    if 'con' not in kwargs.keys():
        con.close()


def move_position_from_strategy_2_strategy(**kwargs):

    strategy_from = kwargs['strategy_from']
    strategy_to = kwargs['strategy_to']

    now_time = dt.datetime.now()

    con = msu.get_my_sql_connection(**kwargs)

    if 'con' not in kwargs.keys():
        kwargs['con'] = con

    if 'as_of_date' in kwargs.keys():
        as_of_date = kwargs['as_of_date']
    else:
        as_of_date = exp.doubledate_shift_bus_days()
        kwargs['as_of_date'] = as_of_date

    net_position_frame = get_net_position_4strategy_alias(alias=strategy_from, **kwargs)

    target_strategy_id = get_strategy_id_from_alias(alias=strategy_to, **kwargs)
    source_strategy_id = get_strategy_id_from_alias(alias=strategy_from, **kwargs)

    futures_position_frame = net_position_frame[net_position_frame['instrument'] == 'F']
    options_position_frame = net_position_frame[net_position_frame['instrument'] == 'O']

    futures_position_frame['trade_price'] = \
        [float(gfp.get_futures_price_4ticker(ticker=x,date_from=as_of_date,date_to=as_of_date,con=con)['close_price'][0]) for x in futures_position_frame['ticker']]

    if not options_position_frame.empty:
        options_position_frame['trade_price'] = options_position_frame.apply(lambda row: gop.get_options_price_from_db(ticker=row['ticker'],
                                                                    strike=row['strike_price'],
                                                                    option_type=row['option_type'],con=con,
                                                                    return_nan_if_emptyQ=True,
                                                                    settle_date=as_of_date)['close_price'][0], axis=1)

        net_position_frame = pd.concat([futures_position_frame,options_position_frame])
    else:
        net_position_frame = futures_position_frame

    column_names = net_position_frame.columns.tolist()

    ticker_indx = column_names.index('ticker')
    option_type_indx = column_names.index('option_type')
    strike_price_indx = column_names.index('strike_price')
    trade_price_indx = column_names.index('trade_price')
    trade_quantity_indx = column_names.index('qty')
    instrument_indx = column_names.index('instrument')

    column_str = "ticker, option_type, strike_price, strategy_id, trade_price, trade_quantity, trade_date, instrument, real_tradeQ, created_date, last_updated_date"
    insert_str = ("%s, " * 11)[:-2]

    final_str = "INSERT INTO trades (%s) VALUES (%s)" % (column_str, insert_str)

    tuples_target = [tuple([x[ticker_indx],x[option_type_indx],
                            None if np.isnan(x[strike_price_indx]) else x[strike_price_indx], target_strategy_id,
              x[trade_price_indx], x[trade_quantity_indx],
              as_of_date,x[instrument_indx], True,now_time,now_time]) for x in net_position_frame.values]

    msu.sql_execute_many_wrapper(final_str=final_str, tuples=tuples_target, con=con)

    tuples_source = [tuple([x[ticker_indx],x[option_type_indx],
                            None if np.isnan(x[strike_price_indx]) else x[strike_price_indx], source_strategy_id,
              x[trade_price_indx], -x[trade_quantity_indx],
              as_of_date,x[instrument_indx], True,now_time,now_time]) for x in net_position_frame.values]

    msu.sql_execute_many_wrapper(final_str=final_str, tuples=tuples_source, con=con)

    if 'con' not in kwargs.keys():
        con.close()


def get_open_strategies(**kwargs):

    con = msu.get_my_sql_connection(**kwargs)

    if 'as_of_date' in kwargs.keys():
        as_of_date = kwargs['as_of_date']
    else:
        as_of_date = int(tm.strftime('%Y%m%d'))

    cur = con.cursor()

    sql_query = 'SELECT * FROM futures_master.strategy WHERE close_date>=' + str(as_of_date) + ' and open_date<=' + str(as_of_date)

    cur.execute(sql_query)
    data = cur.fetchall()

    if 'con' not in kwargs.keys():
        con.close()

    return pd.DataFrame(data,columns=['id','alias','open_date','close_date','pnl','created_date','last_updated_date','description_string'])

def get_filtered_open_strategies(**kwargs):


    open_strategy_frame = get_open_strategies(**kwargs)
    open_strategy_frame['strategy_class'] = [conv.convert_from_string_to_dictionary(string_input=x)['strategy_class'] for
                                             x in open_strategy_frame['description_string']]

    return open_strategy_frame[open_strategy_frame['strategy_class'].isin(kwargs['strategy_class_list'])]


def select_strategies(**kwargs):

    con = msu.get_my_sql_connection(**kwargs)

    sql_query = 'SELECT * FROM futures_master.strategy WHERE '

    if 'open_date_from' in kwargs.keys():
        open_date_from = kwargs['open_date_from']
    else:
        open_date_from = 20171001

    sql_query = sql_query + ' open_date>=' + str(open_date_from)

    if 'open_date_to' in kwargs.keys():
        sql_query = sql_query + ' and open_date<=' + str(kwargs['open_date_to'])

    cur = con.cursor()

    cur.execute(sql_query)
    data = cur.fetchall()

    if 'con' not in kwargs.keys():
        con.close()

    return pd.DataFrame(data,columns=['id','alias','open_date','close_date','pnl','created_date','last_updated_date','description_string'])


def load_strategy_file(**kwargs):

    strategy_class = kwargs['strategy_class']
    report_date = kwargs['report_date']

    if strategy_class == 'futures_butterfly':
        output_dir = create_strategy_output_dir(strategy_class='futures_butterfly', report_date=report_date)
        strategy_output = pd.read_pickle(output_dir + '/summary.pkl')

    return strategy_output


def create_strategy_output_dir(**kwargs):

    strategy_class = kwargs['strategy_class']
    report_date = kwargs['report_date']

    strategy_output_folder = dn.get_directory_name(ext='strategy_output')

    if strategy_class == 'futures_butterfly':
        output_dir = strategy_output_folder + '/futures_butterfly/' + cu.get_directory_extension(report_date)
    elif strategy_class == 'intraday_futures_experimental':
         output_dir = strategy_output_folder + '/ife/' + cu.get_directory_extension(report_date)
    else:
        output_dir = strategy_output_folder + '/' + strategy_class + '/' + cu.get_directory_extension(report_date)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    return output_dir


