
import ta.strategy as ts
import ta.pnl as tapnl
import contract_utilities.expiration as exp
import my_sql_routines.my_sql_utilities as msu
import pandas as pd
import shared.directory_names as dn
import os.path

pd.options.mode.chained_assignment = None  # default='warn'


def get_daily_pnl_snapshot(**kwargs):

    if 'as_of_date' not in kwargs.keys():
        as_of_date = exp.doubledate_shift_bus_days()
        kwargs['as_of_date'] = as_of_date
    else:
        as_of_date = kwargs['as_of_date']

    ta_output_dir = dn.get_dated_directory_extension(folder_date=as_of_date,ext='ta')

    file_name = '/portfolio_pnl_' + kwargs['name'] + '.pkl'

    if os.path.isfile(ta_output_dir + file_name):
        strategy_frame = pd.read_pickle(ta_output_dir + file_name)
        return strategy_frame

    strategy_frame = ts.get_open_strategies(**kwargs)
    pnl_output = [tapnl.get_strategy_pnl(alias=x,**kwargs) for x in strategy_frame['alias']]

    strategy_frame['daily_pnl'] = [x['daily_pnl'] for x in pnl_output]
    strategy_frame['total_pnl'] = [x['total_pnl'] for x in pnl_output]

    strategy_frame = strategy_frame[['alias','daily_pnl','total_pnl']]
    strategy_frame.sort_values('daily_pnl',ascending=False,inplace=True)

    if len(strategy_frame.index)>0:
        strategy_frame.loc[max(strategy_frame.index)+1] = ['TOTAL', strategy_frame['daily_pnl'].sum(), strategy_frame['total_pnl'].sum()]

    strategy_frame.to_pickle(ta_output_dir + file_name)

    return strategy_frame


def get_trades_4portfolio(**kwargs):

    con = msu.get_my_sql_connection(**kwargs)

    if 'trade_date_to' in kwargs.keys():
        filter_string = 'WHERE trade_date<=' + str(kwargs['trade_date_to'])
    else:
        filter_string = ''

    sql_query = 'SELECT ticker, trade_quantity, instrument, option_type, strike_price from trades ' + filter_string
    cur = con.cursor()
    cur.execute(sql_query)
    data = cur.fetchall()

    if 'con' not in kwargs.keys():
        con.close()

    trade_frame = pd.DataFrame(data, columns=['ticker', 'trade_quantity', 'instrument', 'option_type', 'strike_price'])
    return trade_frame


def get_position_4portfolio(**kwargs):

    trades_frame = get_trades_4portfolio(**kwargs)

    trades_frame['generalized_ticker'] = trades_frame['ticker']
    option_indx = trades_frame['instrument']=='O'

    trades_frame['generalized_ticker'][option_indx] = trades_frame['ticker'][option_indx] + '-' + \
                                                         trades_frame['option_type'][option_indx] + '-' + \
                                                         trades_frame['strike_price'][option_indx].astype(str)


    grouped = trades_frame.groupby('generalized_ticker')

    net_position = pd.DataFrame()

    net_position['generalized_ticker'] = (grouped['generalized_ticker'].first()).values
    net_position['ticker'] = (grouped['ticker'].first()).values
    net_position['option_type'] = (grouped['option_type'].first()).values
    net_position['strike_price'] = (grouped['strike_price'].first()).values
    net_position['instrument'] = (grouped['instrument'].first()).values
    net_position['qty'] = (grouped['trade_quantity'].sum()).values
    net_position = net_position[net_position['qty'] != 0]

    return net_position

