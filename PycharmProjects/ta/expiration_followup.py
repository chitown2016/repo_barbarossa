
import ta.portfolio_manager as tpm
import ta.strategy as ts
import contract_utilities.expiration as exp
import my_sql_routines.my_sql_utilities as msu
import pandas as pd


def get_portfolio_expirations(**kwargs):

    con = msu.get_my_sql_connection(**kwargs)

    position_frame = tpm.get_position_4portfolio(trade_date_to=kwargs['report_date'])
    position_frame.reset_index(drop=True, inplace=True)
    futures_indx = position_frame['instrument'] == 'F'
    position_frame.loc[futures_indx,'instrument'] = 'futures'
    position_frame.loc[~futures_indx,'instrument'] = 'options'
    position_frame['tr_days_2roll'] = position_frame.apply(lambda x: exp.get_days2_roll(ticker=x['ticker'],
                                                                                   instrument=x['instrument'],
                                                                                   date_to=kwargs['report_date'],con=con)['tr_days_2roll'],axis=1)

    position_frame['alias'] = 'Portfolio'

    if 'con' not in kwargs.keys():
        con.close()

    return position_frame


def get_strategy_expiration(**kwargs):

    position_frame = ts.get_net_position_4strategy_alias(**kwargs)
    if position_frame.empty:
        return pd.DataFrame()

    con = msu.get_my_sql_connection(**kwargs)

    position_frame.reset_index(drop=True, inplace=True)
    futures_indx = position_frame['instrument'] == 'F'
    position_frame.loc[futures_indx,'instrument'] = 'futures'
    position_frame.loc[~futures_indx,'instrument'] = 'options'
    position_frame['tr_days_2roll'] = position_frame.apply(lambda x: exp.get_days2_roll(ticker=x['ticker'],
                                                                                   instrument=x['instrument'],
                                                                                   date_to=kwargs['as_of_date'],con=con)['tr_days_2roll'],axis=1)
    position_frame['alias'] = kwargs['alias']

    if 'con' not in kwargs.keys():
        con.close()

    return position_frame


def get_expiration_report(**kwargs):

    con = msu.get_my_sql_connection(**kwargs)
    portfolio_frame = get_portfolio_expirations(**kwargs)

    strategy_frame = ts.get_open_strategies(con=con,as_of_date=kwargs['report_date'])

    expiration_list = [get_strategy_expiration(con=con,alias=strategy_frame['alias'].iloc[x],as_of_date=kwargs['report_date']) for x in range(len(strategy_frame.index))]
    expiration_list.append(portfolio_frame)

    if 'con' not in kwargs.keys():
        con.close()

    expiration_list = [x for x in expiration_list if not x.empty]

    expiration_frame = pd.concat(expiration_list,sort=True)
    return expiration_frame.sort_values('tr_days_2roll', ascending=True, inplace=False)










