
import get_price.get_options_price as gop
import my_sql_routines.my_sql_utilities as msu
import contract_utilities.expiration as exp
import contract_utilities.contract_lists as cl
import get_price.get_futures_price as gfp
import option_models.utils as oput
import pandas as pd
import numpy as np


def update_options_daily_pnl_database_4ticker(**kwargs):

    if 'con' not in kwargs.keys():
        con = msu.get_my_sql_connection(**kwargs)
        kwargs['con'] = con
        close_connection_before_exit = True
    else:
        close_connection_before_exit = False
        con = kwargs['con']

    settle_date = kwargs['settle_date']
    settle_date_1 = exp.doubledate_shift_bus_days(double_date=settle_date,shift_in_days=1)

    kwargs['column_names'] = ['id','option_type', 'strike','close_price', 'delta']

    option_prices = gop.get_options_price_from_db(**kwargs)
    option_prices['option_id'] = [option_prices['option_type'].iloc[x] + '_' + str(option_prices['strike'].iloc[x]) for x in range(len(option_prices.index))]

    underlying_ticker = oput.get_option_underlying(**kwargs)
    futures_price_output = gfp.get_futures_price_preloaded(ticker=underlying_ticker, settle_date=kwargs['settle_date'])

    if futures_price_output.empty:
        return pd.DataFrame()

    underlying_price = futures_price_output['close_price'].iloc[0]

    kwargs['settle_date'] = settle_date_1
    option_prices_1 = gop.get_options_price_from_db(**kwargs)
    option_prices_1['option_id'] = [option_prices_1['option_type'].iloc[x] + '_' + str(option_prices_1['strike'].iloc[x]) for x in range(len(option_prices_1.index))]

    futures_price_output_1 = gfp.get_futures_price_preloaded(ticker=underlying_ticker, settle_date=settle_date_1)

    if futures_price_output_1.empty:
        return pd.DataFrame()

    underlying_price_1 = futures_price_output_1['close_price'].iloc[0]

    merged_prices = pd.merge(option_prices_1,option_prices,how='inner',on='option_id')
    merged_prices['option_pnl'] = merged_prices['close_price_y']-merged_prices['close_price_x']
    merged_prices['delta_pnl'] = merged_prices['delta_x']*(underlying_price-underlying_price_1)

    column_names = merged_prices.columns.tolist()

    id_indx = column_names.index('id_x')
    option_pnl_indx = column_names.index('option_pnl')
    delta_pnl_indx = column_names.index('delta_pnl')

    tuples = [tuple([None if np.isnan(x[option_pnl_indx]) else x[option_pnl_indx],
                    None if np.isnan(x[delta_pnl_indx]) else x[delta_pnl_indx],
                    x[id_indx]]) for x in merged_prices.values]

    final_str = "UPDATE daily_option_price SET option_pnl = %s,  delta_pnl = %s WHERE id=%s"

    msu.sql_execute_many_wrapper(final_str=final_str, tuples=tuples, con=con)

    if close_connection_before_exit:
        con.close()

def update_options_multiday_pnl_database_4ticker(**kwargs):

    if 'con' not in kwargs.keys():
        con = msu.get_my_sql_connection(**kwargs)
        kwargs['con'] = con
        close_connection_before_exit = True
    else:
        close_connection_before_exit = False
        con = kwargs['con']

    settle_date = kwargs['settle_date']
    settle_date_list = [exp.doubledate_shift_bus_days(double_date=settle_date,shift_in_days=x) for x in range(5, 0, -1)]

    column_names = ['id','option_type', 'strike','option_pnl','delta_pnl']
    option_prices_list = [gop.get_options_price_from_db(ticker=kwargs['ticker'], column_names=column_names, settle_date=x) for x in settle_date_list]

    for i in range(5):
        if option_prices_list[i].empty:
            if close_connection_before_exit:
                con.close()
            return

    option_prices = option_prices_list[0]
    option_prices['option_id'] = [option_prices['option_type'].iloc[x] + '_' + str(option_prices['strike'].iloc[x]) for x in range(len(option_prices.index))]
    option_prices.drop(['strike','option_type'],1,inplace=True)
    option_prices.rename(columns={'id': 'id0'}, inplace=True)

    merged_prices = option_prices

    for i in range(1, 5):
        option_prices_i = option_prices_list[i]
        option_prices_i['option_id'] = [option_prices_i['option_type'].iloc[x] + '_' + str(option_prices_i['strike'].iloc[x]) for x in range(len(option_prices_i.index))]
        option_prices_i.drop(['strike','option_type'],1,inplace=True)

        merged_prices = pd.merge(merged_prices,option_prices_i,how='inner',on='option_id')

        merged_prices['option_pnl'] = merged_prices['option_pnl_x'] + merged_prices['option_pnl_y']
        merged_prices['delta_pnl'] = merged_prices['delta_pnl_x'] + merged_prices['delta_pnl_y']
        merged_prices.drop(['option_pnl_x','option_pnl_y','delta_pnl_x','delta_pnl_y','id'], 1,inplace=True)

    merged_prices.dropna(inplace=True)

    if len(merged_prices.index)>0:

        column_names = merged_prices.columns.tolist()

        id_indx = column_names.index('id0')
        option_pnl_indx = column_names.index('option_pnl')
        delta_pnl_indx = column_names.index('delta_pnl')

        tuples = [tuple([x[option_pnl_indx],
                    x[delta_pnl_indx],
                    x[id_indx]]) for x in merged_prices.values]

        final_str = "UPDATE daily_option_price SET option_pnl5 = %s,  delta_pnl5 = %s WHERE id=%s"

        msu.sql_execute_many_wrapper(final_str=final_str, tuples=tuples, con=con)

    if close_connection_before_exit:
        con.close()


def update_options_pnls_4date(**kwargs):

    options_frame = cl.generate_liquid_options_list_dataframe(**kwargs)
    [update_options_daily_pnl_database_4ticker(ticker=x, **kwargs) for x in options_frame['ticker']]
    [update_options_multiday_pnl_database_4ticker(ticker=x, **kwargs) for x in options_frame['ticker']]