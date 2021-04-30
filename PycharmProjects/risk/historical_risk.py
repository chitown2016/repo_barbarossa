
import contract_utilities.expiration as exp
import pandas as pd
import ta.strategy as ts
import my_sql_routines.my_sql_utilities as msu
import shared.calendar_utilities as cu
import contract_utilities.contract_meta_info as cmi
import get_price.get_futures_price as gfp
import opportunity_constructs.utilities as opUtil
import pickle as pickle
import opportunity_constructs.constants as const
import shared.statistics as stats
import shared.utils as su
import shared.directory_names as dn
import os.path


def get_historical_risk_4strategy(**kwargs):

    con = msu.get_my_sql_connection(**kwargs)

    alias = kwargs['alias']

    #print(alias)

    if 'as_of_date' in kwargs.keys():
        as_of_date = kwargs['as_of_date']
    else:
        as_of_date = exp.doubledate_shift_bus_days()

    net_position = ts.get_net_position_4strategy_alias(alias=alias,con=con)
    net_position = net_position[net_position['instrument'] != 'O']

    if 'con' not in kwargs.keys():
        con.close()

    if net_position.empty:
        return {'downside': 0, 'pnl_5_change': []}

    amcb_output = [opUtil.get_aggregation_method_contracts_back(cmi.get_contract_specs(x)) for x in net_position['ticker']]

    aggregation_method = pd.DataFrame(amcb_output)['aggregation_method'].max()

    if aggregation_method == 12:
        contracts_back = const.annualContractsBack
    elif aggregation_method == 3:
        contracts_back = const.quarterlyContractsBack
    elif aggregation_method == 1:
        contracts_back = const.monthlyContractsBack

    aligned_output = opUtil.get_aligned_futures_data(contract_list=net_position['ticker'].values,
                                    aggregation_method=aggregation_method,
                                    contracts_back=contracts_back,date_to=as_of_date,**kwargs)
    aligned_data = aligned_output['aligned_data']

    ticker_head_list = [cmi.get_contract_specs(x)['ticker_head'] for x in net_position['ticker']]
    contract_multiplier_list = [cmi.contract_multiplier[x] for x in ticker_head_list]

    pnl_5_change_list = [contract_multiplier_list[x]*
           net_position['qty'].iloc[x]*
           aligned_data['c' + str(x+1)]['change_5'] for x in range(len(net_position.index))]

    pnl_5_change = sum(pnl_5_change_list)

    percentile_vector = stats.get_number_from_quantile(y=pnl_5_change.values,
                                                       quantile_list=[1, 15],
                                                       clean_num_obs=max(100, round(3*len(pnl_5_change.values)/4)))
    downside = (percentile_vector[0]+percentile_vector[1])/2

    unique_ticker_head_list = list(set(ticker_head_list))

    ticker_head_based_pnl_5_change = {x: sum([pnl_5_change_list[y] for y in range(len(ticker_head_list)) if ticker_head_list[y] == x])
                        for x in unique_ticker_head_list}

    return {'downside': downside, 'pnl_5_change': pnl_5_change,'ticker_head_based_pnl_5_change':ticker_head_based_pnl_5_change}


def get_historical_risk_4open_strategies(**kwargs):

    if 'as_of_date' in kwargs.keys():
        as_of_date = kwargs['as_of_date']
    else:
        as_of_date = exp.doubledate_shift_bus_days()
        kwargs['as_of_date'] = as_of_date

    ta_output_dir = dn.get_dated_directory_extension(folder_date=as_of_date, ext='ta')

    if os.path.isfile(ta_output_dir + '/portfolio_risk.pkl'):
        with open(ta_output_dir + '/portfolio_risk.pkl','rb') as handle:
            portfolio_risk_output = pickle.load(handle)
        return portfolio_risk_output

    con = msu.get_my_sql_connection(**kwargs)

    strategy_frame = ts.get_open_strategies(**kwargs)
    futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in cmi.ticker_class.keys()}

    strategy_risk_frame = pd.DataFrame()

    historical_risk_output = [get_historical_risk_4strategy(alias=x,
                                                            as_of_date=as_of_date,
                                                            con=con,
                                                            futures_data_dictionary=futures_data_dictionary)
                              for x in strategy_frame['alias']]
    if 'con' not in kwargs.keys():
        con.close()

    strategy_risk_frame['alias'] = strategy_frame['alias']
    strategy_risk_frame['downside'] = [x['downside'] for x in historical_risk_output]
    strategy_risk_frame.sort_values('downside', ascending=True, inplace=True)

    ticker_head_list = su.flatten_list([list(x['ticker_head_based_pnl_5_change'].keys()) for x in historical_risk_output if x['downside'] != 0])
    unique_ticker_head_list = list(set(ticker_head_list))

    ticker_head_aggregated_pnl_5_change = {ticker_head: sum([x['ticker_head_based_pnl_5_change'][ticker_head] for x in historical_risk_output
         if x['downside'] != 0 and ticker_head in x['ticker_head_based_pnl_5_change'].keys()]) for ticker_head in unique_ticker_head_list}

    percentile_vector = [stats.get_number_from_quantile(y=ticker_head_aggregated_pnl_5_change[ticker_head],
                                                        quantile_list=[1, 15],
                                clean_num_obs=max(100, round(3*len(ticker_head_aggregated_pnl_5_change[ticker_head].values)/4)))
                         for ticker_head in unique_ticker_head_list]

    ticker_head_risk_frame = pd.DataFrame()
    ticker_head_risk_frame['tickerHead'] = unique_ticker_head_list
    ticker_head_risk_frame['downside'] = [(x[0]+x[1])/2 for x in percentile_vector]

    ticker_head_risk_frame.sort_values('downside', ascending=True, inplace=True)

    strategy_risk_frame['downside'] = strategy_risk_frame['downside'].round()
    ticker_head_risk_frame['downside'] = ticker_head_risk_frame['downside'].round()

    portfolio_risk_output = {'strategy_risk_frame': strategy_risk_frame,
                             'ticker_head_aggregated_pnl_5_change': ticker_head_aggregated_pnl_5_change,
                             'ticker_head_risk_frame':ticker_head_risk_frame}

    with open(ta_output_dir + '/portfolio_risk.pkl', 'wb') as handle:
        pickle.dump(portfolio_risk_output, handle)

    return portfolio_risk_output






