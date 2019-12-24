
import my_sql_routines.my_sql_utilities as msu
import ta.strategy as ts
import ta.pnl as tpnl
import contract_utilities.expiration as exp
import shared.converters as sc
import pandas as pd
import shared.directory_names_aux as dna


def get_strategy_class_historical_pnls(**kwargs):

    con = msu.get_my_sql_connection(**kwargs)

    if 'as_of_date' in kwargs.keys():
        as_of_date = kwargs['as_of_date']
    else:
        as_of_date = exp.doubledate_shift_bus_days()

    strategy_frame = ts.select_strategies(con=con,open_date_to=as_of_date)

    strategy_frame['strategy_class'] = [sc.convert_from_string_to_dictionary(string_input=strategy_frame['description_string'][x])['strategy_class']
                           for x in range(len(strategy_frame.index))]

    unique_strategy_class_list = strategy_frame['strategy_class'].unique()
    time_series_list = [None]*len(unique_strategy_class_list)

    for i in range(len(unique_strategy_class_list)):
        strategy_frame_selected = strategy_frame[strategy_frame['strategy_class'] == unique_strategy_class_list[i]]
        pnl_out = [tpnl.get_strategy_pnl(alias=x,as_of_date=as_of_date,con=con)['pnl_frame'][['settle_date','total_pnl']] for x in strategy_frame_selected['alias']]
        [x.set_index('settle_date',drop=True, inplace=True) for x in pnl_out]
        time_series_list[i] = pd.concat(pnl_out,axis=1).fillna(0).sum(axis=1)

    if 'con' not in kwargs.keys():
        con.close()

    merged_pnl = pd.concat(time_series_list,axis=1,keys=unique_strategy_class_list).fillna(0)
    merged_pnl['total'] = merged_pnl.sum(axis=1)

    output_dir = dna.get_directory_name(ext='daily')
    writer = pd.ExcelWriter(output_dir + '/historical_performance_' + str(as_of_date) + '.xlsx', engine='xlsxwriter')
    merged_pnl.to_excel(writer, sheet_name='timeSeries')
    writer.save()

    return merged_pnl




