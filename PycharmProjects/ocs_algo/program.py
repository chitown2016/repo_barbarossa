
import ib_api_utils.subscription as subs
import ib_api_utils.ib_contract as ib_contract
import opportunity_constructs.overnight_calendar_spreads as ocs
import my_sql_routines.my_sql_utilities as msu
import copy as cpy
import ta.trade_fill_loader as tfl
import ta.strategy as ts
import ta.position_manager as pm
import ta.portfolio_manager as tpm
import contract_utilities.contract_meta_info as cmi
import contract_utilities.expiration as exp
import api_utils.portfolio as aup
import shared.utils as su
import shared.calendar_utilities as cu
import shared.directory_names as dn
import numpy as np
import pandas as pd
import math as mth
import threading as thr
import datetime as dt
from ibapi.contract import *
from ibapi.common import *
from ibapi.ticktype import *
from ibapi.order import *
from ibapi.order_state import *
from ibapi.execution import Execution
from ibapi.execution import ExecutionFilter
from ibapi.order_condition import *
import ocs_algo.algo as algo

import shared.log as lg

def main():
    app = algo.Algo()

    admin_dir = dn.get_directory_name(ext='admin')
    risk_file_out = su.read_text_file(file_name=admin_dir + '/RiskParameter.txt')
    app.bet_size = float(risk_file_out[0])

    con = msu.get_my_sql_connection()
    date_now = cu.get_doubledate()
    report_date = exp.doubledate_shift_bus_days()
    report_date_list = [exp.doubledate_shift_bus_days(shift_in_days=x) for x in range(1, 10)]
    overnight_calendars_list = []

    for i in range(len(report_date_list)):
        ocs_output = ocs.generate_overnight_spreads_sheet_4date(date_to=report_date_list[i])
        overnight_calendars = ocs_output['overnight_calendars']

        overnight_calendars = \
            overnight_calendars[overnight_calendars['tickerHead'].isin(['CL', 'HO', 'NG', 'C', 'W', 'KW', 'S', 'SM', 'BO', 'LC', 'LN', 'FC'])]

    #isin(['CL', 'HO','NG', 'C', 'W', 'KW', 'S', 'SM', 'BO', 'LC', 'LN', 'FC'])]

        overnight_calendars = overnight_calendars[(overnight_calendars['ticker1L']!='')&(overnight_calendars['ticker2L']!='')]
        overnight_calendars['back_spread_price'] = np.nan
        overnight_calendars['front_spread_price'] = np.nan
        overnight_calendars['mid_ticker_price'] = np.nan

        overnight_calendars['back_spread_ticker'] = [overnight_calendars['ticker1'].iloc[x] + '-' + overnight_calendars['ticker2'].iloc[x] for x in range(len(overnight_calendars.index))]
        overnight_calendars['front_spread_ticker'] = [overnight_calendars['ticker1L'].iloc[x] + '-' + overnight_calendars['ticker2L'].iloc[x] for x in range(len(overnight_calendars.index))]
        overnight_calendars['target_quantity'] = [min(mth.ceil(app.bet_size / x),app.total_traded_volume_max_before_user_confirmation) for x in overnight_calendars['dollarNoise100']]

        overnight_calendars['alias'] = [overnight_calendars['ticker1'].iloc[x] + '_' + overnight_calendars['ticker2'].iloc[x] + '_ocs' for x in range(len(overnight_calendars.index))]
        overnight_calendars['total_quantity'] = 0
        overnight_calendars['total_risk'] = 0
        overnight_calendars['holding_period'] = 0
        #overnight_calendars['expiring_position_q'] = 0

        overnight_calendars.reset_index(drop=True,inplace=True)

        overnight_calendars_list.append(overnight_calendars)
    overnight_calendars = overnight_calendars_list.pop(0)

    open_strategy_frame = ts.get_filtered_open_strategies(strategy_class_list=['ocs'],as_of_date=date_now)

    for i in range(len(open_strategy_frame.index)):
        position_manager_output = pm.get_ocs_position(alias=open_strategy_frame['alias'].iloc[i], as_of_date=date_now, con=con)

        trades_frame = ts.get_trades_4strategy_alias(alias=open_strategy_frame['alias'].iloc[i], con=con)

        datetime_now = cu.convert_doubledate_2datetime(date_now)
        holding_period = (datetime_now - trades_frame['trade_date'].min()).days

        if (not position_manager_output['empty_position_q'])&(not position_manager_output['correct_position_q']):
            print('Check ' + open_strategy_frame['alias'].iloc[i] + ' ! Position may be incorrect')
        elif position_manager_output['correct_position_q']:

            ticker_head = cmi.get_contract_specs(position_manager_output['sorted_position']['ticker'].iloc[0])['ticker_head']
            position_name = ''

            if position_manager_output['scale']>0:
                position_name = ticker_head + '_long'
            else:
                position_name = ticker_head + '_short'

            app.ocs_portfolio.order_send(ticker=position_name, qty=abs(position_manager_output['scale']))
            app.ocs_portfolio.order_fill(ticker=position_name, qty=abs(position_manager_output['scale']))

            ticker1 = position_manager_output['sorted_position']['ticker'].iloc[0]
            ticker2 = position_manager_output['sorted_position']['ticker'].iloc[1]

            selection_indx = overnight_calendars['back_spread_ticker'] == ticker1 + '-' + ticker2

            if sum(selection_indx)==1:
                overnight_calendars.loc[selection_indx, 'total_quantity'] = position_manager_output['scale']
                overnight_calendars.loc[selection_indx, 'total_risk'] = position_manager_output['scale']*overnight_calendars.loc[selection_indx, 'dollarNoise100']
                overnight_calendars.loc[selection_indx, 'alias'] = open_strategy_frame['alias'].iloc[i]
                overnight_calendars.loc[selection_indx, 'holding_period'] = holding_period

                app.ocs_risk_portfolio.order_send(ticker=position_name, qty=abs(position_manager_output['scale']*overnight_calendars.loc[selection_indx, 'dollarNoise100'].values[0]))
                app.ocs_risk_portfolio.order_fill(ticker=position_name, qty=abs(position_manager_output['scale']*overnight_calendars.loc[selection_indx, 'dollarNoise100'].values[0]))

            else:
                for j in range(len(overnight_calendars_list)):
                    overnight_calendars_past = overnight_calendars_list[j]
                    selection_indx = overnight_calendars_past['back_spread_ticker'] == ticker1 + '-' + ticker2
                    if sum(selection_indx)==1:
                        overnight_calendars_past.loc[selection_indx, 'total_quantity'] = position_manager_output['scale']
                        overnight_calendars_past.loc[selection_indx, 'total_risk'] = position_manager_output['scale'] * overnight_calendars_past.loc[selection_indx, 'dollarNoise100']
                        overnight_calendars_past.loc[selection_indx, 'alias'] = open_strategy_frame['alias'].iloc[i]
                        overnight_calendars_past.loc[selection_indx, 'holding_period'] = holding_period


                        app.ocs_risk_portfolio.order_send(ticker=position_name, qty=abs(position_manager_output['scale'] * overnight_calendars_past.loc[selection_indx, 'dollarNoise100'].values[0]))
                        app.ocs_risk_portfolio.order_fill(ticker=position_name, qty=abs(position_manager_output['scale'] * overnight_calendars_past.loc[selection_indx, 'dollarNoise100'].values[0]))

                        if j>1:
                            overnight_calendars_past.loc[selection_indx,'butterflyMean'] = np.nan
                            overnight_calendars_past.loc[selection_indx,'butterflyNoise'] = np.nan

                        overnight_calendars = overnight_calendars.append(overnight_calendars_past[selection_indx])
                        break

    overnight_calendars.reset_index(drop=True, inplace=True)
    overnight_calendars['working_order_id'] = np.nan

    spread_ticker_list = list(set(overnight_calendars['back_spread_ticker']).union(overnight_calendars['front_spread_ticker']))
    back_spread_ticker_list = list(overnight_calendars['back_spread_ticker'])

    theme_name_list = set([x + '_long' for x in back_spread_ticker_list]).union(set([x + '_short' for x in back_spread_ticker_list]))
    ocs_alias_portfolio = aup.portfolio(ticker_list=theme_name_list)

    for i in range(len(overnight_calendars.index)):

        if overnight_calendars.loc[i,'total_quantity']>0:
            position_name = overnight_calendars.loc[i,'back_spread_ticker'] + '_long'
            ocs_alias_portfolio.order_send(ticker=position_name, qty=overnight_calendars.loc[i,'total_quantity'])
            ocs_alias_portfolio.order_fill(ticker=position_name, qty=overnight_calendars.loc[i,'total_quantity'])
        elif overnight_calendars.loc[i,'total_quantity']<0:
            position_name = overnight_calendars.loc[i, 'back_spread_ticker'] + '_short'
            ocs_alias_portfolio.order_send(ticker=position_name, qty=-overnight_calendars.loc[i, 'total_quantity'])
            ocs_alias_portfolio.order_fill(ticker=position_name, qty=-overnight_calendars.loc[i, 'total_quantity'])

    app.price_request_dictionary['spread'] = spread_ticker_list
    app.price_request_dictionary['outright'] = overnight_calendars['ticker1'].values
    app.overnight_calendars = overnight_calendars
    app.open_strategy_list = list(open_strategy_frame['alias'])
    app.ocs_alias_portfolio = ocs_alias_portfolio
    app.ticker_list = list(set(overnight_calendars['ticker1']).union(overnight_calendars['ticker2']).union(set(overnight_calendars['ticker1L'])).union(set(overnight_calendars['ticker2L'])))
    app.output_dir = ts.create_strategy_output_dir(strategy_class='ocs', report_date=report_date)
    app.log = lg.get_logger(file_identifier='ib_ocs',log_level='INFO')

    app.con = con
    app.pnl_frame = tpm.get_daily_pnl_snapshot(as_of_date=report_date,name='final')
    print('Emre')

    app.connect(client_id=2)
    app.run()

if __name__ == "__main__":
    main()

