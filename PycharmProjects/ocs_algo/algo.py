

import ib_api_utils.subscription as subs
import ib_api_utils.ib_contract as ib_contract
import opportunity_constructs.overnight_calendar_spreads as ocs
import copy as cpy
import ta.trade_fill_loader as tfl
import ta.strategy as ts
import ta.position_manager as pm
import contract_utilities.contract_meta_info as cmi
import contract_utilities.expiration as exp
import ib_api_utils.trade as ib_api_trade
import api_utils.portfolio as aup
import shared.utils as su
import shared.calendar_utilities as cu
import numpy as np
import pandas as pd
import math as mth
import threading as thr
import datetime as dt
import time as tm
from ibapi.contract import *
from ibapi.common import *
from ibapi.ticktype import *
from ibapi.order import *
from ibapi.order_state import *
from ibapi.execution import Execution
from ibapi.execution import ExecutionFilter
from ibapi.order_condition import *

class Algo(subs.subscription):

    contractDetailReqIdDictionary = {}
    market_data_ReqId_dictionary = {}
    contractIDDictionary = {}
    bid_price_dictionary = {}
    ask_price_dictionary = {}
    bid_quantity_dictionary = {}
    ask_quantity_dictionary = {}
    spread_contract_dictionary = {}
    order_filled_dictionary = {}
    nonfinished_contract_detail_ReqId_list = []
    spread_ticker_list = []
    price_request_dictionary = {'spread': [] ,'outright': []}
    nonfinished_bid_price_list = []
    nonfinished_ask_price_list = []
    nonfinished_bid_quantity_list = []
    nonfinished_ask_quantity_list = []
    period_call_initiated_q = False
    min_avg_volume_limit = 100
    bet_size = np.nan
    # bet_size of 240 caused a $12,736 drawdown
    # increased size %44 on June 1st 2018
    # increased size %8 on July 2nd 2018
    # increased size %10 (150->165) on October 1st 2018
    # increase size by %15 (165->190) on November 1st 2018
    # increase size by %15 (190->220) on January 2nd 2019
    # increase size by %100 (220->440) on February 19th 2019
    # increase size by %25 (440->550) on May 24th 2019
    # increase size by %27 (550->700) on Feb 24th 2020
    # increase size by %100 (700->1400) on Aug 7th 2020
    # decrease size by %30 (1400->980) on Jan 12 2021
    # decrease size by %30 (980->686) on Feb 1 2021
    # increase size by %20 (686->823) on Apr 6 2021
    total_traded_volume_max_before_user_confirmation = 500
    total_traded_volume_since_last_confirmation = 0
    total_volume_traded = 0
    max_num_bets = 3
    num_messages = 0
    num_bets = 0
    ticker_head_list = cmi.futures_butterfly_strategy_tickerhead_list
    theme_name_list = set([x + '_long' for x in ticker_head_list]).union(set([x + '_short' for x in ticker_head_list]))
    ocs_portfolio = aup.portfolio(ticker_list=theme_name_list)
    ocs_risk_portfolio = aup.portfolio(ticker_list=theme_name_list)

    def contractDetails(self, reqId: int, contractDetails: ContractDetails):
        super().contractDetails(reqId, contractDetails)
        self.contractIDDictionary[self.contractDetailReqIdDictionary[reqId]] = contractDetails.contract.conId

    def contractDetailsEnd(self, reqId: int):
            super().contractDetailsEnd(reqId)
            self.nonfinished_contract_detail_ReqId_list.remove(reqId)
            self.nonfinished_ticker_list.remove(self.contractDetailReqIdDictionary[reqId])
            if len(self.nonfinished_contract_detail_ReqId_list)==0:
                self.request_spread_market_data()
                self.request_outright_market_data()

    def tickPrice(self, reqId: TickerId, tickType: TickType, price: float,
                  attrib: TickAttrib):
        super().tickPrice(reqId, tickType, price, attrib)

        if tickType==1:
            self.bid_price_dictionary[self.market_data_ReqId_dictionary[reqId]] = price
            if self.market_data_ReqId_dictionary[reqId] in self.nonfinished_bid_price_list:
                self.nonfinished_bid_price_list.remove(self.market_data_ReqId_dictionary[reqId])
        elif tickType==2:
            self.ask_price_dictionary[self.market_data_ReqId_dictionary[reqId]] = price
            if self.market_data_ReqId_dictionary[reqId] in self.nonfinished_ask_price_list:
                self.nonfinished_ask_price_list.remove(self.market_data_ReqId_dictionary[reqId])

        if (len(self.nonfinished_bid_price_list)==0)&(len(self.nonfinished_ask_price_list)==0)&\
                (len(self.nonfinished_bid_quantity_list)==0)&(len(self.nonfinished_ask_quantity_list)==0)&(~self.period_call_initiated_q):

            self.periodic_call()

    def tickSize(self, reqId: TickerId, tickType: TickType, size: int):
        super().tickSize(reqId, tickType, size)

        if tickType==0:
            self.bid_quantity_dictionary[self.market_data_ReqId_dictionary[reqId]] = size
            if self.market_data_ReqId_dictionary[reqId] in self.nonfinished_bid_quantity_list:
                self.nonfinished_bid_quantity_list.remove(self.market_data_ReqId_dictionary[reqId])
        if tickType==3:
            self.ask_quantity_dictionary[self.market_data_ReqId_dictionary[reqId]] = size
            if self.market_data_ReqId_dictionary[reqId] in self.nonfinished_ask_quantity_list:
                self.nonfinished_ask_quantity_list.remove(self.market_data_ReqId_dictionary[reqId])

    def openOrder(self, orderId: OrderId, contract: Contract, order: Order,orderState: OrderState):
        super().openOrder(orderId, contract, order, orderState)
        print("OpenOrder. ID:", orderId, contract.symbol, contract.secType,
        "@", contract.exchange, ":", order.action, order.orderType,order.totalQuantity, orderState.status)

    def orderStatus(self, orderId: OrderId, status: str, filled: float,remaining: float, avgFillPrice: float, permId: int,parentId: int, lastFillPrice: float, clientId: int,whyHeld: str, mktCapPrice: float):
        super().orderStatus(orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice)
        print("OrderStatus. Id:", orderId, "Status:", status, "Filled:", filled,
        "Remaining:", remaining, "AvgFillPrice:", avgFillPrice,
        "PermId:", permId, "ParentId:", parentId, "LastFillPrice:",
        lastFillPrice, "ClientId:", clientId, "WhyHeld:", whyHeld)

        with open(self.output_dir + '/fills.csv', 'a') as file:
            file.write(str(orderId) + ',' + str(permId)+ ',' + str(parentId)+ ',' + str(filled)+ ',' + str(avgFillPrice))
            file.write('\n')

        overnight_calendars = self.overnight_calendars
        selection_index = overnight_calendars['working_order_id'] == orderId

        if sum(selection_index)==0:
            return

        back_spread_ticker = overnight_calendars.loc[selection_index, 'back_spread_ticker'].values[0]
        ticker_head = overnight_calendars.loc[selection_index, 'tickerHead'].values[0]
        dollar_noise100 = overnight_calendars.loc[selection_index, 'dollarNoise100'].values[0]

        working_alias_long = self.ocs_alias_portfolio.position_with_working_orders[back_spread_ticker + '_long']
        working_alias_short = self.ocs_alias_portfolio.position_with_working_orders[back_spread_ticker + '_short']

        theme_name = ''
        alias_portfolio_name = ''
        trade_quantity = 0

        if filled==0 or (filled==self.order_filled_dictionary[orderId]):
            return

        filled_change = filled - self.order_filled_dictionary[orderId]
        self.order_filled_dictionary[orderId] = filled

        if working_alias_long>0:
            alias_portfolio_name = back_spread_ticker + '_long'
            theme_name = ticker_head + '_long'
            trade_quantity = filled_change
        elif working_alias_short>0:
            alias_portfolio_name = back_spread_ticker + '_short'
            theme_name = ticker_head + '_short'
            trade_quantity = filled_change
        elif working_alias_long<0:
            alias_portfolio_name = back_spread_ticker + '_long'
            theme_name = ticker_head + '_long'
            trade_quantity = -filled_change
        elif working_alias_short<0:
            alias_portfolio_name = back_spread_ticker + '_short'
            theme_name = ticker_head + '_short'
            trade_quantity = -filled_change

        self.ocs_portfolio.order_fill(ticker=theme_name, qty=trade_quantity)
        self.ocs_alias_portfolio.order_fill(ticker=alias_portfolio_name, qty=trade_quantity)
        self.ocs_risk_portfolio.order_fill(ticker=theme_name, qty=trade_quantity * dollar_noise100)

        #print('Order filled!')
        #total_alias = self.ocs_alias_portfolio.position_with_all_orders[alias_portfolio_name]
        #working_alias = self.ocs_alias_portfolio.position_with_working_orders[alias_portfolio_name]
        #filled_alias = self.ocs_alias_portfolio.position_with_filled_orders[alias_portfolio_name]
        #print('Total position: ' + str(total_alias), ', Working Position: ' + str(working_alias) + ', Filled Position: ' + str(filled_alias))


    def execDetails(self, reqId: int, contract: Contract, execution: Execution):
        super().execDetails(reqId, contract, execution)
        try:
            self.log.info('ExecDetails: ' + str(reqId) + ', ' + contract.symbol + ', ' +  contract.secType + ', ' + contract.currency + ', ' + str(execution.execId) + ', ' + str(execution.orderId) + ', ' + str(execution.shares))

            if contract.secType!='BAG':
                ticker = ib_contract.get_db_ticker_from_ib_contract(ib_contract=contract,contract_id_dictionary=self.contractIDDictionary)

                with open(self.output_dir + '/fillsDetail.csv', 'a') as file:
                    file.write(str(execution.orderId) + ',' + str(execution.execId) + ',' + str(ticker) + ',' + str(execution.price) + ',' + str(execution.shares) + ',' + str(execution.side))
                    file.write('\n')

                overnight_calendars = self.overnight_calendars
                selection_index = overnight_calendars['working_order_id']==execution.orderId
                strategy_alias = overnight_calendars.loc[selection_index,'alias'].values[0]

                if execution.side=='BOT':
                    trade_quantity = execution.shares
                elif execution.side=='SLD':
                    trade_quantity = -execution.shares


                if strategy_alias in self.open_strategy_list:
                    strategy_alias_2write = strategy_alias

                else:

                    db_strategy_output = ts.generate_db_strategy_from_alias(alias=strategy_alias,con=self.con,description_string='strategy_class=ocs&betSize=' + str(self.bet_size) +
                                                                                                        '&ticker1=' +overnight_calendars.loc[selection_index,'ticker1'].values[0] +
                                                                                                        '&ticker2=' +overnight_calendars.loc[selection_index, 'ticker2'].values[0])

                    strategy_alias_2write = db_strategy_output['alias']
                    overnight_calendars.loc[selection_index, 'alias'] = strategy_alias_2write
                    self.open_strategy_list.append(strategy_alias_2write)
                    self.overnight_calendars = overnight_calendars

                trade_frame = pd.DataFrame.from_items([('alias', [strategy_alias_2write]),
                                                   ('ticker', [ticker]),
                                                   ('option_type', [None]),
                                                   ('strike_price', [np.nan]),
                                                   ('trade_price', [execution.price]),
                                                   ('trade_quantity', [trade_quantity]),
                                                   ('instrument', ['F']),
                                                   ('real_tradeQ', [True])])

                ts.load_trades_2strategy(trade_frame=trade_frame, con=self.con)
        except Exception:
            self.log.error('execDetails failed', exc_info=True)


    def periodic_call(self):

        overnight_calendars = self.overnight_calendars
        self.period_call_initiated_q = True

        datetime_now = dt.datetime.now()
        self.log.info(datetime_now.strftime('%Y-%m-%d %H:%M:%S'))
        self.log.info('total volume traded so far: ' + str(self.total_volume_traded))


        for i in range(len(overnight_calendars.index)):
            front_spread_ticker = overnight_calendars.loc[i, 'front_spread_ticker']
            back_spread_ticker = overnight_calendars.loc[i, 'back_spread_ticker']
            mid_ticker = overnight_calendars.loc[i, 'ticker1']

            if (self.bid_quantity_dictionary[front_spread_ticker]==0) or \
                    (self.ask_quantity_dictionary[front_spread_ticker]==0)or \
                    (self.bid_quantity_dictionary[back_spread_ticker]==0) or \
                    (self.ask_quantity_dictionary[back_spread_ticker]==0) or \
                    (self.bid_quantity_dictionary[mid_ticker]==0) or \
                    (self.ask_quantity_dictionary[mid_ticker]==0):
                continue


            overnight_calendars.loc[i, 'front_spread_price'] = (self.bid_price_dictionary[front_spread_ticker]*self.bid_quantity_dictionary[front_spread_ticker] +
                                                                self.ask_price_dictionary[front_spread_ticker]*self.ask_quantity_dictionary[front_spread_ticker])\
                                                               /(self.bid_quantity_dictionary[front_spread_ticker]+self.ask_quantity_dictionary[front_spread_ticker])

            overnight_calendars.loc[i, 'back_spread_price'] = (self.bid_price_dictionary[back_spread_ticker]*self.bid_quantity_dictionary[back_spread_ticker] +
                                                               self.ask_price_dictionary[back_spread_ticker]*self.ask_quantity_dictionary[back_spread_ticker]) \
                                                              /(self.bid_quantity_dictionary[back_spread_ticker]+self.ask_quantity_dictionary[back_spread_ticker])

            overnight_calendars.loc[i, 'mid_ticker_price'] = (self.bid_price_dictionary[mid_ticker]*self.bid_quantity_dictionary[mid_ticker] +
                                                              self.ask_price_dictionary[mid_ticker]*self.ask_quantity_dictionary[mid_ticker]) \
                                                             /(self.bid_quantity_dictionary[mid_ticker]+self.ask_quantity_dictionary[mid_ticker])

            overnight_calendars.loc[i,'normalized_butterfly_price'] = 100 * (overnight_calendars.loc[i,'front_spread_price'] - overnight_calendars.loc[i,'back_spread_price']) / overnight_calendars.loc[i,'mid_ticker_price']
            overnight_calendars.loc[i, 'normalized_butterfly_price_b'] = 100 * (overnight_calendars.loc[i, 'front_spread_price'] - self.bid_price_dictionary[back_spread_ticker]) /overnight_calendars.loc[i, 'mid_ticker_price']
            overnight_calendars.loc[i, 'normalized_butterfly_price_a'] = 100 * (overnight_calendars.loc[i, 'front_spread_price'] - self.ask_price_dictionary[back_spread_ticker]) / overnight_calendars.loc[i, 'mid_ticker_price']

            overnight_calendars.loc[i,'butterfly_z_current'] = (overnight_calendars.loc[i,'normalized_butterfly_price'] -overnight_calendars.loc[i,'butterflyMean']) / overnight_calendars.loc[i,'butterflyNoise']
            overnight_calendars.loc[i, 'butterfly_z_current_b'] = (overnight_calendars.loc[i, 'normalized_butterfly_price_b'] -overnight_calendars.loc[i, 'butterflyMean']) / overnight_calendars.loc[i, 'butterflyNoise']
            overnight_calendars.loc[i, 'butterfly_z_current_a'] = (overnight_calendars.loc[i, 'normalized_butterfly_price_a'] -overnight_calendars.loc[i, 'butterflyMean']) / overnight_calendars.loc[i, 'butterflyNoise']

            ticker_head = overnight_calendars['tickerHead'].loc[i]
            ticker_class = overnight_calendars['tickerClass'].loc[i]
            ticker1 = overnight_calendars['ticker1'].loc[i]
            ticker2 = overnight_calendars['ticker2'].loc[i]
            trDte1 = overnight_calendars['trDte1'].loc[i]

            spread_contract = self.spread_contract_dictionary[back_spread_ticker]
            spread_price = overnight_calendars.loc[i,'back_spread_price']
            bid_price = self.bid_price_dictionary[back_spread_ticker]
            ask_price = self.ask_price_dictionary[back_spread_ticker]

            bid_quantity = self.bid_quantity_dictionary[back_spread_ticker]
            ask_quantity = self.ask_quantity_dictionary[back_spread_ticker]

            tick_size = cmi.tick_size[ticker_head]
            bid_ask_in_ticks = (self.ask_price_dictionary[back_spread_ticker] - self.bid_price_dictionary[back_spread_ticker]) / tick_size

            butterfly_price = overnight_calendars.loc[i,'front_spread_price'] - overnight_calendars.loc[i,'back_spread_price']
            normalized_butterfly_price = overnight_calendars.loc[i,'normalized_butterfly_price']
            normalized_butterfly_price_b = overnight_calendars.loc[i, 'normalized_butterfly_price_b']
            normalized_butterfly_price_a = overnight_calendars.loc[i, 'normalized_butterfly_price_a']
            butterfly_q25 = overnight_calendars.loc[i,'butterfly_q25']
            butterfly_q75 = overnight_calendars.loc[i,'butterfly_q75']
            min_avg_volume = overnight_calendars.loc[i,'min_avg_volume']

            z_score = overnight_calendars.loc[i,'butterfly_z_current']
            z_score_b = overnight_calendars.loc[i, 'butterfly_z_current_b']
            z_score_a = overnight_calendars.loc[i, 'butterfly_z_current_a']
            z_score_s = overnight_calendars.loc[i, 'butterflyZ']
            qCarry = overnight_calendars.loc[i, 'qCarry']

            target_quantity = overnight_calendars.loc[i,'target_quantity']
            dollar_noise100 = overnight_calendars.loc[i,'dollarNoise100']
            holding_period = overnight_calendars.loc[i, 'holding_period']
            continue_q = 'n'

            pnl_frame = self.pnl_frame
            position_select = pnl_frame['alias'] == overnight_calendars.loc[i, 'alias']

            if sum(position_select) ==0:
                realized_pnl = 0
            else:
                realized_pnl = pnl_frame['total_pnl'].loc[position_select].values[0]

            total_position_long = self.ocs_portfolio.position_with_all_orders[ticker_head + '_long']
            working_position_long = self.ocs_portfolio.position_with_working_orders[ticker_head + '_long']
            filled_position_long = self.ocs_portfolio.position_with_filled_orders[ticker_head + '_long']

            total_position_short = self.ocs_portfolio.position_with_all_orders[ticker_head + '_short']
            working_position_short = self.ocs_portfolio.position_with_working_orders[ticker_head + '_short']
            filled_position_short = self.ocs_portfolio.position_with_filled_orders[ticker_head + '_short']

            total_alias_long = self.ocs_alias_portfolio.position_with_all_orders[back_spread_ticker + '_long']
            working_alias_long = self.ocs_alias_portfolio.position_with_working_orders[back_spread_ticker + '_long']
            filled_alias_long = self.ocs_alias_portfolio.position_with_filled_orders[back_spread_ticker + '_long']

            total_alias_short = self.ocs_alias_portfolio.position_with_all_orders[back_spread_ticker + '_short']
            working_alias_short = self.ocs_alias_portfolio.position_with_working_orders[back_spread_ticker + '_short']
            filled_alias_short = self.ocs_alias_portfolio.position_with_filled_orders[back_spread_ticker + '_short']

            total_risk_long = self.ocs_risk_portfolio.position_with_all_orders[ticker_head + '_long']
            working_risk_long = self.ocs_risk_portfolio.position_with_working_orders[ticker_head + '_long']
            filled_risk_long = self.ocs_risk_portfolio.position_with_filled_orders[ticker_head + '_long']

            total_risk_short = self.ocs_risk_portfolio.position_with_all_orders[ticker_head + '_short']
            working_risk_short = self.ocs_risk_portfolio.position_with_working_orders[ticker_head + '_short']
            filled_risk_short = self.ocs_risk_portfolio.position_with_filled_orders[ticker_head + '_short']

            if self.total_traded_volume_since_last_confirmation+target_quantity>self.total_traded_volume_max_before_user_confirmation:
                self.log.info('total volume traded since last user approval is: ' + str(self.total_traded_volume_since_last_confirmation))
                self.log.info('new target quantity:' + str(target_quantity))
                continue_q = input('Continue? (y/n): ')
                if continue_q == 'y':
                    self.total_traded_volume_since_last_confirmation = 0
                else:
                    continue

            if (z_score < -1) and (z_score <= z_score_s) and (qCarry <= -9) and (normalized_butterfly_price < butterfly_q25) and (min_avg_volume >= self.min_avg_volume_limit) and (self.num_bets < self.max_num_bets):

                self.log.info(back_spread_ticker + ' might be a good short')
                self.log.info('working position short: ' + str(working_position_short))
                trade_quantity = min(mth.ceil((self.bet_size - total_risk_short) / dollar_noise100),
                                     self.total_traded_volume_max_before_user_confirmation)
                self.log.info('trade quantity: ' + str(trade_quantity))

            if (z_score > 1) and (z_score >= z_score_s) and (qCarry >= 19) and (normalized_butterfly_price > butterfly_q75) and (min_avg_volume >= self.min_avg_volume_limit) and (ticker_head not in ['CL', 'HO', 'NG']) and (self.num_bets < self.max_num_bets):
                self.log.info(back_spread_ticker + ' might be a good long')
                self.log.info('working position long: ' + str(working_position_long))
                trade_quantity = min(mth.ceil((self.bet_size - total_risk_long) / dollar_noise100),
                                     self.total_traded_volume_max_before_user_confirmation)
                self.log.info('trade quantity: ' + str(trade_quantity))

            # new sell entry orders
            if (z_score < -1) and (z_score<=z_score_s) and (qCarry<=-9) and (normalized_butterfly_price<butterfly_q25) and (min_avg_volume>=self.min_avg_volume_limit) and (working_position_short == 0) and (
                self.num_bets < self.max_num_bets):


                #self.log.info(self.total_traded_volume_max_before_user_confirmation)

                trade_quantity = min(mth.ceil((self.bet_size-total_risk_short)/dollar_noise100),self.total_traded_volume_max_before_user_confirmation)

                if (self.total_traded_volume_since_last_confirmation+trade_quantity < self.total_traded_volume_max_before_user_confirmation) and (trade_quantity > 0):

                    self.ocs_portfolio.order_send(ticker=ticker_head + '_short', qty=trade_quantity)
                    self.ocs_alias_portfolio.order_send(ticker=back_spread_ticker + '_short', qty=trade_quantity)
                    self.ocs_risk_portfolio.order_send(ticker=ticker_head + '_short', qty=trade_quantity*dollar_noise100)
                    self.log.info('real order!! id:' + str(self.next_val_id))
                    self.log.info('total_position_short:' + str(total_position_short) + ', working_position_short: ' + str(working_position_short))
                    overnight_calendars.loc[i, 'working_order_id'] = self.next_val_id
                    self.order_filled_dictionary[self.next_val_id] = 0

                    if (z_score_b<-1) and (z_score_b<=z_score_s) and (normalized_butterfly_price_b<butterfly_q25) and (bid_quantity<(ask_quantity/2)) and (bid_ask_in_ticks<1.1):
                        self.placeOrder(self.next_valid_id(), spread_contract, ib_api_trade.ComboLimitIcebergOrder('SELL', trade_quantity,min(bid_quantity,trade_quantity), bid_price, False))
                    else:
                        self.placeOrder(self.next_valid_id(), spread_contract,ib_api_trade.ComboLimitIcebergOrder('SELL', trade_quantity,min(bid_quantity,trade_quantity) ,ask_price, False))

                    self.total_volume_traded += trade_quantity
                    self.total_traded_volume_since_last_confirmation += trade_quantity
                    self.num_bets += 1
                    self.log.info(back_spread_ticker + ' is a sell, Spread Price: %.4f' % spread_price + ', Butterfly Price: %.4f' % butterfly_price + ', z_score: %.2f' % z_score + ', z_score_b: %.2f' % z_score_b)

            # new buy entry orders
            if (z_score > 1)  and (z_score>=z_score_s) and (qCarry>=19) and (normalized_butterfly_price>butterfly_q75)  and (min_avg_volume>=self.min_avg_volume_limit) and (ticker_head not in ['CL', 'HO', 'NG']) and (working_position_long == 0) and (self.num_bets < self.max_num_bets):

                trade_quantity = min(mth.ceil((self.bet_size - total_risk_long) / dollar_noise100),self.total_traded_volume_max_before_user_confirmation)

                if (self.total_traded_volume_since_last_confirmation+trade_quantity < self.total_traded_volume_max_before_user_confirmation) and (trade_quantity>0):
                    self.ocs_portfolio.order_send(ticker=ticker_head + '_long', qty=trade_quantity)
                    self.ocs_alias_portfolio.order_send(ticker=back_spread_ticker + '_long', qty=trade_quantity)
                    self.ocs_risk_portfolio.order_send(ticker=ticker_head + '_long',qty=trade_quantity * dollar_noise100)
                    self.log.info('real order!! id:' + str(self.next_val_id))
                    self.log.info('total_position_long:' + str(total_position_long) + ', working_position_long: ' + str(working_position_long))
                    overnight_calendars.loc[i, 'working_order_id'] = self.next_val_id
                    self.order_filled_dictionary[self.next_val_id] = 0

                    if (z_score_a>1) and (z_score_a>=z_score_s) and (normalized_butterfly_price_a>butterfly_q75) and (ask_quantity<(bid_quantity/2)) and (bid_ask_in_ticks<1.1):
                        self.placeOrder(self.next_valid_id(), spread_contract, ib_api_trade.ComboLimitIcebergOrder('BUY', trade_quantity, min(trade_quantity, ask_quantity), ask_price, False))
                    else:
                        self.placeOrder(self.next_valid_id(), spread_contract, ib_api_trade.ComboLimitIcebergOrder('BUY', trade_quantity, min(trade_quantity, ask_quantity), bid_price, False))

                    self.total_volume_traded += trade_quantity
                    self.total_traded_volume_since_last_confirmation += trade_quantity
                    self.num_bets += 1
                    self.log.info(back_spread_ticker + ' is a buy, Spread Price: %.4f' % spread_price + ', Butterfly Price: %.4f' % butterfly_price + ', z_score: %.2f' % z_score + ', z_score_a: %.2f' % z_score_a)

            # cancel working short entry orders before exiting filled position to ensure correct accounting
            if (working_alias_short>0) and (total_alias_short>0):
                self.log.info('Monitor Existing Short Entry Orders: ' + back_spread_ticker + ', Spread Price: %.4f' % spread_price + ', Butterfly Price: %.4f' % butterfly_price + ', z_score: %.2f' % z_score)

                if (z_score>=-0.5) or (mth.isnan(z_score)):
                    self.cancelOrder(int(overnight_calendars.loc[i, 'working_order_id']))
                    self.log.info(back_spread_ticker + ' normalized, cancelled entry orders with id ' + str(overnight_calendars.loc[i, 'working_order_id']))
                    overnight_calendars.loc[i, 'working_order_id'] = np.nan
                    self.ocs_portfolio.order_send(ticker=ticker_head + '_short', qty=-working_alias_short)
                    self.ocs_alias_portfolio.order_send(ticker=back_spread_ticker + '_short', qty=-working_alias_short)
                    self.ocs_risk_portfolio.order_send(ticker=ticker_head + '_short',qty=-working_alias_short * dollar_noise100)

            # cancel working long entry orders before exiting filled position to ensure correct accounting
            if (working_alias_long > 0) and (total_alias_long > 0):
                self.log.info('Monitor Existing Long Entry Orders: ' + back_spread_ticker + ', Spread Price: %.4f' % spread_price + ', Butterfly Price: %.4f' % butterfly_price + ', z_score: %.2f' % z_score)

                if (z_score <= 0.5) or (mth.isnan(z_score)):
                    self.cancelOrder(int(overnight_calendars.loc[i, 'working_order_id']))
                    self.log.info(back_spread_ticker + ' normalized, cancelled entry orders with id ' + str(overnight_calendars.loc[i, 'working_order_id']))
                    overnight_calendars.loc[i, 'working_order_id'] = np.nan
                    self.ocs_portfolio.order_send(ticker=ticker_head + '_long', qty=-working_alias_long)
                    self.ocs_alias_portfolio.order_send(ticker=back_spread_ticker + '_long',qty=-working_alias_long)
                    self.ocs_risk_portfolio.order_send(ticker=ticker_head + '_long',qty=-working_alias_long * dollar_noise100)

            # before closing a short position, make sure there's actaully a position on the books, the first condition ensures that we avoid sending close orders repeatedly
            if (filled_alias_short>0) and (total_alias_short>0):
                self.log.info('Monitor Existing Short Position: ' + back_spread_ticker + ', Spread Price: %.4f' % spread_price + ', Butterfly Price: %.4f' % butterfly_price + ', z_score: %.2f' % z_score)

                # Livestock original holdings time: 14
                # Ag, Energy original holding time: 28
                if ((z_score > 0) and (z_score>=z_score_s) and ((qCarry>-4) or (trDte1<20))) or \
                        ((ticker_class=='Livestock') and (holding_period>=14) and (z_score > 0) and (z_score>=z_score_s) and (realized_pnl>0)) or \
                        ((ticker_class in ['Ag','Energy']) and (holding_period>=28) and (z_score > 0) and (z_score>=z_score_s) and (realized_pnl>0)) or \
                        (trDte1<15) or (mth.isnan(z_score)):
                    self.log.info(back_spread_ticker + ' normalized, closing position')
                    overnight_calendars.loc[i, 'working_order_id'] = self.next_val_id
                    self.order_filled_dictionary[self.next_val_id] = 0
                    self.placeOrder(self.next_valid_id(), spread_contract,ib_api_trade.ComboLimitIcebergOrder('BUY', filled_alias_short, min(filled_alias_short,ask_quantity), bid_price, False))
                    self.total_volume_traded += filled_alias_short
                    self.total_traded_volume_since_last_confirmation += filled_alias_short
                    self.ocs_portfolio.order_send(ticker=ticker_head + '_short', qty=-filled_alias_short)
                    self.ocs_alias_portfolio.order_send(ticker=back_spread_ticker + '_short', qty=-filled_alias_short)
                    self.ocs_risk_portfolio.order_send(ticker=ticker_head + '_short',qty=-filled_alias_short * dollar_noise100)

            # before closing a long position, make sure there's actaully a position on the books, the first condition ensures that we avoid sending close orders repeatedly
            if (filled_alias_long > 0) and (total_alias_long > 0):
                self.log.info('Monitor Existing Long Position: ' + back_spread_ticker + ', Spread Price: %.4f' % spread_price + ', Butterfly Price: %.4f' % butterfly_price + ', z_score: %.2f' % z_score)

                if ((z_score < 0) and (z_score<=z_score_s) and ((qCarry<9) or (trDte1<20))) or \
                        ((ticker_class == 'Livestock') and (holding_period >= 14) and (z_score < 0) and (z_score<=z_score_s) and (realized_pnl>0)) or \
                        ((ticker_class in ['Ag', 'Energy']) and (holding_period >= 28) and (z_score < 0) and (z_score<=z_score_s) and (realized_pnl>0)) or \
                        (trDte1 < 15) or (mth.isnan(z_score)):
                    self.log.info(back_spread_ticker + ' normalized, closing position')
                    overnight_calendars.loc[i, 'working_order_id'] = self.next_val_id
                    self.order_filled_dictionary[self.next_val_id] = 0
                    self.placeOrder(self.next_valid_id(), spread_contract,ib_api_trade.ComboLimitIcebergOrder('SELL', filled_alias_long, min(filled_alias_long,bid_quantity), bid_price, False))
                    self.total_volume_traded += filled_alias_long
                    self.total_traded_volume_since_last_confirmation += filled_alias_long
                    self.ocs_portfolio.order_send(ticker=ticker_head + '_long', qty=-filled_alias_long)
                    self.ocs_alias_portfolio.order_send(ticker=back_spread_ticker + '_long',qty=-filled_alias_long)
                    self.ocs_risk_portfolio.order_send(ticker=ticker_head + '_long',qty=-filled_alias_long * dollar_noise100)

        self.overnight_calendars = overnight_calendars
        thr.Timer(10, self.periodic_call).start()

    def main_run(self):

        ticker_list = self.ticker_list
        self.nonfinished_ticker_list = ticker_list

        for i in range(len(ticker_list)):
            contract_i = ib_contract.get_ib_contract_from_db_ticker(ticker=ticker_list[i], sec_type='F')
            self.contractDetailReqIdDictionary[self.next_val_id] = ticker_list[i]
            self.nonfinished_contract_detail_ReqId_list.append(self.next_val_id)
            self.reqContractDetails(self.next_valid_id(), contract_i)


        #self.reqExecutions(10001, ExecutionFilter())

        #self.run()
        #self.disconnect()

    def request_spread_market_data(self):

        spread_ticker_list = self.price_request_dictionary['spread']

        self.nonfinished_bid_price_list.extend(spread_ticker_list)
        self.nonfinished_ask_price_list.extend(spread_ticker_list)
        self.nonfinished_bid_quantity_list.extend(spread_ticker_list)
        self.nonfinished_ask_quantity_list.extend(spread_ticker_list)

        for i in range(len(spread_ticker_list)):

            if self.num_messages>=30:
                tm.sleep(2)
                self.num_messages = 0

            split_out = spread_ticker_list[i].split("-")
            ticker1 = split_out[0]
            ticker2 = split_out[1]

            contract_specs_output = cmi.get_contract_specs(ticker1)
            ticker_head = contract_specs_output['ticker_head']
            exchange = cmi.get_ib_exchange_name(ticker_head)

            ib_ticker_head = su.get_key_in_dictionary(dictionary_input=tfl.conversion_from_tt_ticker_head,
                                                      value=contract_specs_output['ticker_head'])

            spread_contract = Contract()
            spread_contract.symbol = ib_ticker_head
            spread_contract.secType = "BAG"
            spread_contract.currency = "USD"
            spread_contract.exchange = exchange

            leg1 = ComboLeg()
            leg1.conId = self.contractIDDictionary[ticker1]
            leg1.ratio = 1
            leg1.action = "BUY"
            leg1.exchange = exchange

            leg2 = ComboLeg()
            leg2.conId = self.contractIDDictionary[ticker2]
            leg2.ratio = 1
            leg2.action = "SELL"
            leg2.exchange = exchange

            spread_contract.comboLegs = []
            spread_contract.comboLegs.append(leg1)
            spread_contract.comboLegs.append(leg2)

            self.market_data_ReqId_dictionary[self.next_val_id] = spread_ticker_list[i]
            self.log.info('req id: ' + str(self.next_val_id) + ', spread_ticker:' + str(spread_ticker_list[i]));
            self.reqMktData(self.next_valid_id(), spread_contract, "", False, False, [])
            self.num_messages += 1
            self.spread_contract_dictionary[spread_ticker_list[i]] = spread_contract

    def request_outright_market_data(self):

        outright_ticker_list = self.price_request_dictionary['outright']

        self.nonfinished_bid_price_list.extend(outright_ticker_list)
        self.nonfinished_ask_price_list.extend(outright_ticker_list)
        self.nonfinished_bid_quantity_list.extend(outright_ticker_list)
        self.nonfinished_ask_quantity_list.extend(outright_ticker_list)

        for i in range(len(outright_ticker_list)):

            if self.num_messages>=30:
                tm.sleep(2)
                self.num_messages = 0

            outright_ib_contract = ib_contract.get_ib_contract_from_db_ticker(ticker=outright_ticker_list[i], sec_type='F')
            self.market_data_ReqId_dictionary[self.next_val_id] = outright_ticker_list[i]
            self.log.info('req id: ' + str(self.next_val_id) + ', outright_ticker:' + str(outright_ticker_list[i]));
            self.reqMktData(self.next_valid_id(), outright_ib_contract, "", False, False, [])
            self.num_messages += 1

