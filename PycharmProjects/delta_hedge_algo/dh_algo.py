
import ib_api_utils.subscription as subs
import copy as cpy
import ib_api_utils.ib_contract as ib_contract
import ib_api_utils.trade as ib_api_trade
import contract_utilities.contract_meta_info as cmi
import shared.utils as su
import ta.trade_fill_loader as tfl
import ta.strategy_hedger as tsh
import ta.strategy as tas
from ibapi.contract import *
from ibapi.common import *
from ibapi.ticktype import *
import numpy as np
import pandas as pd
import math as m

class Algo(subs.subscription):

    contractDetailReqIdDictionary = {}
    market_data_ReqId_dictionary = {}
    nonfinished_contract_detail_ReqId_list = []
    contractIDDictionary = {}

    bid_price_dictionary = {}
    ask_price_dictionary = {}
    fair_price_dictionary = {}
    bid_quantity_dictionary = {}
    ask_quantity_dictionary = {}
    outright_contract_dictionary = {}
    spread_contract_dictionary = {}

    nonfinished_bid_price_list = []
    nonfinished_ask_price_list = []
    nonfinished_bid_quantity_list = []
    nonfinished_ask_quantity_list = []
    price_request_dictionary = {'spread': [], 'outright': []}

    order_ticker_dictionary = {}
    order_urgency_dictionary = {}

    option_model_call_initiated_q = False

    def contractDetails(self, reqId: int, contractDetails: ContractDetails):
        super().contractDetails(reqId, contractDetails)
        self.contractIDDictionary[self.contractDetailReqIdDictionary[reqId]] = contractDetails.contract.conId
        contract_specs_output = cmi.get_contract_specs(self.contractDetailReqIdDictionary[reqId])

    def contractDetailsEnd(self, reqId: int):
            super().contractDetailsEnd(reqId)
            self.nonfinished_contract_detail_ReqId_list.remove(reqId)
            self.nonfinished_ticker_list.remove(self.contractDetailReqIdDictionary[reqId])
            if len(self.nonfinished_contract_detail_ReqId_list)==0:
                self.request_spread_market_data()
                self.request_outright_market_data()

    def orderStatus(self, orderId: OrderId, status: str, filled: float,remaining: float, avgFillPrice: float, permId: int,parentId: int, lastFillPrice: float, clientId: int,whyHeld: str, mktCapPrice: float):
        super().orderStatus(orderId, status, filled, remaining,avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice)
        print("OrderStatus. Id:", orderId, "Status:", status, "Filled:", filled,
        "Remaining:", remaining, "AvgFillPrice:", avgFillPrice,
        "PermId:", permId, "ParentId:", parentId, "LastFillPrice:",
        lastFillPrice, "ClientId:", clientId, "WhyHeld:", whyHeld)

        if status in ['Submitted','PreSubmitted']:
            with open(self.trade_file, 'a') as file:
                file.write(str(orderId) + ',' + self.order_ticker_dictionary[orderId] + ',' + str(self.order_urgency_dictionary[orderId]) + ',' + self.delta_alias + ',delta_hedge,strategy_class=delta')
                file.write('\n')

        print(orderId)
        print(status)

    def calculate_mid_price(self,ticker_str):

        if (self.bid_quantity_dictionary[ticker_str]==0) or (self.ask_quantity_dictionary[ticker_str]==0):
            return np.nan
        else:
            return (self.bid_price_dictionary[ticker_str] * self.bid_quantity_dictionary[ticker_str] + \
         self.ask_price_dictionary[ticker_str] * self.ask_quantity_dictionary[ticker_str]) / \
        (self.bid_quantity_dictionary[ticker_str] + self.ask_quantity_dictionary[ticker_str])


    def tickPrice(self, reqId: TickerId, tickType: TickType, price: float,
                  attrib: TickAttrib):
        super().tickPrice(reqId, tickType, price, attrib)

        ticker_str = self.market_data_ReqId_dictionary[reqId]

        if tickType==1:
            self.bid_price_dictionary[ticker_str] = price
            if ticker_str in self.nonfinished_bid_price_list:
                self.nonfinished_bid_price_list.remove(ticker_str)
        elif tickType==2:
            self.ask_price_dictionary[ticker_str] = price
            if ticker_str in self.nonfinished_ask_price_list:
                self.nonfinished_ask_price_list.remove(ticker_str)

        if (tickType==1) or (tickType==2):

            self.fair_price_dictionary[ticker_str] = self.calculate_mid_price(ticker_str)

        if (len(self.nonfinished_bid_price_list)==0)&(len(self.nonfinished_ask_price_list)==0)&\
                (len(self.nonfinished_bid_quantity_list)==0)&(len(self.nonfinished_ask_quantity_list)==0)&(~self.option_model_call_initiated_q):

            fair_price_dictionary = self.fair_price_dictionary
            print(self.bid_price_dictionary)
            underlying_ticker_list = []
            mid_price_list = []

            for key in fair_price_dictionary:
                selection_indx = self.contract_frame['ticker'] == key
                self.contract_frame['mid_price'].loc[selection_indx] = fair_price_dictionary[key]
                self.contract_frame['bid_p'].loc[selection_indx] = self.bid_price_dictionary[key]
                self.contract_frame['ask_p'].loc[selection_indx] = self.ask_price_dictionary[key]
                self.contract_frame['bid_q'].loc[selection_indx] = self.bid_quantity_dictionary[key]
                self.contract_frame['ask_q'].loc[selection_indx] = self.ask_quantity_dictionary[key]

            #self.contract_frame['underlying_ticker'] = self.contract_frame['ticker']
            self.contract_frame['spread_cost'] = self.contract_frame['contract_multiplier']*(self.contract_frame['ask_p']-self.contract_frame['bid_p'])
            self.contract_frame['urgency_aux'] = self.contract_frame['risk']/self.contract_frame['spread_cost']

            self.contract_frame['urgency'] = 0
            self.contract_frame['urgency'].loc[self.contract_frame['urgency_aux']>5] = 100

            self.contract_frame.rename(columns={'ticker':'underlying_ticker'},inplace=True)

            self.option_model_call_initiated_q = True
            #print(self.contract_frame)
            tsh.strategy_hedge_report(intraday_price_frame=self.contract_frame, delta_alias=self.delta_alias, con=self.con)
            position_frame = tas.get_net_position_4strategy_alias(alias=self.delta_alias, as_of_date=self.current_date, con=self.con)
            print(position_frame)

            hedge_frame = tsh.get_hedge_frame(con=self.con)
            hedge_aux = self.contract_frame
            hedge_aux.rename(columns={'underlying_ticker': 'ticker'}, inplace=True)
            hedge_aux = hedge_aux[['ticker','urgency']]

            hedge_frame = pd.merge(hedge_frame,hedge_aux,how='inner',on='ticker')

            unique_ticker_head_list = hedge_frame['ticker_head'].unique()

            for i in range(len(unique_ticker_head_list)):
                hedge_frame_ticker_head = hedge_frame[hedge_frame['ticker_head'] == unique_ticker_head_list[i]]

                for j in range(len(hedge_frame_ticker_head.index)):
                    self.log.info(str(hedge_frame_ticker_head['hedge'].iloc[j]) + ' ' + hedge_frame_ticker_head['ticker'].iloc[j]
                                  + ' with urgency ' + '% 4.1f' %  hedge_frame_ticker_head['urgency'].iloc[j])

            continue_q = 'n'

            if len(hedge_frame.index)==0:
                self.log.info('No hedging needed today')
            else:
                continue_q = input('Do you agree? (y/n): ')

            if continue_q == 'y':
                self.log.info('Hedging now')

                for i in range(len(hedge_frame.index)):
                    ticker_i = hedge_frame['ticker'].iloc[i]

                    split_output = ticker_i.split('-')

                    contract_specs_output = cmi.get_contract_specs(split_output[0])
                    tick_size = cmi.tick_size[contract_specs_output['ticker_head']]

                    action_str = ''
                    trade_quantity = abs(hedge_frame['hedge'].iloc[i])

                    if hedge_frame['hedge'].iloc[i]>0:
                        if hedge_frame['urgency'].iloc[i] == 100:
                            limit_price = self.ask_price_dictionary[ticker_i]
                        else:
                            limit_price = m.ceil(self.fair_price_dictionary[ticker_i] / tick_size) * tick_size
                        action_str = 'BUY'
                    else:
                        if hedge_frame['urgency'].iloc[i] == 100:
                            limit_price = self.bid_price_dictionary[ticker_i]
                        else:
                            limit_price = m.floor(self.fair_price_dictionary[ticker_i] / tick_size) * tick_size
                        action_str = 'SELL'

                    if hedge_frame['is_spread_q'].iloc[i]:
                        contract_i = self.spread_contract_dictionary[ticker_i]
                        order = ib_api_trade.ComboLimitOrder(action_str, trade_quantity, limit_price, False)
                    else:
                        contract_i = self.outright_contract_dictionary[ticker_i]
                        order = ib_api_trade.LimitOrder(action_str, trade_quantity, limit_price)

                    self.order_ticker_dictionary[self.next_val_id] = ticker_i
                    self.order_urgency_dictionary[self.next_val_id] = hedge_frame['urgency'].iloc[i]
                    self.placeOrder(self.next_valid_id(), contract_i, order)




                    #print(m.floor(self.fair_price_dictionary[ticker_i]/tick_size)*tick_size)

                    #print(m.ceil(self.fair_price_dictionary[ticker_i]/tick_size) * tick_size)






    def tickSize(self, reqId: TickerId, tickType: TickType, size: int):
        super().tickSize(reqId, tickType, size)

        ticker_str = self.market_data_ReqId_dictionary[reqId]

        if tickType==0:
            self.bid_quantity_dictionary[ticker_str] = size
            if ticker_str in self.nonfinished_bid_quantity_list:
                self.nonfinished_bid_quantity_list.remove(ticker_str)
        if tickType==3:
            self.ask_quantity_dictionary[ticker_str] = size
            if ticker_str in self.nonfinished_ask_quantity_list:
                self.nonfinished_ask_quantity_list.remove(ticker_str)

        if (tickType==0) or (tickType==3):
            self.fair_price_dictionary[ticker_str] = self.calculate_mid_price(ticker_str)

    def request_outright_market_data(self):

        outright_ticker_list = self.price_request_dictionary['outright']

        self.nonfinished_bid_price_list.extend(outright_ticker_list)
        self.nonfinished_ask_price_list.extend(outright_ticker_list)
        self.nonfinished_bid_quantity_list.extend(outright_ticker_list)
        self.nonfinished_ask_quantity_list.extend(outright_ticker_list)

        for i in range(len(outright_ticker_list)):

            self.bid_price_dictionary[outright_ticker_list[i]] = np.nan
            self.ask_price_dictionary[outright_ticker_list[i]] = np.nan
            self.bid_quantity_dictionary[outright_ticker_list[i]] = np.nan
            self.ask_quantity_dictionary[outright_ticker_list[i]] = np.nan
            self.fair_price_dictionary[outright_ticker_list[i]] = np.nan

            outright_ib_contract = ib_contract.get_ib_contract_from_db_ticker(ticker=outright_ticker_list[i], sec_type='F')
            self.outright_contract_dictionary[outright_ticker_list[i]] = outright_ib_contract
            self.market_data_ReqId_dictionary[self.next_val_id] = outright_ticker_list[i]
            self.log.info('req id: ' + str(self.next_val_id) + ', outright_ticker:' + str(outright_ticker_list[i]))
            self.reqMktData(self.next_valid_id(), outright_ib_contract, "", False, False, [])

    def request_spread_market_data(self):

        spread_ticker_list = self.price_request_dictionary['spread']

        self.nonfinished_bid_price_list.extend(spread_ticker_list)
        self.nonfinished_ask_price_list.extend(spread_ticker_list)
        self.nonfinished_bid_quantity_list.extend(spread_ticker_list)
        self.nonfinished_ask_quantity_list.extend(spread_ticker_list)

        for i in range(len(spread_ticker_list)):

            self.bid_price_dictionary[spread_ticker_list[i]] = np.nan
            self.ask_price_dictionary[spread_ticker_list[i]] = np.nan
            self.bid_quantity_dictionary[spread_ticker_list[i]] = np.nan
            self.ask_quantity_dictionary[spread_ticker_list[i]] = np.nan
            self.fair_price_dictionary[spread_ticker_list[i]] = np.nan

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
            self.log.info('req id: ' + str(self.next_val_id) + ', spread_ticker:' + str(spread_ticker_list[i]))
            self.reqMktData(self.next_valid_id(), spread_contract, "", False, False, [])
            self.spread_contract_dictionary[spread_ticker_list[i]] = spread_contract


    def main_run(self):
        print('emre')
        ticker_list = self.ticker_list
        self.nonfinished_ticker_list = cpy.deepcopy(ticker_list)

        for i in range(len(ticker_list)):
            contract_i = ib_contract.get_ib_contract_from_db_ticker(ticker=ticker_list[i], sec_type='F')
            self.contractDetailReqIdDictionary[self.next_val_id] = ticker_list[i]
            self.nonfinished_contract_detail_ReqId_list.append(self.next_val_id)
            self.reqContractDetails(self.next_valid_id(), contract_i)