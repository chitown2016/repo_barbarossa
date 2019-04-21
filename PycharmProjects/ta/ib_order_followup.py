
import ib_api_utils.subscription as subs
import ib_api_utils.ib_contract as ib_contract
from ibapi.execution import Execution
from ibapi.execution import ExecutionFilter
import shared.directory_names as dn
import shared.calendar_utilities as cu
import my_sql_routines.my_sql_utilities as msu
import contract_utilities.contract_meta_info as cmi
import ta.strategy as ts
from ibapi.contract import *
from ibapi.common import *
import pandas as pd
import os.path as pth

class ib_order_follow_up(subs.subscription):

    trade_frame = pd.DataFrame()

    trade_assignment_by_tickerhead = {'LN': "LNQ19V19VCS"}

    def orderStatus(self, orderId: OrderId, status: str, filled: float,remaining: float, avgFillPrice: float, permId: int,parentId: int, lastFillPrice: float, clientId: int,whyHeld: str, mktCapPrice: float):
        super().orderStatus(orderId, status, filled, remaining,avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice)
        print("OrderStatus. Id:", orderId, "Status:", status, "Filled:", filled,
        "Remaining:", remaining, "AvgFillPrice:", avgFillPrice,
        "PermId:", permId, "ParentId:", parentId, "LastFillPrice:",
        lastFillPrice, "ClientId:", clientId, "WhyHeld:", whyHeld)

    def execDetails(self, reqId: int, contract: Contract, execution: Execution):
        super().execDetails(reqId, contract, execution)

        trade_frame = self.trade_frame

        if contract.secType != 'BAG':    # contract.secType != 'BAG':  contract.secType == 'FOP':
            print(contract)
            db_ticker_output = ib_contract.get_db_ticker_from_ib_contract(ib_contract=contract)
            trade_frame.loc[len(trade_frame.index), 'ticker'] = db_ticker_output['ticker']

            if execution.side == 'BOT':
                qty = execution.shares
            elif execution.side == 'SLD':
                qty = -execution.shares

            trade_frame.loc[len(trade_frame.index) - 1, 'option_type'] = db_ticker_output['option_type']
            trade_frame.loc[len(trade_frame.index) - 1, 'strike_price'] = db_ticker_output['strike']
            trade_frame.loc[len(trade_frame.index) - 1, 'trade_quantity'] = qty
            trade_frame.loc[len(trade_frame.index) - 1, 'trade_price'] = execution.price
            trade_frame.loc[len(trade_frame.index) - 1, 'instrument'] = db_ticker_output['instrument']
            trade_frame.loc[len(trade_frame.index) - 1, 'order_id'] = execution.orderId

    def execDetailsEnd(self, reqId: int):
        super().execDetailsEnd(reqId)
        print("ExecDetailsEnd. ", reqId)
        trade_frame = self.trade_frame
        trade_frame['real_tradeQ'] = True

        trade_frame['ticker_head'] = [cmi.get_contract_specs(x)['ticker_head'] for x in trade_frame['ticker']]

        if pth.exists(self.trade_file):
            order_frame = pd.read_csv(self.trade_file,names=['order_id','ticker','urgency','alias','strategy_class','strategy_description'])

            print(order_frame['order_id'])
            print(trade_frame['order_id'])

            for i in range(len(trade_frame.index)):
                selected_frame = order_frame[order_frame['order_id'] == trade_frame['order_id'].iloc[i]]

                if len(selected_frame.index)>0:
                    if selected_frame['strategy_class'].iloc[0]=='delta_hedge':
                        trade_frame.loc[i,'alias'] = self.delta_alias
        elif self.trade_assignment_by_tickerhead:
            for key, value in self.trade_assignment_by_tickerhead.items():
                trade_frame.loc[trade_frame['ticker_head'] == key,'alias'] = value

        else:
            trade_frame['alias'] = ''

        trade_frame = trade_frame[pd.notnull(trade_frame['alias'])]
        trade_frame.reset_index(inplace=True, drop=True)
        ts.load_trades_2strategy(trade_frame=trade_frame, con=self.con)




    def main_run(self):
        self.reqAllOpenOrders()
        self.reqExecutions(self.next_valid_id(), ExecutionFilter())


def test_ib_order_follow_up():
    app = ib_order_follow_up()
    date_now = cu.get_doubledate()
    con = msu.get_my_sql_connection()
    delta_strategy_frame = ts.get_filtered_open_strategies(as_of_date=date_now, con=con, strategy_class_list=['delta'])

    if len(delta_strategy_frame.index)>0:
        app.delta_alias = delta_strategy_frame['alias'].iloc[-1]

    ta_folder = dn.get_dated_directory_extension(folder_date=date_now, ext='ta')
    app.trade_file = ta_folder + '/trade_dir.csv'
    app.con = con

    app.connect(client_id=7)
    app.run()

if __name__ == "__main__":
    test_ib_order_follow_up()