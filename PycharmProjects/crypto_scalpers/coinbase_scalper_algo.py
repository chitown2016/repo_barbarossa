
import threading as thr
import coinbase.utils as coin_utils
import shared.directory_names as dn
import shared.calendar_utilities as cu
import coinbase.order_manager as om
import shared.log as lg
import pandas as pd
import numpy as np
import datetime as dt

class Algo():

    client = coin_utils.get_coinbase_client()
    ticker = ''
    min_order_size = np.nan
    working_position = 0
    total_position = 0
    tick_pnl = 0
    entry_price = 0
    add_position = False
    past_candle_high = np.nan
    past_candle_low = np.nan
    open_orders = []
    target_order = ''
    stop_order = ''
    buy_order_price = np.nan
    sell_order_price = np.nan
    stop_price = np.nan
    target_price = np.nan
    frame5 = pd.DataFrame()
    range10min = np.nan
    range10max = np.nan
    log = lg.get_logger(file_identifier='crypto_scalper', log_level='INFO')

    frame_out = pd.DataFrame(client.get_products())

    selected_frame = frame_out[(frame_out['quote_currency'] == 'BTC') & (frame_out['base_currency'] == 'ETH')]
    quote_increment = float(selected_frame['quote_increment'].iloc[0])

    date_now = cu.get_doubledate()
    ta_folder = dn.get_dated_directory_extension(folder_date=date_now, ext='ta')

    trade_file = ta_folder + '/crypto_trade_dir_' + dt.datetime.now().strftime('%H%M') + '_.csv'

    with open(trade_file, 'a') as file:
        file.write('EntryTime,ExitTime,EntryPrice,ExitPrice,TickPnl')
        file.write('\n')

    def periodic_call(self):

        try:
            order_book_out = self.client.get_product_order_book(self.ticker, level=1)
        except Exception as e:
            self.log.error(e)
            return

        current_best_bid = float(order_book_out["bids"][0][0])
        current_best_ask = order_book_out["asks"][0][0]

        range10_q = (100 * (current_best_bid - self.range10min) / (self.range10max - self.range10min))

        open_orders = self.open_orders

        for i in range(len(self.open_orders)):
            order_i = self.client.get_order(self.open_orders[i])

            if order_i["status"] == "done":
                open_orders.remove(self.open_orders[i])
                if order_i["side"] == "buy":
                    self.working_position -= self.min_order_size
                    self.sell_order_price = round(float(order_i["price"]) + 5 * self.quote_increment, 5)

                    order_out = om.send_post_only_limit_order(ticker=self.ticker, qty=-self.min_order_size, price=self.sell_order_price, client=self.client)

                    if order_out['success']:
                        open_orders.append(order_out["id"])
                        self.target_order = order_out["id"]
                    else:
                        self.log.info('Unable to send a post only order exiting the algorithm...')
                        exit()

                if order_i["side"] == "sell":
                    self.total_position -= self.min_order_size
                    self.tick_pnl += (self.sell_order_price-self.buy_order_price)/self.quote_increment
                    self.log.info('Realized tick pnl: ' + str(self.tick_pnl))

                    # cancel the stop or limit order depending on which one got executed
                    if order_i['id']==self.stop_order:
                        self.stop_order = ''
                        cancel_out = self.client.cancel_order(self.target_order)
                        if type(cancel_out) == dict:
                            self.log.info('Stop order was executed and no target order can be found')
                        else:
                            self.log.info('Stop order was executed and target order was cancelled')
                        self.target_order = ''
                    elif order_i['id'] == self.target_order:
                        self.target_order = ''
                        cancel_out = self.client.cancel_order(self.stop_order)
                        if type(cancel_out) == dict:
                            self.log.info('Target order was executed and no stop order can be found')
                        else:
                            self.log.info('Target order was executed and stop order was cancelled')
                        self.stop_order = ''

            if order_i["status"] == "open":
                if order_i["side"] == "buy":
                    if range10_q>50:
                        cancel_out = self.client.cancel_order(order_i['id'])
                        if type(cancel_out) == dict:
                            self.log.info('No initial buy order can be found')
                        else:
                            self.log.info('Initial buy order is cancelled')
                            open_orders.remove(order_i['id'])
                            self.working_position -= self.min_order_size
                            self.total_position -= self.min_order_size
                        continue




                    if self.buy_order_price<=current_best_bid-self.quote_increment:
                        maintenance_out = om.maintain_competitive_level4limit_order(client=self.client,order=order_i,new_price_str=order_book_out["bids"][0][0])
                        if maintenance_out['status'] == 'order replaced':
                            open_orders.remove(order_i['id'])
                            open_orders.append(maintenance_out['new_order_id'])



                        #cancel_out = self.client.cancel_order((self.open_orders[i]))
                        #print('long cancellation details...')
                        #print(cancel_out)
                        #open_orders.remove(self.open_orders[i])
                        #self.working_position -= self.min_order_size
                        #self.total_position -= self.min_order_size
                if order_i["side"] == "sell" and order_i["id"] == self.stop_order:
                    # make sure your stop order is competitive
                    if float(order_i["price"]) >= current_best_ask + self.quote_increment:
                        maintenance_out = om.maintain_competitive_level4limit_order(client=self.client, order=order_i, new_price_str=order_book_out["asks"][0][0])
                        if maintenance_out['status'] == 'order replaced':
                            open_orders.remove(order_i['id'])
                            open_orders.append(maintenance_out['new_order_id'])


                        self.client.cancel_order((self.stop_order))
                        open_orders.remove(self.stop_order)
                        order_out = self.client.place_limit_order(product_id=self.ticker, side='sell',price=str(current_best_ask),size=str(self.min_order_size), post_only=True)
                        self.log.info("Stop loss adjusted")
                        open_orders.append(order_out["id"])
                        self.stop_order = maintenance_out['new_order_id']

        self.open_orders = open_orders

        #print(self.client.get_time())
        #print(dt.datetime.now())

        time_str = dt.datetime.now().strftime('%H:%M:%S')


        print(time_str + ' range10q: ' + str(range10_q) + ', current best bid: ' + str(current_best_bid) + ', past candle high: ' + str(self.past_candle_high))

        self.log.info(time_str + ' range10q: ' + str(range10_q) + ', current best bid: ' + str(current_best_bid) + ', past candle high: ' + str(self.past_candle_high))

        if current_best_bid>self.past_candle_high and range10_q<30:
            if not self.add_position:
                self.add_position = True
                self.log.info('Buy signal triggered')
                print('Buy signal triggered')

        if current_best_bid<self.past_candle_low or range10_q>50:
            if self.add_position:
                self.add_position = False
                self.log.info('Buy signal removed')

        if self.total_position<self.min_order_size and self.add_position:
            order_out = self.client.place_limit_order(product_id=self.ticker,side='buy',price=str(current_best_bid),size=str(self.min_order_size),post_only=True)
            self.buy_order_price = current_best_bid

            self.target_price = self.range10max
            self.stop_price = current_best_bid-(self.range10max-current_best_bid)

            self.open_orders.append(order_out["id"])
            self.working_position += self.min_order_size
            self.total_position += self.min_order_size

        if current_best_bid<self.stop_price and len(self.stop_order)<1:
            self.open_orders.remove(self.target_order)
            cancel_out = self.client.cancel_order(self.target_order)
            order_out = self.client.place_limit_order(product_id=self.ticker, side='sell', price=str(current_best_ask), size=str(self.min_order_size),post_only=True)
            self.log.info("Stop loss triggered")
            self.open_orders.append(order_out["id"])
            self.stop_order = order_out["id"]

        thr.Timer(10, self.periodic_call).start()

    def slower_call(self):

        data_raw = self.client.get_product_historic_rates(self.ticker, granularity=300, start=dt.datetime.now() -dt.timedelta(minutes=30))

        frame_out = pd.DataFrame(data_raw, columns=['time', 'low', 'high', 'open', 'close', 'volume'])

        frame_out['time'] = [dt.datetime.utcfromtimestamp(x) for x in frame_out['time']]
        frame_out.sort_values(by='time', ascending=True, inplace=True)
        frame_out.reset_index(drop=True, inplace=True)
        self.frame5 = frame_out

        self.past_candle_high = frame_out['high'].iloc[-2]
        self.past_candle_low = frame_out['low'].iloc[-2]

        self.range10max = frame_out['high'].iloc[-20:-1].max()
        self.range10min = frame_out['low'].iloc[-20:-1].min()

        thr.Timer(300, self.slower_call).start()
