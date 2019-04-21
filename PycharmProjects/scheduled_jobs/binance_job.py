
import binance_trading.save_data as sd
import binance_trading.get_data as gd
import shared.calendar_utilities as cu
import binance_trading.utils as btu
import time as tm

client = btu.get_binance_client()

sd.save_ticker_frame(client=client)

folder_date = cu.get_doubledate()

ticker_frame = gd.get_ticker_frame(date=folder_date)

for i in range(len(ticker_frame.index)):
    sd.save_daily_price_data4ticker(client=client,ticker=ticker_frame['symbol'].iloc[i])
    tm.sleep(0.5)
