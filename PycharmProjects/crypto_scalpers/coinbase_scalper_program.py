

import crypto_scalpers.coinbase_scalper_algo as algo
import api_utils.portfolio as aup
import threading as thr
import datetime as dt
import pandas as pd


def main():



    app = algo.Algo()
    app.ticker = 'ETH-BTC'
    app.min_order_size = 0.01

    app.periodic_call()
    app.slower_call()























if __name__ == "__main__":
    main()