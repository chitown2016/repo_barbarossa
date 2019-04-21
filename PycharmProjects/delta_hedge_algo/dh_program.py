
import delta_hedge_algo.dh_algo as dh_algo
import delta_hedge_algo.utils as dh_ut
import my_sql_routines.my_sql_utilities as msu
import shared.calendar_utilities as cu
import shared.directory_names as dn
import ta.strategy_hedger as tsh
import ta.strategy as tas
import shared.log as lg
import numpy as np


def main():

    app = dh_algo.Algo()
    con = msu.get_my_sql_connection()
    date_now = cu.get_doubledate()

    contract_frame = tsh.get_intraday_data_contract_frame()

    contract_frame = dh_ut.calculate_contract_risk(contract_frame=contract_frame, current_date=date_now)

    contract_frame['bid_p'] = np.nan
    contract_frame['ask_p'] = np.nan
    contract_frame['mid_price'] = np.nan
    contract_frame['spread_cost'] = np.nan

    contract_frame['bid_q'] = np.nan
    contract_frame['ask_q'] = np.nan

    contract_frame_outright = contract_frame[contract_frame['is_spread_q'] == False]
    outright_ticker_list = list(contract_frame_outright['ticker'].values)
    contract_frame_spread = contract_frame[contract_frame['is_spread_q']]

    ta_folder = dn.get_dated_directory_extension(folder_date=date_now,ext='ta')

    app.ticker_list = outright_ticker_list
    app.price_request_dictionary['spread'] = list(contract_frame_spread['ticker'].values)
    app.price_request_dictionary['outright'] = outright_ticker_list
    app.contract_frame = contract_frame

    app.log = lg.get_logger(file_identifier='ib_delta_hedge', log_level='INFO')
    app.trade_file = ta_folder + '/trade_dir.csv'
    app.delta_alias = tsh.get_delta_strategy_alias(con=con)
    app.con = con
    app.current_date = date_now
    app.connect(client_id=6)
    app.run()




if __name__ == "__main__":
    main()