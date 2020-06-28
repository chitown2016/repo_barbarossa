

import warnings
with warnings.catch_warnings():
    warnings.filterwarnings("ignore",category=FutureWarning)
    import statsmodels.api

warnings.filterwarnings("ignore", message="numpy.ufunc size changed")


import shared.log as lg
log = lg.get_logger(file_identifier='evening_job_2',log_level='INFO')

import shared.directory_names as dn
import shared.downloads as sd
import shared.calendar_utilities as cu
import pickle as pickle
import my_sql_routines.futures_price_loader as fpl
import my_sql_routines.options_price_loader as opl
import my_sql_routines.options_greek_loader as ogl
import my_sql_routines.options_signal_loader as osl
import my_sql_routines.my_sql_utilities as msu
import get_price.presave_price as pp
import opportunity_constructs.vcs as vcs
import formats.options_strategy_formats as osf
import formats.futures_strategy_formats as fsf
import formats.intraday_futures_strategy_formats as ifsf
import contract_utilities.expiration as exp
import ta.prepare_daily as prep
import datetime as dt
import fundamental_data.cot_data as cot
import get_price.save_stock_data as ssd
import my_sql_routines.options_pnl_loader as opnl

commodity_address = 'ftp://ftp.cmegroup.com/pub/settle/stlags'
equity_address = 'ftp://ftp.cmegroup.com/pub/settle/stleqt'
fx_address = 'ftp://ftp.cmegroup.com/pub/settle/stlcur'
interest_rate_address = 'ftp://ftp.cmegroup.com/pub/settle/stlint'
comex_futures_csv_address = 'ftp://ftp.cmegroup.com/pub/settle/comex_future.csv'
comex_options_csv_address = 'ftp://ftp.cmegroup.com/pub/settle/comex_option.csv'
nymex_futures_csv_address = 'ftp://ftp.cmegroup.com/pub/settle/nymex_future.csv'
nymex_options_csv_address = 'ftp://ftp.cmegroup.com/pub/settle/nymex_option.csv'

#folder_date = cu.get_doubledate()
folder_date = exp.doubledate_shift_bus_days()

options_data_dir = dn.get_dated_directory_extension(folder_date=folder_date, ext='raw_options_data')

commodity_output = sd.download_txt_from_web(web_address=commodity_address)
equity_output = sd.download_txt_from_web(web_address=equity_address)
fx_output = sd.download_txt_from_web(web_address=fx_address)
interest_rate_output = sd.download_txt_from_web(web_address=interest_rate_address)

comex_futures_output = sd.download_csv_from_web(web_address=comex_futures_csv_address)
comex_options_output = sd.download_csv_from_web(web_address=comex_options_csv_address)

nymex_futures_output = sd.download_csv_from_web(web_address=nymex_futures_csv_address)
nymex_options_output = sd.download_csv_from_web(web_address=nymex_options_csv_address)

with open(options_data_dir + '/commodity.pkl', 'wb') as handle:
        pickle.dump(commodity_output, handle)

with open(options_data_dir + '/equity.pkl', 'wb') as handle:
        pickle.dump(equity_output, handle)

with open(options_data_dir + '/fx.pkl', 'wb') as handle:
        pickle.dump(fx_output, handle)

with open(options_data_dir + '/interest_rate.pkl', 'wb') as handle:
        pickle.dump(interest_rate_output, handle)

with open(options_data_dir + '/comex_futures.pkl', 'wb') as handle:
        pickle.dump(comex_futures_output, handle)

with open(options_data_dir + '/comex_options.pkl', 'wb') as handle:
        pickle.dump(comex_options_output, handle)

with open(options_data_dir + '/nymex_futures.pkl', 'wb') as handle:
        pickle.dump(nymex_futures_output, handle)

with open(options_data_dir + '/nymex_options.pkl', 'wb') as handle:
        pickle.dump(nymex_options_output, handle)

con = msu.get_my_sql_connection()

try:
    log.info('update_futures_price_database')
    fpl.update_futures_price_database_from_cme_file(con=con, settle_date=folder_date)
    pp.generate_and_update_futures_data_files(ticker_head_list='cme_futures')
except Exception:
    log.error('update_futures_price_database failed', exc_info=True)

try:
    log.info('update_options_price_database')
    opl.update_options_price_database_from_cme_files(con=con, settle_date=folder_date)
except Exception:
    log.error('update_options_price_database failed', exc_info=True)

try:
    log.info('update_options_greeks')
    ogl.update_options_greeks_4date(con=con, settle_date=folder_date)
except Exception:
    log.error('update_options_greeks failed', exc_info=True)

try:
    log.info('load_ticker_signals')
    osl.load_ticker_signals_4settle_date(con=con, settle_date=folder_date)
except Exception:
    log.error('load_ticker_signals failed', exc_info=True)

try:
    log.info('generate_vcs_sheet')
    vcs.generate_vcs_sheet_4date(con=con,date_to=folder_date)
except Exception:
    log.error('generate_vcs_sheet failed', exc_info=True)

try:
    osf.generate_vcs_formatted_output(report_date=folder_date)
    prep.prepare_strategy_daily(strategy_class='vcs', report_date=folder_date)
except Exception:
    pass

try:
    log.info('generate_scv_sheet')
    osf.generate_scv_formatted_output(report_date=folder_date)
    prep.prepare_strategy_daily(strategy_class='scv', report_date=folder_date)
except Exception:
    log.error('generate_scv_sheet failed', exc_info=True)

try:
    log.info('update options pnls')
    opnl.update_options_pnls_4date(con=con, settle_date=folder_date)
except Exception:
    log.error('update options pnls failed', exc_info=True)
    pass

try:
    log.info('save stock list')
    ssd.get_symbol_frame(frame_type='other', settle_date=folder_date)
    ssd.get_symbol_frame(frame_type='nasdaq', settle_date=folder_date)
except Exception:
    log.error('save stock list failed', exc_info=True)

try:
    if dt.datetime.today().weekday() == 5:
        cot.presave_cot_data()
except Exception:
    pass

con.close()
