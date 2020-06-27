__author__ = 'kocat_000'

import warnings
with warnings.catch_warnings():
    warnings.filterwarnings("ignore",category=FutureWarning)
    import h5py

warnings.filterwarnings("ignore", message="numpy.ufunc size changed")

import shared.log as lg
log = lg.get_logger(file_identifier='morning_job',log_level='INFO')

import formats.futures_strategy_formats as fsf
import formats.strategy_followup_formats as sff
import formats.risk_pnl_formats as rpf
import my_sql_routines.futures_price_loader as fpl
import my_sql_routines.my_sql_utilities as msu
import get_price.presave_price as pp
import opportunity_constructs.futures_butterfly as fb
import opportunity_constructs.overnight_calendar_spreads as ocs
import contract_utilities.expiration as exp
import ta.prepare_daily as prep
import ta.underlying_proxy as up
import fundamental_data.cot_data as cot
import datetime as dt
import ta.email_reports as er

con = msu.get_my_sql_connection()

fpl.update_futures_price_database(con=con)
pp.generate_and_update_futures_data_files(ticker_head_list='butterfly')

report_date = exp.doubledate_shift_bus_days()
fb.generate_futures_butterfly_sheet_4date(date_to=report_date, con=con)

try:
    log.info('generate_overnight_spreads_sheet')
    ocs.generate_overnight_spreads_sheet_4date(date_to=report_date, con=con)
    fsf.generate_ocs_formatted_output(report_date=report_date)
except Exception:
    log.error('generate_overnight_spreads_sheet failed', exc_info=True)
    quit()

fsf.generate_futures_butterfly_formatted_output()

prep.prepare_strategy_daily(strategy_class='futures_butterfly')

try:
    log.info('generate_spread_carry')
    fsf.generate_spread_carry_formatted_output(report_date=report_date)
except Exception:
    log.error('generate_spread_carry failed', exc_info=True)
    quit()

try:
    log.info('generate_curve_pca')
    fsf.generate_curve_pca_formatted_output()
    prep.prepare_strategy_daily(strategy_class='curve_pca')
except Exception:
    log.error('generate_curve_pca failed', exc_info=True)
    quit()

try:
    log.info('generate_historic_risk_report')
    rpf.generate_historic_risk_report(as_of_date=report_date, con=con)
    prep.move_from_dated_folder_2daily_folder(ext='ta', file_name='risk', folder_date=report_date)
except Exception:
    log.error('generate_historic_risk_report failed', exc_info=True)
    quit()

try:
    log.info('generate_portfolio_pnl_report')
    rpf.generate_portfolio_pnl_report(as_of_date=report_date, con=con, name='final')
    prep.move_from_dated_folder_2daily_folder(ext='ta', file_name='pnl_final', folder_date=report_date)
except Exception:
    log.error('generate_portfolio_pnl_report failed', exc_info=True)
    quit()

try:
    log.info('followup_report')
    writer_out = sff.generate_futures_butterfly_followup_report(as_of_date=report_date, con=con)
    writer_out = sff.generate_spread_carry_followup_report(as_of_date=report_date, con=con, writer=writer_out)
    writer_out = sff.generate_vcs_followup_report(as_of_date=report_date, con=con, writer=writer_out)
    sff.generate_ocs_followup_report(as_of_date=report_date, con=con, broker='abn',writer=writer_out)
    prep.move_from_dated_folder_2daily_folder(ext='ta', file_name='followup', folder_date=report_date)
except Exception:
    log.error('followup_report failed', exc_info=True)
    quit()

try:
    log.info('send_wh_report')
    er.send_wh_report(report_date=report_date)
except Exception:
    log.error('send_wh_report failed', exc_info=True)
    quit()

try:
    log.info('send_followup_report')
    er.send_followup_report(report_date=report_date)
except Exception:
    log.error('send_followup_report failed', exc_info=True)
    quit()

try:
    log.info('generate_underlying_proxy_report')
    up.generate_underlying_proxy_report(report_date=report_date, con=con)
except Exception:
    log.error('generate_underlying_proxy_report failed', exc_info=True)
    quit()

try:
    log.info('presave_cot_data')
    if dt.datetime.today().weekday() == 5:
        cot.presave_cot_data()
except Exception:
    log.error('presave_cot_data failed', exc_info=True)
    quit()

con.close()

