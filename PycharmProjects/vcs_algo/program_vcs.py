
import contract_utilities.expiration as exp
import contract_utilities.contract_meta_info as cmi
import interest_curve.get_rate_from_stir as grfs
import my_sql_routines.my_sql_utilities as msu
import get_price.get_futures_price as gfp
import opportunity_constructs.vcs as vcs
import ta.underlying_proxy as up
import option_models.utils as omu
import signals.options_filters as of
import vcs_algo.algo as algo
import shared.calendar_utilities as cu
import shared.directory_names_aux as dna
import shared.directory_names as dn
import shared.utils as su
import shared.log as lg
import numpy as np

def main():
    app = algo.Algo()
    report_date = exp.doubledate_shift_bus_days()
    todays_date = cu.get_doubledate()
    con = msu.get_my_sql_connection()
    vcs_output = vcs.generate_vcs_sheet_4date(date_to=report_date)
    vcs_pairs = vcs_output['vcs_pairs']

    filter_out = of.get_vcs_filters(data_frame_input=vcs_pairs, filter_list=['long2', 'short2'])
    vcs_pairs = filter_out['selected_frame']

    vcs_pairs = vcs_pairs[vcs_pairs['downside'].notnull() & vcs_pairs['upside'].notnull()]
    # &(vcs_pairs.tickerClass!='Energy')
    vcs_pairs = vcs_pairs[(vcs_pairs['trDte1'] >= 50)&(vcs_pairs.tickerClass!='Metal')&(vcs_pairs.tickerClass!='FX')&(vcs_pairs.tickerClass!='Energy')]
    vcs_pairs = vcs_pairs[
        ((vcs_pairs['Q'] <= 30) & (vcs_pairs['fwdVolQ'] >= 30)) | (
        (vcs_pairs['Q'] >= 70) & (vcs_pairs['fwdVolQ'] <= 70))]
    vcs_pairs.reset_index(drop=True, inplace=True)

    vcs_pairs['underlying_ticker1'] = [omu.get_option_underlying(ticker=x) for x in vcs_pairs['ticker1']]
    vcs_pairs['underlying_ticker2'] = [omu.get_option_underlying(ticker=x) for x in vcs_pairs['ticker2']]

    vcs_pairs['underlying_tickerhead'] = [cmi.get_contract_specs(x)['ticker_head'] for x in vcs_pairs['underlying_ticker1']]
    futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in vcs_pairs['underlying_tickerhead'].unique()}

    proxy_output_list1 = [up.get_underlying_proxy_ticker(ticker=x, settle_date=report_date,futures_data_dictionary=futures_data_dictionary) for x in vcs_pairs['underlying_ticker1']]
    vcs_pairs['proxy_ticker1'] = [x['ticker'] for x in proxy_output_list1]
    vcs_pairs['add_2_proxy1'] = [x['add_2_proxy'] for x in proxy_output_list1]

    proxy_output_list2 = [up.get_underlying_proxy_ticker(ticker=x, settle_date=report_date,futures_data_dictionary=futures_data_dictionary) for x in vcs_pairs['underlying_ticker2']]
    vcs_pairs['proxy_ticker2'] = [x['ticker'] for x in proxy_output_list2]
    vcs_pairs['add_2_proxy2'] = [x['add_2_proxy'] for x in proxy_output_list2]

    vcs_pairs['expiration_date1'] = [int(exp.get_expiration_from_db(instrument='options', ticker=x, con=con).strftime('%Y%m%d')) for x in vcs_pairs['ticker1']]
    vcs_pairs['expiration_date2'] = [int(exp.get_expiration_from_db(instrument='options', ticker=x, con=con).strftime('%Y%m%d')) for x in vcs_pairs['ticker2']]

    vcs_pairs['interest_date1'] = [grfs.get_simple_rate(as_of_date=report_date, date_to=x)['rate_output'] for x in vcs_pairs['expiration_date1']]
    vcs_pairs['interest_date2'] = [grfs.get_simple_rate(as_of_date=report_date, date_to=x)['rate_output'] for x in vcs_pairs['expiration_date2']]
    vcs_pairs['exercise_type'] = [cmi.get_option_exercise_type(ticker_head=x) for x in vcs_pairs['tickerHead']]

    admin_dir = dna.get_directory_name(ext='admin')
    risk_file_out = su.read_text_file(file_name=admin_dir + '/RiskParameter.txt')
    vcs_risk_parameter = 5*2*float(risk_file_out[0])

    vcs_pairs['long_quantity'] = vcs_risk_parameter/abs(vcs_pairs['downside'])
    vcs_pairs['short_quantity'] = vcs_risk_parameter/vcs_pairs['upside']
    vcs_pairs['long_quantity'] = vcs_pairs['long_quantity'].round()
    vcs_pairs['short_quantity'] = vcs_pairs['short_quantity'].round()

    vcs_pairs['alias'] = [generate_vcs_alias(vcs_row=vcs_pairs.iloc[x]) for x in range(len(vcs_pairs.index))]

    vcs_pairs['call_mid_price1'] = np.nan
    vcs_pairs['put_mid_price1'] = np.nan
    vcs_pairs['call_mid_price2'] = np.nan
    vcs_pairs['put_mid_price2'] = np.nan
    vcs_pairs['call_iv1'] = np.nan
    vcs_pairs['put_iv1'] = np.nan
    vcs_pairs['call_iv2'] = np.nan
    vcs_pairs['put_iv2'] = np.nan
    vcs_pairs['underlying_mid_price1'] = np.nan
    vcs_pairs['underlying_mid_price2'] = np.nan
    vcs_pairs['proxy_mid_price1'] = np.nan
    vcs_pairs['proxy_mid_price2'] = np.nan
    vcs_pairs['current_strike1'] = np.nan
    vcs_pairs['current_strike2'] = np.nan

    ta_folder = dn.get_dated_directory_extension(folder_date=todays_date, ext='ta')

    app.vcs_pairs = vcs_pairs
    app.con = con
    app.futures_data_dictionary = futures_data_dictionary
    app.report_date = report_date
    app.todays_date = todays_date
    app.log = lg.get_logger(file_identifier='vcs', log_level='INFO')
    app.trade_file = ta_folder + '/trade_dir.csv'
    app.vcs_risk_parameter = vcs_risk_parameter
    app.connect(client_id=3)
    app.run()

def generate_vcs_alias(**kwargs):
    vcs_row = kwargs['vcs_row']

    ticker1 = vcs_row['ticker1']
    ticker2 = vcs_row['ticker2']
    ticker_head = vcs_row['tickerHead']
    Q = vcs_row['Q']

    contract_contract_specs1 = cmi.get_contract_specs(ticker1)
    contract_contract_specs2 = cmi.get_contract_specs(ticker2)
    ticker_year_short1 = contract_contract_specs1['ticker_year'] % 100
    ticker_year_short2 = contract_contract_specs2['ticker_year'] % 100

    alias = ''

    if Q < 50:
        alias = ticker_head + contract_contract_specs1['ticker_month_str'] + str(ticker_year_short1) + \
                contract_contract_specs2['ticker_month_str'] + str(ticker_year_short2) + 'VCS'
    elif Q > 50:
        alias = ticker_head + contract_contract_specs2['ticker_month_str'] + str(ticker_year_short2) + \
                contract_contract_specs1['ticker_month_str'] + str(ticker_year_short1) + 'VCS'

    return alias



if __name__ == "__main__":
    main()