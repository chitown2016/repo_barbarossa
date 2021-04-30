__author__ = 'kocat_000'

import contract_utilities.expiration as exp
import opportunity_constructs.futures_butterfly as fb
import opportunity_constructs.intraday_future_spreads as ifs
import opportunity_constructs.overnight_calendar_spreads as ocs
import opportunity_constructs.outright_contract_summary as out_cs
import opportunity_constructs.spread_carry as sc
import opportunity_constructs.curve_pca as cpc
import signals.futures_filters as ff
import shared.directory_names as dn
import pandas as pd
import ta.strategy as ts
import formats.utils as futil


def generate_futures_butterfly_formatted_output(**kwargs):

    if 'report_date' in kwargs.keys():
        report_date = kwargs['report_date']
    else:
        report_date = exp.doubledate_shift_bus_days()

    output_dir = ts.create_strategy_output_dir(strategy_class='futures_butterfly', report_date=report_date)

    butterfly_output = fb.generate_futures_butterfly_sheet_4date(date_to=report_date)
    butterflies = butterfly_output['butterflies']

    filter_out = ff.get_futures_butterfly_filters(data_frame_input=butterflies, filter_list=['long1', 'short1'])
    good_butterflies = filter_out['selected_frame']

    good_butterflies = good_butterflies[(good_butterflies['second_spread_weight'] <= 2) & (good_butterflies['second_spread_weight'] >= 0.66)]

    butterflies_w_selected_columns = butterflies[['ticker1', 'ticker2', 'ticker3',
                                                  'tickerHead', 'trDte1', 'trDte2', 'trDte3',
                                                  'z1', 'QF', 'rr', 'bf_price',
                                                  'bfw_price','second_spread_weight', 'upside', 'downside',
                                                   'recent_5day_pnl']]

    good_butterflies_w_selected_columns = good_butterflies[['ticker1', 'ticker2', 'ticker3',
                                                  'tickerHead', 'trDte1', 'trDte2', 'trDte3',
                                                  'z1', 'QF', 'rr', 'bf_price',
                                                  'bfw_price','second_spread_weight', 'upside', 'downside',
                                                   'recent_5day_pnl']]

    writer = pd.ExcelWriter(output_dir + '/' + futil.xls_file_names['futures_butterfly'] + '.xlsx', engine='xlsxwriter')

    butterflies_w_selected_columns.to_excel(writer, sheet_name='all')
    good_butterflies_w_selected_columns.to_excel(writer, sheet_name='good')

    worksheet_good = writer.sheets['good']
    worksheet_all = writer.sheets['all']

    worksheet_good.freeze_panes(1, 0)
    worksheet_all.freeze_panes(1, 0)

    worksheet_good.autofilter(0, 0, len(good_butterflies_w_selected_columns.index),
                              len(good_butterflies_w_selected_columns.columns))

    worksheet_all.autofilter(0, 0, len(butterflies_w_selected_columns.index),
                                   len(butterflies_w_selected_columns.columns))

    writer.save()


def generate_spread_carry_formatted_output(**kwargs):

    if 'report_date' in kwargs.keys():
        report_date = kwargs['report_date']
    else:
        report_date = exp.doubledate_shift_bus_days()

    output_dir = ts.create_strategy_output_dir(strategy_class='spread_carry', report_date=report_date)

    spread_carry_output = sc.generate_spread_carry_sheet_4date(report_date=report_date)
    spread_report = spread_carry_output['spread_report']

    writer = pd.ExcelWriter(output_dir + '/' + futil.get_xls_file_name('spread_carry') + '.xlsx', engine='xlsxwriter')
    spread_report.to_excel(writer, sheet_name='summary')
    writer.save()


def generate_curve_pca_formatted_output(**kwargs):

    if 'report_date' in kwargs.keys():
        report_date = kwargs['report_date']
    else:
        report_date = exp.doubledate_shift_bus_days()

    output_dir = ts.create_strategy_output_dir(strategy_class='curve_pca', report_date=report_date)

    ticker_head_list = ['CL', 'B']
    selected_column_list = ['ticker1', 'ticker2', 'monthSpread', 'tr_dte_front', 'residuals', 'price', 'yield', 'z', 'z2', 'factor_load1', 'factor_load2']
    writer = pd.ExcelWriter(output_dir + '/' + futil.xls_file_names['curve_pca'] + '.xlsx', engine='xlsxwriter')

    for ticker_head in ticker_head_list:

        curve_pca_output = cpc.get_curve_pca_report(ticker_head=ticker_head, date_to=report_date)

        if curve_pca_output['success']:

            all_spreads = curve_pca_output['pca_results']
            filter_out = ff.get_curve_pca_filters(data_frame_input=all_spreads, filter_list=['long1', 'short1'])
            good_spreads = filter_out['selected_frame']

            all_spreads = all_spreads[selected_column_list]
            good_spreads = good_spreads[selected_column_list]

            all_spreads.to_excel(writer, sheet_name=ticker_head + '-all')
            good_spreads.to_excel(writer, sheet_name=ticker_head + '-good')

            worksheet_good = writer.sheets[ticker_head + '-good']
            worksheet_all = writer.sheets[ticker_head + '-all']

            worksheet_good.freeze_panes(1, 0)
            worksheet_all.freeze_panes(1, 0)

            worksheet_good.autofilter(0, 0, len(good_spreads.index), len(selected_column_list))

            worksheet_all.autofilter(0, 0, len(all_spreads.index), len(selected_column_list))
    writer.save()


def generate_ifs_formatted_output(**kwargs):

    if 'report_date' in kwargs.keys():
        report_date = kwargs['report_date']
    else:
        report_date = exp.doubledate_shift_bus_days()

    output_dir = ts.create_strategy_output_dir(strategy_class='ifs', report_date=report_date)

    ifs_output = ifs.generate_ifs_sheet_4date(date_to=report_date)
    intraday_spreads = ifs_output['intraday_spreads']

    intraday_spreads.rename(columns={'ticker_head1': 'tickerHead1', 'ticker_head2': 'tickerHead2', 'ticker_head3': 'tickerHead3'},inplace=True)

    writer = pd.ExcelWriter(output_dir + '/' + futil.xls_file_names['ifs'] + '.xlsx', engine='xlsxwriter')

    intraday_spreads.to_excel(writer, sheet_name='all')


def generate_ocs_formatted_output(**kwargs):

    if 'report_date' in kwargs.keys():
        report_date = kwargs['report_date']
    else:
        report_date = exp.doubledate_shift_bus_days()

    output_dir = ts.create_strategy_output_dir(strategy_class='ocs', report_date=report_date)

    ocs_output = ocs.generate_overnight_spreads_sheet_4date(date_to=report_date)
    overnight_calendars = ocs_output['overnight_calendars']
    overnight_calendars = overnight_calendars[overnight_calendars['butterflyQ'].notnull()]

    writer = pd.ExcelWriter(output_dir + '/' + futil.xls_file_names['ocs'] + '.xlsx', engine='xlsxwriter')

    overnight_calendars.to_excel(writer, sheet_name='all')

    writer.save()


def generate_outright_summary_formatted_output(**kwargs):

    if 'report_date' in kwargs.keys():
        report_date = kwargs['report_date']
    else:
        report_date = exp.doubledate_shift_bus_days()

    output_dir = ts.create_strategy_output_dir(strategy_class='os', report_date=report_date)

    out_dictionary = out_cs.generate_outright_summary_sheet_4date(date_to=report_date)
    cov_matrix = out_dictionary['cov_output']['cov_matrix']

    cov_matrix.reset_index(drop=False, inplace=True)

    writer = pd.ExcelWriter(output_dir + '/' + 'cov_matrix.xlsx', engine='xlsxwriter')
    cov_matrix.to_excel(writer, sheet_name='cov_matrix')
    writer.save()

    cov_data_integrity = round(out_dictionary['cov_output']['cov_data_integrity'], 2)

    with open(output_dir + '/' + 'covDataIntegrity.txt','w') as text_file:
        text_file.write(str(cov_data_integrity))

    sheet_4date = out_dictionary['sheet_4date']

    writer = pd.ExcelWriter(output_dir + '/summary.xlsx', engine='xlsxwriter')

    sheet_4date.to_excel(writer, sheet_name='all')

    worksheet_all = writer.sheets['all']
    worksheet_all.freeze_panes(1, 0)
    worksheet_all.autofilter(0, 0, len(sheet_4date.index),len(sheet_4date.columns))
    writer.save()



