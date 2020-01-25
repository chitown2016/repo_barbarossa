
import contract_utilities.expiration as exp
import ta.strategy as ts
import pandas as pd
import opportunity_constructs.vcs as vcs
import opportunity_constructs.scv as scv
import signals.options_filters as of
import formats.utils as futil


def generate_vcs_formatted_output(**kwargs):

    if 'report_date' in kwargs.keys():
        report_date = kwargs['report_date']
    else:
        report_date = exp.doubledate_shift_bus_days()

    output_dir = ts.create_strategy_output_dir(strategy_class='vcs', report_date=report_date)

    vcs_output = vcs.generate_vcs_sheet_4date(date_to=report_date)
    vcs_pairs = vcs_output['vcs_pairs']

    filter_out = of.get_vcs_filters(data_frame_input=vcs_pairs, filter_list=['long2', 'short2'])
    good_vcs_pairs = filter_out['selected_frame']

    vcs_pairs_w_selected_columns = vcs_pairs[['ticker1', 'ticker2', 'tickerHead', 'tickerClass', 'trDte1', 'trDte2', 'Q',
                                              'Q1', 'fwdVolQ', 'downside', 'upside', 'atmVolRatio', 'fwdVol','realVolRatio',
                                              'atmRealVolRatio','theta']]

    good_vcs_pairs_w_selected_columns = good_vcs_pairs[['ticker1', 'ticker2', 'tickerHead', 'tickerClass', 'trDte1', 'trDte2', 'Q',
                                              'Q1', 'fwdVolQ', 'downside', 'upside', 'atmVolRatio', 'fwdVol','realVolRatio',
                                              'atmRealVolRatio','theta']]

    writer = pd.ExcelWriter(output_dir + '/' + futil.xls_file_names['vcs'] + '.xlsx', engine='xlsxwriter')

    vcs_pairs_w_selected_columns.to_excel(writer, sheet_name='all')
    good_vcs_pairs_w_selected_columns.to_excel(writer, sheet_name='good')

    worksheet_good = writer.sheets['good']
    worksheet_all = writer.sheets['all']

    worksheet_good.freeze_panes(1, 0)
    worksheet_all.freeze_panes(1, 0)

    worksheet_good.autofilter(0, 0, len(good_vcs_pairs_w_selected_columns.index),
                              len(good_vcs_pairs_w_selected_columns.columns))

    worksheet_all.autofilter(0, 0, len(vcs_pairs_w_selected_columns.index),
                                   len(vcs_pairs_w_selected_columns.columns))

    writer.save()


def generate_scv_formatted_output(**kwargs):

    if 'report_date' in kwargs.keys():
        report_date = kwargs['report_date']
    else:
        report_date = exp.doubledate_shift_bus_days()

    output_dir = ts.create_strategy_output_dir(strategy_class='scv', report_date=report_date)

    scv_output = scv.generate_scv_sheet_4date(date_to=report_date)
    scv_frame = scv_output['scv_frame']

    filter_out = of.get_scv_filters(data_frame_input=scv_frame, filter_list=['long1', 'short1'])
    good_scv_frame = filter_out['selected_frame']

    writer = pd.ExcelWriter(output_dir + '/' + futil.xls_file_names['scv'] + '.xlsx', engine='xlsxwriter')

    scv_frame.to_excel(writer, sheet_name='all')
    good_scv_frame.to_excel(writer, sheet_name='good')

    worksheet_good = writer.sheets['good']
    worksheet_all = writer.sheets['all']

    worksheet_good.freeze_panes(1, 0)
    worksheet_all.freeze_panes(1, 0)

    worksheet_good.autofilter(0, 0, len(good_scv_frame.index),
                              len(good_scv_frame.columns))

    worksheet_all.autofilter(0, 0, len(scv_frame.index),
                                   len(scv_frame.columns))

    writer.save()

