
import shared.email as se
import shared.directory_names as dn
import contract_utilities.expiration as exp
import shared.calendar_utilities as cu
import ta.strategy as ts
import ta.expiration_followup as ef
import signals.dual_momentum as dm


def send_hrsn_report(**kwargs):

    daily_dir = dn.get_directory_name(ext='daily')

    if 'report_date' in kwargs.keys():
        report_date = kwargs['report_date']
    else:
        report_date = exp.doubledate_shift_bus_days()

    ibo_dir = ts.create_strategy_output_dir(strategy_class='os', report_date=report_date)
    cov_data_integrity = ''

    try:
        with open(ibo_dir + '/' + 'covDataIntegrity.txt','r') as text_file:
            cov_data_integrity = text_file.read()
    except Exception:
        pass

    try:
        expiration_report = ef.get_expiration_report(report_date=report_date, con=kwargs['con'])
        expiration_report = expiration_report[expiration_report['tr_days_2roll'] < 5]

        if expiration_report.empty:
            expiration_text = 'No near expirations.'
        else:
            expiration_text = 'Check for approaching expirations!'
    except Exception:
        expiration_text = 'Check expiration report for errors!'

    se.send_email_with_attachment(subject='hrsn_' + str(report_date),
                                  email_text='cov_data_integrity: ' + cov_data_integrity + "\r\n" + expiration_text,
                                  attachment_list = [daily_dir + '/' + 'pnl_final_' + str(report_date) + '.xlsx', daily_dir +
                                                     '/' + 'followup_' + str(report_date) + '.xlsx'])


def send_hrsn_early_report(**kwargs):

    if 'report_date' in kwargs.keys():
        report_date = kwargs['report_date']
    else:
        report_date = exp.doubledate_shift_bus_days()

    ta_output_dir = dn.get_dated_directory_extension(folder_date=report_date,ext='ta')

    se.send_email_with_attachment(subject='hrsn_early_' + str(report_date),
                                  attachment_list = [ta_output_dir + '/' + 'pnl_early.xlsx'])




def send_dual_momentum_report(**kwargs):

    if 'report_date' in kwargs.keys():
        report_date = kwargs['report_date']
    else:
        report_date = exp.doubledate_shift_bus_days()

    output = dm.get_signals_4date(report_date=report_date)

    if not output['success']:
        email_text = 'Report Failed!'
    else:
        performance_dictionary = output['performance_dictionary']

        ivv = performance_dictionary['IVV']
        veu = performance_dictionary['VEU']
        bil = performance_dictionary['BIL']

        if ivv>=max(veu, bil):
            recommended_ticker = 'IVV'
        elif (veu>ivv) and (ivv>bil):
            recommended_ticker = 'VEU'
        else:
            recommended_ticker = 'AGG'

        email_text = 'US Stocks: ' + '% 3.2f' % ivv + "\r\n" \
                     + 'World Stocks: ' + '% 3.2f' % veu + "\r\n" \
                     + 'T-Bills: ' + ' % 3.2f' % bil + "\r\n" \
                     + "\r\n" \
                     + 'Recommended Ticker: ' + recommended_ticker

    se.send_email_with_attachment(subject='Dual Momentum Results On ' +
                                          cu.convert_datestring_format({'date_string': str(report_date),
                                                                        'format_from':'yyyymmdd',
                                                                        'format_to':'yyyy-mm-dd'}),email_text=email_text)





