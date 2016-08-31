
import shared.email as se
import contract_utilities.expiration as exp
import shared.directory_names as dn
import ta.strategy as ts


def send_hrsn_report(**kwargs):

    daily_dir = dn.get_directory_name(ext='daily')

    if 'report_date' in kwargs.keys():
        report_date = kwargs['report_date']
    else:
        report_date = exp.doubledate_shift_bus_days()

    ibo_dir = ts.create_strategy_output_dir(strategy_class='ibo', report_date=report_date)
    cov_data_integrity = ''

    try:
        with open(ibo_dir + '/' + 'covDataIntegrity.txt','r') as text_file:
            cov_data_integrity = text_file.read()
    except Exception:
        pass

    se.send_email_with_attachment(subject='hrsn_' + str(report_date),
                                  email_text='cov_data_integrity: ' + cov_data_integrity,
                                  attachment_list = [daily_dir + '/' + 'pnl_' + str(report_date) + '.xlsx', daily_dir +
                                                     '/' + 'followup_' + str(report_date) + '.xlsx'])

