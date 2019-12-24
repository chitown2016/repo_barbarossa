
import contract_utilities.expiration as exp
import ta.strategy as ts
import shutil as sutil
import shared.directory_names_aux as dna
import shared.directory_names as dn
import formats.utils as futil

daily_dir = dna.get_directory_name(ext='daily')


def prepare_strategy_daily(**kwargs):

    strategy_class = kwargs['strategy_class']

    if 'report_date' in kwargs.keys():
        report_date = kwargs['report_date']
    else:
        report_date = exp.doubledate_shift_bus_days()

    output_dir = ts.create_strategy_output_dir(strategy_class=strategy_class, report_date=report_date)

    sutil.copyfile(output_dir + '/' + futil.xls_file_names[strategy_class] + '.xlsx',
                   daily_dir + '/' + futil.xls_file_names[strategy_class] + '_' + str(report_date) + '.xlsx')


def move_from_dated_folder_2daily_folder(**kwargs):

    ext = kwargs['ext']
    file_name_raw = kwargs['file_name']

    file_name_split = file_name_raw.split('.')

    if len(file_name_split) == 1:
        file_name = file_name_raw
        file_ext = '.xlsx'
    else:
        file_name = file_name_split[0]
        file_ext = file_name_split[1]

    if 'folder_date' in kwargs.keys():
        folder_date = kwargs['folder_date']
    else:
        folder_date = exp.doubledate_shift_bus_days()

    dated_folder = dn.get_dated_directory_extension(folder_date=folder_date, ext=ext)

    sutil.copyfile(dated_folder + '/' + file_name + file_ext,
                   daily_dir + '/' + file_name + '_' + str(folder_date) + file_ext)



