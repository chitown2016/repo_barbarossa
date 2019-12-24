__author__ = 'kocat_000'

import os
import contract_utilities.expiration as exp
import shared.calendar_utilities as cu
import shared.directory_names_aux as dna


def get_dated_directory_extension(**kwargs):

    if 'folder_date' in kwargs.keys():
        folder_date = kwargs['folder_date']
    else:
        folder_date = exp.doubledate_shift_bus_days()

    if 'ext' not in kwargs.keys():
        print('Need to provide a valid ext !')
        return

    directory_name = dna.get_directory_name(**kwargs)

    dated_directory_name = directory_name + '/' + cu.get_directory_extension(folder_date)

    if not os.path.exists(dated_directory_name):
        os.makedirs(dated_directory_name)

    return dated_directory_name












