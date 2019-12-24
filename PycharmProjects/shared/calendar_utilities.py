__author__ = 'kocat_000'

import datetime as dt

three_letter_month_dictionary = {"JAN": 1, "MAR": 3,"MAY": 5, 'JUL': 7 , 'AUG': 8,'SEP': 9 ,'OCT':10,'NOV': 11, 'DEC': 12}


def convert_doubledate_2datetime(double_date):
    return dt.datetime.strptime(str(double_date), '%Y%m%d')


def doubledate_shift(double_date, shift_in_days):
    shifted_datetime = convert_doubledate_2datetime(double_date)-dt.timedelta(shift_in_days)
    return int(shifted_datetime.strftime('%Y%m%d'))

def get_datetime_shift(**kwargs):

    if 'reference_date' in kwargs.keys():
        reference_date = kwargs['reference_date']
    else:
        reference_date = dt.datetime.now()

    return reference_date-dt.timedelta(kwargs['shift_in_days'])


def convert_datestring_format(cu_input):
    date_string = cu_input['date_string']
    format_from = cu_input['format_from']
    format_to = cu_input['format_to']

    if format_from=='yyyymmdd':
        datetime_out = dt.datetime.strptime(date_string,'%Y%m%d')

    if format_to=='yyyy-mm-dd':
        datestring_out = datetime_out.strftime('%Y-%m-%d')

    return datestring_out


def get_doubledate(**kwargs):

    datetime_out = dt.datetime.now()
    return int(datetime_out.strftime('%Y%m%d'))

def get_directory_extension(date_to):

    date_to_datetime = convert_doubledate_2datetime(date_to)
    return str(date_to_datetime.year) + '/' + str(100*date_to_datetime.year+date_to_datetime.month) + '/' + str(date_to)






