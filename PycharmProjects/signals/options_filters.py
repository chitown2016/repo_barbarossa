
vcs_filter_dict = {'Ag_long': 21,
                   'Ag_long1': 34,
                   'Livestock_long': 16,
                   'Livestock_long1': 37,
                   'Metal_long': 31,
                   'Metal_long1': 28,
                   'FX_long': 12,
                   'FX_long1': 24,
                   'CL_long': 4,
                   'CL_long1': 31,
                   'Ag_short': 65,
                   'Ag_short1': 66,
                   'Livestock_short': 65,
                   'Livestock_short1': 70,
                   'Metal_short': 61,
                   'Metal_short1': 66,
                   'CL_short': 33,
                   'CL_short1': 67,
                   'NG_short': 69,
                   'NG_short1': 66,
                   'Treasury_short': 93,
                   'Treasury_short1': 71,
                   'Index_short': 74,
                   'Index_short1': 60}

import contract_utilities.contract_meta_info as cmi
import pandas as pd


def get_vcs_filter_values(**kwargs):

    product_group = kwargs['product_group']
    filter_type = kwargs['filter_type']
    direction = kwargs['direction']
    indicator = kwargs['indicator']

    if indicator == 'Q1':
        indicator_dict_input = '1'
    elif indicator == 'Q':
        indicator_dict_input = ''

    key_requested = product_group + '_' + direction + indicator_dict_input

    if direction == 'long':
        filter_value = -1
    elif direction == 'short':
        filter_value = 101

    if filter_type == 'tickerHead':
        if key_requested in vcs_filter_dict.keys():
            filter_value = vcs_filter_dict[key_requested]
        else:
            ticker_class = cmi.ticker_class[product_group]
            key_requested = ticker_class + '_' + direction + indicator_dict_input
            if key_requested in vcs_filter_dict.keys():
                filter_value = vcs_filter_dict[key_requested]
    elif filter_type == 'tickerClass':
        if key_requested in vcs_filter_dict.keys():
            filter_value = vcs_filter_dict[key_requested]

    return filter_value


def get_vcs_filters(**kwargs):

    data_frame_input = kwargs['data_frame_input']
    filter_list = kwargs['filter_list']

    selection_indx = [False]*len(data_frame_input.index)

    if 'long1' in filter_list:
        selection_indx = selection_indx|((data_frame_input['tickerClass'] == 'Ag') & (data_frame_input['Q'] <= 13))
        selection_indx = selection_indx|((data_frame_input['tickerHead'] == 'CL') & (data_frame_input['Q'] <= 2))
        selection_indx = selection_indx|((data_frame_input['tickerClass'] == 'FX') & (data_frame_input['Q'] <= 4))
        selection_indx = selection_indx|((data_frame_input['tickerClass'] == 'Livestock') & (data_frame_input['Q'] <= 10))
        selection_indx = selection_indx|((data_frame_input['tickerClass'] == 'Metal') & (data_frame_input['Q'] <= 15))

    if 'short1' in filter_list:
        selection_indx = selection_indx|((data_frame_input['tickerClass'] == 'Ag') & (data_frame_input['Q'] >= 90))
        selection_indx = selection_indx|((data_frame_input['tickerClass'] == 'Treasury') & (data_frame_input['Q'] >= 93))
        selection_indx = selection_indx|((data_frame_input['tickerHead'] == 'CL') & (data_frame_input['Q'] >= 60))
        selection_indx = selection_indx|((data_frame_input['tickerClass'] == 'Index') & (data_frame_input['Q'] >= 84))
        selection_indx = selection_indx|((data_frame_input['tickerClass'] == 'Livestock') & (data_frame_input['Q'] >= 70))
        selection_indx = selection_indx|((data_frame_input['tickerClass'] == 'Metal') & (data_frame_input['Q'] >= 64))

    if 'long2' in filter_list:

        long_tuples = [('Ag','tickerClass'),('Livestock','tickerClass'),('Metal','tickerClass'),('FX','tickerClass'),('CL','tickerHead')]

        for i in range(len(long_tuples)):

                selection_indx = selection_indx|((data_frame_input[long_tuples[i][1]] == long_tuples[i][0]) &
                                         (data_frame_input['Q1'] <= get_vcs_filter_values(product_group=long_tuples[i][0],
                                                                                          filter_type=long_tuples[i][1],
                                                                                          direction='long',
                                                                                          indicator='Q1')) &
                                         (data_frame_input['Q'] <= get_vcs_filter_values(product_group=long_tuples[i][0],
                                                                                         filter_type=long_tuples[i][1],
                                                                                         direction='long',
                                                                                         indicator='Q')))

    if 'short2' in filter_list:

        short_tuples = [('Ag','tickerClass'),('Livestock','tickerClass'),('Metal','tickerClass'),('CL','tickerHead'),('NG','tickerHead'),
                        ('Treasury','tickerClass'),('Index','tickerClass')]

        for i in range(len(short_tuples)):

                selection_indx = selection_indx|((data_frame_input[short_tuples[i][1]] == short_tuples[i][0]) &
                                         (data_frame_input['Q1'] >= get_vcs_filter_values(product_group=short_tuples[i][0],
                                                                                          filter_type=short_tuples[i][1],
                                                                                          direction='short',
                                                                                          indicator='Q1')) &
                                         (data_frame_input['Q'] >= get_vcs_filter_values(product_group=short_tuples[i][0],
                                                                                         filter_type=short_tuples[i][1],
                                                                                         direction='short',
                                                                                         indicator='Q')))

    selection_indx = selection_indx&pd.notnull(data_frame_input['downside'])&\
                     pd.notnull(data_frame_input['upside'])&\
                     (data_frame_input['atmVolRatio']<=1.3)&(data_frame_input['atmVolRatio']>=0.7)&\
                     (((data_frame_input['Q']<=30)&(data_frame_input['fwdVolQ']>=30))|((data_frame_input['Q']>=60)&(data_frame_input['fwdVolQ']<=70)))



    return {'selected_frame': data_frame_input[selection_indx],'selection_indx': selection_indx}

def get_scv_filters(**kwargs):

    data_frame_input = kwargs['data_frame_input']
    filter_list = kwargs['filter_list']

    selection_indx = [False]*len(data_frame_input.index)

    if 'long1' in filter_list:

        selection_indx = selection_indx|((data_frame_input['premium'] <= -28) & (data_frame_input['Q'] <= 29))

        selection_indx = selection_indx|((data_frame_input['premium'] > -28) &
                                         (data_frame_input['premium'] <= -7) &
                                         (data_frame_input['Q'] <= 14))

    if 'short1' in filter_list:

        selection_indx = selection_indx|((data_frame_input['premium'] >= 21) & (data_frame_input['Q'] >= 80))

        selection_indx = selection_indx|((data_frame_input['premium'] < 21) &
                                         (data_frame_input['premium'] >= 12) &
                                         (data_frame_input['Q'] >= 91))

    return {'selected_frame': data_frame_input[selection_indx],'selection_indx': selection_indx }

