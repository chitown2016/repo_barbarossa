
import pickle as pickle
import pandas as pd
import shared.directory_names as dn


def read_cme_settle_txt_files(**kwargs):

    file_name = kwargs['file_name']
    report_date = kwargs['report_date']

    options_data_dir = dn.get_dated_directory_extension(folder_date=report_date, ext='raw_options_data')

    with open(options_data_dir + '/' + file_name + '.pkl', 'rb') as handle:
        raw_data = pickle.load(handle)

    title_list = []
    data_start_list = []
    data_end_list = []

    decoded_data = [x.decode('UTF-8') for x in raw_data]

    volume_start_indx = decoded_data[2].find('EST.VOL')-1
    interest_indx = decoded_data[2].find('INT')-5
    month_strike_indx = decoded_data[2].find('STRIKE')
    settle_indx = decoded_data[2].find('SETT')-4

    open_indx = decoded_data[2].find('OPEN')-4
    high_indx = decoded_data[2].find('HIGH')-4
    low_indx = decoded_data[2].find('LOW')-5

    for indx in range(len(decoded_data)):

        if any(x in decoded_data[indx] for x in ['OPTIONS', 'OPTION', 'Options','FUTURE','Future','CSO','AIRASIA']):
            title_list.append(decoded_data[indx])

            if len(data_start_list) == 0:
                data_start_list.append(indx+1)
            else:
                data_start_list.append(indx+1)
                data_end_list.append(indx)

        if 'END OF REPORT' in decoded_data[indx]:
            data_end_list.append(indx)

    data_list = [decoded_data[data_start_list[x]:data_end_list[x]] for x in range(len(data_start_list))]

    volume_list = []
    volume_filtered_list = []

    interest_list = []
    interest_filtered_list = []

    settle_list = []
    open_list = []
    high_list = []
    low_list = []

    month_strike_list = []
    filter_1_list = []
    filter_2_list = []
    total_volume_list = []

    for i in range(len(data_list)):
        volume_column = [x[volume_start_indx:(volume_start_indx+10)] for x in data_list[i]]
        interest_column = [x[interest_indx:(interest_indx+10)] for x in data_list[i]]

        settle_column = [x[settle_indx:(settle_indx+9)] for x in data_list[i]]

        open_column = [x[open_indx:(open_indx+9)] for x in data_list[i]]
        high_column = [x[high_indx:(high_indx+9)] for x in data_list[i]]
        low_column = [x[low_indx:(low_indx+9)] for x in data_list[i]]

        month_strike_column = [x[month_strike_indx:(month_strike_indx+5)] for x in data_list[i]]
        month_strike_column_filtered = [x for x in month_strike_column if x != 'TOTAL']

        settle_column_filtered = [settle_column[x].strip() for x in range(len(settle_column)) if month_strike_column[x] != 'TOTAL']
        volume_column_filtered = [volume_column[x].strip() for x in range(len(volume_column)) if month_strike_column[x] != 'TOTAL']
        interest_column_filtered = [interest_column[x].strip() for x in range(len(interest_column)) if month_strike_column[x] != 'TOTAL']

        open_column_filtered = [open_column[x].strip() for x in range(len(open_column)) if month_strike_column[x] != 'TOTAL']
        high_column_filtered = [high_column[x].strip() for x in range(len(high_column)) if month_strike_column[x] != 'TOTAL']
        low_column_filtered = [low_column[x].strip() for x in range(len(low_column)) if month_strike_column[x] != 'TOTAL']

        filter_1 = ['TOTAL' not in x for x in month_strike_column]
        filter_1_list.append(filter_1)
        filter_2 = [bool((volume_column[x]).strip()) if filter_1[x] else False for x in range(len(volume_column))]
        filter_2_list.append(filter_2)
        volume_list.append(volume_column)
        volume_filtered_list.append(volume_column_filtered)

        interest_list.append(interest_column)
        interest_filtered_list.append(interest_column_filtered)

        settle_list.append(settle_column_filtered)
        open_list.append(open_column_filtered)
        high_list.append(high_column_filtered)
        low_list.append(low_column_filtered)
        month_strike_list.append(month_strike_column_filtered)

        total_volume_list.append((sum([int(volume_column[x]) if filter_2[x] else 0 for x in range(len(volume_column))])))

    title_frame = pd.DataFrame([process_title(x) for x in title_list])

    return {'decoded_data': decoded_data,
            'data_start_list': data_start_list,
            'data_end_list': data_end_list,
            'title_list': title_list,
            'data_list': data_list,
            'volume_list': volume_list,
            'volume_filtered_list': volume_filtered_list,
            'interest_list': interest_list,
            'interest_filtered_list': interest_filtered_list,
            'settle_list': settle_list,
            'open_list': open_list,
            'high_list': high_list,
            'low_list': low_list,
            'month_strike_list': month_strike_list,
            'filter_1_list': filter_1_list,
            'filter_2_list': filter_2_list,
            'total_volume_list': total_volume_list,
            'title_frame': title_frame}


def process_title(title_input):

    title_list = title_input.split(' ')

    #print(title_input)

    if any(x in title_input for x in ['OPTIONS', 'OPTION', 'Options']):
        if 'Calendar Spread Options' in title_input:
            asset_type = 'calendar_spread_option'
        else:
            asset_type = 'options'
    elif any(x in title_input for x in ['FUTURES']):
        asset_type = 'futures'
    else:
        asset_type = ''

    if 'WHEAT-CORN' in title_input:
        ticker_head = 'CW'
        title_indx = title_list.index('WHEAT-CORN')
    elif 'CORN' in title_input:
        ticker_head = 'C'
        title_indx = title_list.index('CORN')
    elif 'Corn' in title_input:
        ticker_head = 'C'
        title_indx = title_list.index('Corn')
    elif 'SOYBEAN CRUSH' in title_input:
        ticker_head = 'crush'
        title_indx = title_list.index('BCO')+2
    elif 'SOYBEANS' in title_input:
        ticker_head = 'S'
        title_indx = title_list.index('SOYBEANS')
    elif 'SOY BEAN' in title_input:
        ticker_head = 'S'
        if asset_type == 'options':
            title_indx = title_list.index('MZ2')+2
    elif 'Soybean Meal' in title_input:
        ticker_head = 'SM'
        if asset_type == 'options':
            title_indx = title_list.index('MY')+2
    elif 'Soybean Oil' in title_input:
        ticker_head = 'BO'
        if asset_type == 'options':
            title_indx = title_list.index('OYC')+2
    elif 'Wheat' in title_input:
        ticker_head = 'W'
        title_indx = title_list.index('Wheat')
    elif 'WHEAT' in title_input:
        ticker_head = 'W'
        title_indx = title_list.index('WHEAT')
    elif 'LEAN HOG' in title_input:
        ticker_head = 'LN'
        if asset_type == 'options':
            title_indx = title_list.index('OH')+2
    elif 'LIVE CATTLE' in title_input:
        ticker_head = 'LC'
        if asset_type == 'options':
            title_indx = title_list.index('OK')+2
    elif 'FEEDER CATTLE' in title_input:
        ticker_head = 'FC'
        if asset_type == 'options':
            title_indx = title_list.index('ZF')+2
    elif 'Oats' in title_input:
        ticker_head = 'O'
        if asset_type == 'options':
            title_indx = title_list.index('OO')+2
    elif 'CME NON-FAT DRY MILK' in title_input:
        ticker_head = 'NF'
        if asset_type == 'options':
            title_indx = title_list.index('NZ')+2
    elif 'CME CLASS IV MILK' in title_input:
        ticker_head = 'DK'
        if asset_type == 'options':
            title_indx = title_list.index('DZ')+2
    elif 'MILK' in title_input:
        ticker_head = 'DA'
        if asset_type == 'options':
            title_indx = title_list.index('OA')+2
    elif 'Rough Rice' in title_input:
        ticker_head = 'RR'
        if asset_type == 'options':
            title_indx = title_list.index('RRC')+2
    elif 'LUMBER' in title_input:
        ticker_head = 'LB'
        if asset_type == 'options':
            title_indx = title_list.index('LUMBER')
    elif 'CME CASH SETTLED BUTTER' in title_input:
        ticker_head = 'CB'
        if asset_type == 'options':
            title_indx = title_list.index('CB0')+2
    elif 'DRY WHEY' in title_input:
        ticker_head = 'DY'
        if asset_type == 'options':
            title_indx = title_list.index('DY0')+2
    elif 'BMD-KUALA LUMPAR IDX' in title_input:
        ticker_head = 'kuala'
        if asset_type == 'options':
            title_indx = title_list.index('OKL')+2
    elif 'Short Dated Options on New Crop Hard Red Winter' in title_input:
        ticker_head = 'KW'
        if asset_type == 'options':
            title_indx = title_list.index('KWO')+2
    elif title_list[0] == 'ED':
        ticker_head = 'ED'
    else:
        ticker_head = ''

    if 'PUT' in title_input:
        option_type = 'P'
    elif 'CALL' in title_input:
        option_type = 'C'
    else:
        option_type = ''

    if asset_type == 'options' and ticker_head is not '':
        maturity_string = title_list[title_indx-1]
    else:
        maturity_string = []

    return {'ticker_head': ticker_head,
            'option_type': option_type,
            'asset_type': asset_type,
            'maturity_string': maturity_string}


























