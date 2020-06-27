
import shared.directory_names_aux as dna
import shared.directory_names as dn
import shared.utils as su
import shared.calendar_utilities as cu
import pandas as pd
import os as os
import datetime as dt
import contract_utilities.contract_meta_info as cmi
import my_sql_routines.my_sql_utilities as msu
pd.options.mode.chained_assignment = None
import ta.strategy as ts
import numpy as np
import math as m
cme_direct_fill_file_name = 'cme_direct_fills.csv'
manual_trade_entry_file_name = 'manual_trade_entry.csv'


conversion_from_tt_ticker_head = {'GE': 'ED',
                                  'ZC': 'C',
                                  'ZW': 'W',
                                  'ZS': 'S',
                                  'ZM': 'SM',
                                  'ZL': 'BO',
                                  'KE': 'KW',
                                  'NG': 'NG',
                                  'LE': 'LC', 'HE': 'LN', 'GF': 'FC',
                                  '6A': 'AD', '6C': 'CD', '6E': 'EC', '6J': 'JY', '6B': 'BP',
                                  'ZT': 'TU', 'ZF': 'FV', 'ZN': 'TY', 'ZB': 'US',
                                  'BRN': 'B'}

conversion_from_cme_direct_ticker_head = {'S': 'S',
                                          'W': 'W',
                                          'C': 'C',
                                          'LC': 'LC',
                                          'LN': 'LN',
                                          'J1': 'JY',
                                          '21': 'TY',
                                          'C1': 'CD',
                                          'EC': 'EC',
                                          'GBU': 'BP',
                                          'EUU': 'EC',
                                          'BP': 'BP',
                                          'CAU': 'CD',
                                          'LO': 'CL',
                                          'CL': 'CL',
                                          'SO': 'SI',
                                          'SI': 'SI',
                                          'OG': 'GC',
                                          'ON': 'NG',
                                          'NG': 'NG',
                                          'GC': 'GC',
                                          'ES': 'ES',
                                          '07': 'BO',
                                          '06': 'SM'}

product_type_instrument_conversion = {'Future': 'F'}


def convert_trade_price_from_tt(**kwargs):

    ticker_head = kwargs['ticker_head']
    price = kwargs['price']

    if pd.isnull(price):
        return np.NaN

    if ticker_head in ['CL', 'BO', 'ED', 'ES', 'NQ','B', 'KC', 'SB', 'CC', 'CT', 'OJ','HO','RB', 'AD', 'CD', 'EC', 'BP','LC', 'LN', 'FC', 'NG', 'SI','SM','GC']:
        converted_price = float(price)
    elif ticker_head in ['C', 'S', 'KW', 'W']:
        split_list = price.split("'")
        converted_price = float(split_list[0]) + float(split_list[1])*0.125
    elif ticker_head == 'JY':
        converted_price = price*1e7
    elif ticker_head =='TU':
        split_list = price.split("'")
        aux1 = float(split_list[0])
        aux2 = split_list[1][-1]
        aux3 = float(split_list[1][:-1])

        if aux2 == '1':
            aux4 = 0.125
        elif aux2 == '2':
            aux4 = 0.25
        elif aux2 == '3':
            aux4 = 0.375
        elif aux2 == '5':
            aux4 = 0.5
        elif aux2 == '6':
            aux4 = 0.625
        elif aux2 == '7':
            aux4 = 0.75
        elif aux2 == '8':
            aux4 = 0.875
        elif aux2 == '0':
            aux4 = 0

        converted_price = aux1 + (aux3 + aux4) / 32

    elif ticker_head in ['FV', 'TY']:

        split_list = price.split("'")
        aux1 = float(split_list[0])
        aux2 = split_list[1][-1]
        aux3 = float(split_list[1][:-1])

        if aux2 == '2':
            aux4 = 0.25
        elif aux2 == '5':
            aux4 = 0.5
        elif aux2 == '7':
            aux4 = 0.75
        elif aux2 == '0':
            aux4 = 0

        converted_price = aux1+(aux3+aux4)/32

    elif ticker_head == 'US':

        split_list = price.split("'")
        aux1 = float(split_list[0])
        aux2 = float(split_list[1])

        converted_price = aux1 + aux2/ 32

    return converted_price


def convert_trade_price_from_cme_direct(**kwargs):

    ticker_head = kwargs['ticker_head']
    price = kwargs['price']

    if ticker_head in ['JY']:
        converted_price = price*(10**7)
    else:
        converted_price = price

    return converted_price


def convert_from_cme_contract_code(contract_code):

    split_list = contract_code.split(':')

    result_dictionary = {'instrument': split_list[1]}

    ticker_month = int(split_list[3]) % 100
    ticker_year = m.floor(float(split_list[3])/100)

    ticker_head = conversion_from_cme_direct_ticker_head[split_list[2]]

    result_dictionary['ticker'] = ticker_head + cmi.full_letter_month_list[ticker_month-1] + str(ticker_year)
    result_dictionary['ticker_head'] = ticker_head

    if len(split_list) >= 6:
        result_dictionary['option_type'] = split_list[4]
        result_dictionary['strike_price'] = split_list[5]
    else:
        result_dictionary['option_type'] = None
        result_dictionary['strike_price'] = None

    return result_dictionary


def load_latest_tt_fills(**kwargs):

    file_list = os.listdir(dna.tt_fill_directory)
    num_files = len(file_list)

    time_list = []

    for i in range(num_files):
        time_list.append(os.path.getmtime(dna.tt_fill_directory + '/' + file_list[i]))

    loc_latest_file = time_list.index(max(time_list))

    dna.get_directory_name(ext='daily') + '/ttFills.xlsx'

    tt_export_frame = pd.read_excel(dna.get_directory_name(ext='daily') + '/ttFills.xlsx',header=None,
                                    names=['trade_date','trade_time','exchange','product','contract','prod_type','B/S','qty','price','P/F', 'route','account','order_tag',
                                           'originator','current_user','tt_order_id','parent_id'])

    tt_export_frame_filtered = tt_export_frame[tt_export_frame['prod_type']=='Future']

    if 'tags2exclude' in kwargs.keys():

        tt_export_frame_filtered['order_tag'] = tt_export_frame_filtered['order_tag'].astype('str')
        tt_export_frame_filtered = tt_export_frame_filtered[[not any([y in x for y in kwargs['tags2exclude']]) for x in tt_export_frame_filtered['order_tag']]]

    return tt_export_frame_filtered


def load_cme__fills(**kwargs):

    fill_frame = pd.read_csv(dna.get_directory_name(ext='daily') + '/' + cme_direct_fill_file_name, header=1)

    fill_frame_filtered = fill_frame[fill_frame['IsStrategy'] == False]
    fill_frame_filtered.reset_index(inplace=True,drop=True)


    return fill_frame_filtered[['ContractCode', 'Side', 'Price', 'FilledQuantity']]


def get_formatted_tt_fills(**kwargs):

    fill_frame = load_latest_tt_fills(**kwargs)
    fill_frame = fill_frame[fill_frame['exchange'] != 'CME*']

    datetime_conversion = [dt.datetime.strptime(x[-5:],'%b%y') for x in fill_frame['contract']]
    fill_frame['ticker_year'] = [x.year for x in datetime_conversion]
    fill_frame['ticker_month'] = [x.month for x in datetime_conversion]
    fill_frame['ticker_head'] = [conversion_from_tt_ticker_head.get(x,x) for x in fill_frame['product']]

    fill_frame['ticker'] = [fill_frame.loc[x,'ticker_head'] +
                            cmi.full_letter_month_list[fill_frame.loc[x,'ticker_month']-1] +
                            str(fill_frame.loc[x,'ticker_year']) for x in fill_frame.index]

    fill_frame['trade_price'] = [convert_trade_price_from_tt(price=fill_frame.loc[x,'price'],ticker_head=fill_frame.loc[x,'ticker_head'])
                                 for x in fill_frame.index]

    fill_frame['PQ'] = fill_frame['trade_price']*fill_frame['qty']

    grouped = fill_frame.groupby(['ticker','B/S'])

    aggregate_trades = pd.DataFrame()
    aggregate_trades['trade_price'] = grouped['PQ'].sum()/grouped['qty'].sum()
    aggregate_trades['trade_quantity'] = grouped['qty'].sum()

    if 'S' in aggregate_trades.index.levels[1]:
        aggregate_trades.loc[(slice(None),'S'),'trade_quantity']=-aggregate_trades.loc[(slice(None),'S'),'trade_quantity']
    aggregate_trades['ticker'] = grouped['ticker'].first()
    aggregate_trades['ticker_head'] = grouped['ticker_head'].first()
    aggregate_trades['instrument'] = [product_type_instrument_conversion[x] for x in grouped['prod_type'].first()]

    aggregate_trades['option_type'] = None
    aggregate_trades['strike_price'] = None
    aggregate_trades['real_tradeQ'] = True

    return {'raw_trades': fill_frame, 'aggregate_trades': aggregate_trades}


def get_tagged_tt_fills(**kwargs):

    fill_frame = load_latest_tt_fills(**kwargs)
    fill_frame = fill_frame[fill_frame['exchange'] != 'CME*']


    datetime_conversion = [dt.datetime.strptime(x[-5:],'%b%y') for x in fill_frame['contract']]
    fill_frame['ticker_year'] = [x.year for x in datetime_conversion]
    fill_frame['ticker_month'] = [x.month for x in datetime_conversion]
    fill_frame['ticker_head'] = [conversion_from_tt_ticker_head.get(x,x) for x in fill_frame['product']]

    fill_frame['ticker'] = [fill_frame.loc[x,'ticker_head'] +
                            cmi.full_letter_month_list[fill_frame.loc[x,'ticker_month']-1] +
                            str(fill_frame.loc[x,'ticker_year']) for x in fill_frame.index]

    fill_frame['trade_price'] = [convert_trade_price_from_tt(price=fill_frame.loc[x,'price'],ticker_head=fill_frame.loc[x,'ticker_head'])
                                 for x in fill_frame.index]

    fill_frame['PQ'] = fill_frame['trade_price']*fill_frame['qty']

    grouped = fill_frame.groupby(['ticker','B/S', 'order_tag'])

    aggregate_trades = pd.DataFrame()
    aggregate_trades['trade_price'] = grouped['PQ'].sum()/grouped['qty'].sum()
    aggregate_trades['trade_quantity'] = grouped['qty'].sum()

    aggregate_trades.loc[(slice(None),'S'),'trade_quantity']=-aggregate_trades.loc[(slice(None),'S'),'trade_quantity']
    aggregate_trades['ticker'] = grouped['ticker'].first()
    aggregate_trades['ticker_head'] = grouped['ticker_head'].first()
    aggregate_trades['order_tag'] = grouped['order_tag'].first()
    aggregate_trades['instrument'] = [product_type_instrument_conversion[x] for x in grouped['prod_type'].first()]

    aggregate_trades['option_type'] = None
    aggregate_trades['strike_price'] = None
    aggregate_trades['real_tradeQ'] = True

    ta_directory = dn.get_dated_directory_extension(ext='ta', folder_date=cu.get_doubledate())
    trade_alias_frame = pd.read_csv(ta_directory + '/tradeAlias2.csv')

    combined_list = [None]*len(trade_alias_frame.index)

    for i in range(len(trade_alias_frame.index)):

        selected_trades = aggregate_trades[aggregate_trades['order_tag'] == trade_alias_frame['tag'].iloc[i]]
        combined_list[i] = selected_trades[['ticker','option_type','strike_price','trade_price','trade_quantity','instrument','real_tradeQ']]
        combined_list[i]['alias'] = trade_alias_frame['alias'].iloc[i]

    aggregate_trades = pd.concat(combined_list).reset_index(drop=True)

    return {'raw_trades': fill_frame, 'aggregate_trades': aggregate_trades}

def get_ticker_from_tt_instrument_name_and_product_name(**kwargs):

    instrument_name = kwargs['instrument_name']
    #print(instrument_name)
    product_name = kwargs['product_name']

    ticker_head = conversion_from_tt_ticker_head.get(product_name,product_name)

    string_list = instrument_name.split()

    exchange_string = string_list[0]
    maturity_string = string_list[-1]

    if ('Spread' in instrument_name) & \
            ('Q1' not in instrument_name) & ('Q2' not in instrument_name) &('Q3' not in instrument_name) &('Q4' not in instrument_name) &\
            (exchange_string == 'ICE_IPE'):
        contract1_datetime = dt.datetime.strptime(maturity_string[0:5],'%b%y')
        contract2_datetime = dt.datetime.strptime(maturity_string[6:11],'%b%y')
        ticker = ticker_head + cmi.full_letter_month_list[contract1_datetime.month-1] + str(contract1_datetime.year) + '-' + \
               ticker_head + cmi.full_letter_month_list[contract2_datetime.month-1] + str(contract2_datetime.year)

    elif ('Calendar' in instrument_name) & (exchange_string == 'CME'):
        contract1_datetime = dt.datetime.strptime(maturity_string[0:5],'%b%y')
        contract2_datetime = dt.datetime.strptime(maturity_string[9:14],'%b%y')
        ticker = ticker_head + cmi.full_letter_month_list[contract1_datetime.month-1] + str(contract1_datetime.year) + '-' + \
               ticker_head + cmi.full_letter_month_list[contract2_datetime.month-1] + str(contract2_datetime.year)
    elif (len(maturity_string) >= 5) &('Spread' not in instrument_name)&('Butterfly' not in instrument_name)&('x' not in instrument_name):
        contract_datetime = dt.datetime.strptime(maturity_string,'%b%y')
        ticker = ticker_head + cmi.full_letter_month_list[contract_datetime.month-1] + str(contract_datetime.year)
    else:
        ticker = ''

    return {'ticker': ticker, 'ticker_head': ticker_head }


def convert_ticker_from_db2tt(db_ticker):

    if '-' in db_ticker:
        spreadQ = True
        ticker_list = db_ticker.split('-')
    else:
        spreadQ = False
        ticker_list = [db_ticker]

    contract_specs_list = [cmi.get_contract_specs(x) for x in ticker_list]
    ticker_head_list = [x['ticker_head'] for x in contract_specs_list]
    exchange_traded = cmi.get_exchange_traded(ticker_head_list[0])

    if exchange_traded == 'CME':
        exchange_string = 'CME'
    elif exchange_traded == 'ICE':
        exchange_string = 'ICE_IPE'

    if ticker_head_list[0] in conversion_from_tt_ticker_head:
        tt_ticker_head = su.get_key_in_dictionary(dictionary_input=conversion_from_tt_ticker_head, value=ticker_head_list[0])
    else:
        tt_ticker_head = ticker_head_list[0]

    maturity_string_list = [dt.date(x['ticker_year'],x['ticker_month_num'],1).strftime('%b%y') for x in contract_specs_list]

    if spreadQ:
        if exchange_traded == 'ICE':
            tt_ticker = exchange_string + ' ' + tt_ticker_head + ' Spread ' + maturity_string_list[0] + '-' + maturity_string_list[1]
        elif exchange_traded == 'CME':
            tt_ticker = exchange_string + ' Calendar- 1x' + tt_ticker_head + ' ' + maturity_string_list[0] + '--1x' + maturity_string_list[1]
    else:
        tt_ticker = exchange_string + ' ' + tt_ticker_head + ' ' + maturity_string_list[0]

    return tt_ticker


def get_formatted_cme_direct_fills(**kwargs):

    fill_frame = load_cme__fills(**kwargs)

    formatted_frame = pd.DataFrame([convert_from_cme_contract_code(x) for x in fill_frame['ContractCode']])

    formatted_frame['strike_price'] = formatted_frame['strike_price'].astype('float64')

    formatted_frame['trade_price'] = fill_frame['Price']

    formatted_frame['trade_price'] = [convert_trade_price_from_cme_direct(ticker_head=formatted_frame['ticker_head'].iloc[x],
                                        price=formatted_frame['trade_price'].iloc[x]) for x in range(len(formatted_frame.index))]

    formatted_frame['strike_price'] = [convert_trade_price_from_cme_direct(ticker_head=formatted_frame['ticker_head'].iloc[x],
                                        price=formatted_frame['strike_price'].iloc[x]) for x in range(len(formatted_frame.index))]

    formatted_frame['trade_quantity'] = fill_frame['FilledQuantity']
    formatted_frame['side'] = fill_frame['Side']

    formatted_frame['PQ'] = formatted_frame['trade_price']*formatted_frame['trade_quantity']

    formatted_frame['generalized_ticker'] = formatted_frame['ticker']
    option_indx = formatted_frame['instrument'] == 'O'
    formatted_frame['generalized_ticker'][option_indx] = formatted_frame['ticker'][option_indx] + '-' + \
                                                         formatted_frame['option_type'][option_indx] + '-' + \
                                                         formatted_frame['strike_price'][option_indx].astype(str)

    grouped = formatted_frame.groupby(['generalized_ticker', 'side'])

    aggregate_trades = pd.DataFrame()
    aggregate_trades['trade_price'] = grouped['PQ'].sum()/grouped['trade_quantity'].sum()
    aggregate_trades['trade_quantity'] = grouped['trade_quantity'].sum()

    if 'Sell' in list(aggregate_trades.index.get_level_values(1)):
        aggregate_trades.loc[(slice(None), 'Sell'),'trade_quantity'] =- \
        aggregate_trades.loc[(slice(None), 'Sell'),'trade_quantity']

    aggregate_trades['ticker'] = grouped['ticker'].first()
    aggregate_trades['ticker_head'] = grouped['ticker_head'].first()
    aggregate_trades['instrument'] = grouped['instrument'].first()
    aggregate_trades['option_type'] = grouped['option_type'].first()
    aggregate_trades['strike_price'] = grouped['strike_price'].first()
    aggregate_trades['real_tradeQ'] = True

    return {'raw_trades': fill_frame, 'aggregate_trades': aggregate_trades }


def get_formatted_manual_entry_fills(**kwargs):

    fill_frame = pd.read_csv(dna.get_directory_name(ext='daily') + '/' + manual_trade_entry_file_name)
    formatted_frame = fill_frame
    formatted_frame.rename(columns={'optionType': 'option_type',
                                    'strikePrice': 'strike_price',
                                    'tradePrice': 'trade_price',
                                    'quantity': 'trade_quantity'},
                           inplace=True)

    formatted_frame['strike_price'] = formatted_frame['strike_price'].astype('float64')

    formatted_frame['PQ'] = formatted_frame['trade_price']*formatted_frame['trade_quantity']

    formatted_frame['instrument'] = 'O'



    formatted_frame.loc[formatted_frame['option_type'].isnull(),'instrument'] = 'F'
    formatted_frame.loc[[cmi.is_stockQ(x) for x in formatted_frame['ticker']], 'instrument'] = 'S'

    option_type = formatted_frame['option_type']
    formatted_frame['option_type']= option_type.where(pd.notnull(option_type),None)

    option_indx = formatted_frame['instrument'] == 'O'

    formatted_frame['generalized_ticker'] = formatted_frame['ticker']
    formatted_frame['generalized_ticker'][option_indx] = formatted_frame['ticker'][option_indx] + '-' + \
                                                         formatted_frame['option_type'][option_indx] + '-' + \
                                                         formatted_frame['strike_price'][option_indx].astype(str)

    formatted_frame['side'] = np.sign(formatted_frame['trade_quantity'])
    formatted_frame['ticker_head'] = [cmi.get_contract_specs(x)['ticker_head'] for x in formatted_frame['ticker']]

    grouped = formatted_frame.groupby(['generalized_ticker', 'side'])

    aggregate_trades = pd.DataFrame()
    aggregate_trades['trade_price'] = grouped['PQ'].sum()/grouped['trade_quantity'].sum()
    aggregate_trades['trade_quantity'] = grouped['trade_quantity'].sum()
    aggregate_trades['ticker'] = grouped['ticker'].first()
    aggregate_trades['ticker_head'] = grouped['ticker_head'].first()
    aggregate_trades['instrument'] = grouped['instrument'].first()
    aggregate_trades['option_type'] = grouped['option_type'].first()
    aggregate_trades['strike_price'] = grouped['strike_price'].first()
    aggregate_trades['real_tradeQ'] = True

    return {'raw_trades': fill_frame, 'aggregate_trades': aggregate_trades }


def assign_trades_2strategies(**kwargs):

    trade_source = kwargs['trade_source']

    if trade_source == 'tt':
        formatted_fills = get_formatted_tt_fills(**kwargs)
    elif trade_source == 'cme_direct':
        formatted_fills = get_formatted_cme_direct_fills()
    elif trade_source == 'manual_entry':
        formatted_fills = get_formatted_manual_entry_fills()

    aggregate_trades = formatted_fills['aggregate_trades']

    allocation_frame = pd.read_excel(dna.get_directory_name(ext='daily') + '/' + 'trade_allocation.xlsx')
    combined_list = [None]*len(allocation_frame.index)

    for i in range(len(allocation_frame.index)):

        if allocation_frame['criteria'][i]=='tickerhead':

            selected_trades = aggregate_trades[aggregate_trades['ticker_head'] == allocation_frame['value'][i]]

        elif allocation_frame['criteria'][i]=='ticker':

            selected_trades = aggregate_trades[aggregate_trades['ticker'] == allocation_frame['value'][i]]

        combined_list[i] = selected_trades[['ticker','option_type','strike_price','trade_price','trade_quantity','instrument','real_tradeQ']]
        combined_list[i]['alias'] = allocation_frame['alias'][i]

    return pd.concat(combined_list).reset_index(drop=True)


def load_tt_trades(**kwargs):

    trade_frame = assign_trades_2strategies(trade_source='tt',**kwargs)
    con = msu.get_my_sql_connection(**kwargs)
    ts.load_trades_2strategy(trade_frame=trade_frame,con=con,**kwargs)

    if 'con' not in kwargs.keys():
        con.close()


def load_tagged_tt_trades(**kwargs):

    assign_output = get_tagged_tt_fills(**kwargs)
    aggregate_trades = assign_output['aggregate_trades']
    con = msu.get_my_sql_connection(**kwargs)

    final_alias_dictionary = {}
    unique_alias_list = aggregate_trades['alias'].unique()

    open_strategy_frame = ts.get_open_strategies()

    for i in range(len(unique_alias_list)):
        if ~open_strategy_frame['alias'].str.contains(unique_alias_list[i]).any():
            print('Need to create ' + unique_alias_list[i])
            if '_ocs' in unique_alias_list[i]:
                gen_output = ts.generate_db_strategy_from_alias_and_class(alias=unique_alias_list[i],
                                                                          strategy_class='ocs')
                final_alias_dictionary[unique_alias_list[i]] = gen_output['alias']
        else:
            final_alias_dictionary[unique_alias_list[i]] = unique_alias_list[i]

    aggregate_trades['alias_final'] = [final_alias_dictionary[x] for x in aggregate_trades['alias']]
    aggregate_trades['alias'] = aggregate_trades['alias_final']

    ts.load_trades_2strategy(trade_frame=aggregate_trades,con=con,**kwargs)

    if 'con' not in kwargs.keys():
        con.close()


def load_cme_direct_trades(**kwargs):

    trade_frame = assign_trades_2strategies(trade_source='cme_direct')
    con = msu.get_my_sql_connection(**kwargs)
    ts.load_trades_2strategy(trade_frame=trade_frame,con=con,**kwargs)

    if 'con' not in kwargs.keys():
        con.close()


def load_manual_entry_trades(**kwargs):

    trade_frame = assign_trades_2strategies(trade_source='manual_entry')
    con = msu.get_my_sql_connection(**kwargs)
    ts.load_trades_2strategy(trade_frame=trade_frame,con=con,**kwargs)

    if 'con' not in kwargs.keys():
        con.close()