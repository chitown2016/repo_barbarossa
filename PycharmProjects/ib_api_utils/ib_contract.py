
from ibapi.contract import *
import contract_utilities.contract_meta_info as cmi
import stock_utilities.stock_meta_info as smi
import shared.calendar_utilities as cu
import decimal as dec
import datetime as dt
import shared.utils as su
import math as m

conversion_from_ib_ticker_head = {'CL': 'CL',
                                  'HO': 'HO',
                                  'RB': 'RB',
                                  'GE': 'ED',
                                  'ZC': 'C',
                                  'ZW': 'W',
                                  'ZS': 'S',
                                  'ZM': 'SM',
                                  'ZL': 'BO',
                                  'KE': 'KW',
                                  'NG': 'NG',
                                  'LE': 'LC', 'HE': 'LN', 'GF': 'FC',
                                  'ES': 'ES', 'NQ': 'NQ',
                                  'AUD': 'AD', 'CAD': 'CD', 'EUR': 'EC', 'JPY': 'JY', 'GBP': 'BP',
                                  'M6E': 'MEC',
                                  'ZT': 'TU', 'ZF': 'FV', 'ZN': 'TY', 'ZB': 'US',
                                  'GC': 'GC', 'SI': 'SI',
                                  'IPE e-Brent': 'B',
                                  'Coffee C': 'KC',
                                  'Cocoa': 'CC',
                                  'Sugar No 11': 'SB',
                                  'Cotton No 2': 'CT',
                                  'FCOJ A': 'OJ'}

ib_option_trading_class_dictionary = {'AD': 'ADU','CD': 'CAU', 'EC': 'EUU', 'BP': 'GBU', 'JY': 'JPU',
                                    'C': 'OZC', 'S': 'OZS', 'BO': 'OZL', 'SM': 'OZM', 'W': 'OZW', 'LN': 'HE', 'LC': 'LE',
                                    'CL': 'LO', 'NG': 'ON', 'GC': 'OG', 'SI': 'SO', 'ES': 'ES',
                                    'TU': 'OZT', 'FV': 'OZF', 'TY': 'OZN', 'US': 'OZB'}

ib_strike_multiplier_dictionary = {'JY': 0.0000001, 'LN': 0.01, 'LC': 0.01, 'W': 0.01}

ib_multiplier_dictionary = {'BO': '60000','SM': '100', 'C': '5000', 'S': '5000','W': '5000', 'LC': '40000', 'LN': '40000', 'TY': '1000', 'HO': '42000', 'ES': '50'}

ib_underlying_multiplier_dictionary = {'JY': 0.0000001}


def get_ib_contract_from_db_ticker(**kwargs):

    sec_type = kwargs['sec_type']
    ticker = kwargs['ticker']
    contract_out = Contract()
    contract_specs_output = cmi.get_contract_specs(ticker)
    ticker_head = contract_specs_output['ticker_head']

    if sec_type in ['F', 'OF']:
        secType = "FUT"
        currency = 'USD'

        ib_ticker_head = su.get_key_in_dictionary(dictionary_input=conversion_from_ib_ticker_head,
                                                  value=contract_specs_output['ticker_head'])

        ib_contract_month = str(contract_specs_output['ticker_year']*100 + contract_specs_output['ticker_month_num'])

        exchange = cmi.get_ib_exchange_name(contract_specs_output['ticker_head'])


        contract_out.secType = secType
        contract_out.symbol = ib_ticker_head
        contract_out.exchange = exchange
        contract_out.currency = currency
        contract_out.lastTradeDateOrContractMonth = ib_contract_month

    if sec_type=='OF':
        contract_out.secType = "FOP"
        if 'option_type' in kwargs.keys():
            contract_out.right = kwargs['option_type']

        if 'strike' in kwargs.keys():
            contract_out.strike =  str(round(kwargs['strike'],2))

        contract_out.tradingClass = ib_option_trading_class_dictionary[ticker_head]
        contract_out.multiplier = ib_multiplier_dictionary.get(ticker_head, 1)


    if sec_type=='S':
        contract_out.secType = 'STK'
        contract_out.symbol = ticker
        contract_out.currency = 'USD'
        contract_out.exchange = smi.get_ib_exchange_name(ticker)

    return contract_out

def get_db_ticker_from_ib_contract(**kwargs):

    ib_contract = kwargs['ib_contract']

    if 'contract_id_dictionary' in kwargs.keys():
        contract_id_dictionary = kwargs['contract_id_dictionary']
        return su.get_key_in_dictionary(dictionary_input=contract_id_dictionary, value=ib_contract.conId)

    contract_output = {}
    contract_output['option_type'] = None
    contract_output['strike'] = None

    sec_type = ib_contract.secType


    if sec_type=='STK':
        contract_output['ticker'] = ib_contract.symbol
        contract_output['instrument'] = 'S'
        return contract_output

    ticker_head = conversion_from_ib_ticker_head[ib_contract.symbol]
    local_symbol_out = ib_contract.localSymbol.split(' ')

    date_now = cu.get_doubledate()

    if len(local_symbol_out) in [1,2]:
        contract_month_str = local_symbol_out[0][-2]
        contract_year_str = str(m.floor(date_now / 100000)) + local_symbol_out[0][-1]
    else:
        contract_month_str = cmi.full_letter_month_list[cu.three_letter_month_dictionary[local_symbol_out[3]]-1]
        contract_year_str = str(m.floor(date_now / 1000000)) + local_symbol_out[4]

    contract_output['ticker'] = ticker_head + contract_month_str + contract_year_str


    if sec_type=='FOP':
        contract_output['instrument'] = 'O'
        contract_output['option_type'] = ib_contract.right
        contract_output['strike'] = round(ib_contract.strike/ib_strike_multiplier_dictionary.get(ticker_head, 1),4)
    else:
        contract_output['instrument'] = 'F'


    return contract_output


def main():
    print(get_ib_contract_from_db_ticker(ticker='BOZ2017',sec_type='F'))

if __name__ == "__main__":
    main()