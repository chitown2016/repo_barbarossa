
import contract_utilities.expiration as exp
import contract_utilities.contract_meta_info as cmi
import opportunity_constructs.spread_carry as sc
import opportunity_constructs.utilities as opUtil
import shared.statistics as stats
import get_price.get_futures_price as gfp
import signals.technical_indicators as ti
import numpy as np


def get_overnight_calendar_signals(**kwargs):

    ticker_list = kwargs['ticker_list']
    date_to = kwargs['date_to']

    #print(ticker_list)

    if 'tr_dte_list' in kwargs.keys():
        tr_dte_list = kwargs['tr_dte_list']
    else:
        tr_dte_list = [exp.get_futures_days2_expiration({'ticker': x,'date_to': date_to}) for x in ticker_list]

    if 'aggregation_method' in kwargs.keys() and 'contracts_back' in kwargs.keys():
        aggregation_method = kwargs['aggregation_method']
        contracts_back = kwargs['contracts_back']
    else:
        amcb_output = opUtil.get_aggregation_method_contracts_back(cmi.get_contract_specs(ticker_list[0]))
        aggregation_method = amcb_output['aggregation_method']
        contracts_back = amcb_output['contracts_back']

    if 'use_last_as_current' in kwargs.keys():
        use_last_as_current = kwargs['use_last_as_current']
    else:
        use_last_as_current = False

    if 'futures_data_dictionary' in kwargs.keys():
        futures_data_dictionary = kwargs['futures_data_dictionary']
    else:
        futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in [cmi.get_contract_specs(ticker_list[0])['ticker_head']]}

    if 'contract_multiplier' in kwargs.keys():
        contract_multiplier = kwargs['contract_multiplier']
    else:
        contract_multiplier = cmi.contract_multiplier[cmi.get_contract_specs(ticker_list[0])['ticker_head']]

    aligned_output = opUtil.get_aligned_futures_data(contract_list=ticker_list,
                                                          tr_dte_list=tr_dte_list,
                                                          aggregation_method=aggregation_method,
                                                          contracts_back=contracts_back,
                                                          date_to=date_to,
                                                          futures_data_dictionary=futures_data_dictionary,
                                                          use_last_as_current=use_last_as_current)

    ticker1L = ''
    ticker2L = ''
    reward_risk = np.nan
    q_carry = np.nan
    butterfly_q = np.nan
    butterfly_z = np.nan
    butterfly_q10 = np.nan
    butterfly_q25 = np.nan
    butterfly_q35 = np.nan
    butterfly_q50 = np.nan
    butterfly_q65 = np.nan
    butterfly_q75 = np.nan
    butterfly_q90 = np.nan
    butterfly_noise = np.nan
    butterfly_mean = np.nan

    if not aligned_output['success']:
        return {'success': False,
                'ticker1L': ticker1L,
                'ticker2L': ticker2L,
                'reward_risk': reward_risk,
                'q_carry': q_carry,
                'butterfly_q': butterfly_q,
                'butterfly_z': butterfly_z,
                'spread_price': np.nan,
                'butterfly_q10': butterfly_q10,
                'butterfly_q25': butterfly_q25,
                'butterfly_q35': butterfly_q35,
                'butterfly_q50': butterfly_q50,
                'butterfly_q65': butterfly_q65,
                'butterfly_q75': butterfly_q75,
                'butterfly_q90': butterfly_q90,
                'butterfly_mean': butterfly_mean,
                'butterfly_noise': butterfly_noise,
                'noise_100': np.nan,
                'dollar_noise_100': np.nan,
                'pnl1': np.nan,
                'pnl1_instant': np.nan,
                'pnl2': np.nan,
                'pnl5': np.nan,
                'pnl10': np.nan}

    aligned_data = aligned_output['aligned_data']
    current_data = aligned_output['current_data']

    aligned_data['spread_change_1'] = aligned_data['c1']['change_1']-aligned_data['c2']['change_1']
    aligned_data['spread_price'] = aligned_data['c1']['close_price']-aligned_data['c2']['close_price']
    spread_price_current = current_data['c1']['close_price']-current_data['c2']['close_price']

    pnl1 = (current_data['c1']['change1'] - current_data['c2']['change1'])*contract_multiplier
    pnl1_instant = (current_data['c1']['change1_instant'] - current_data['c2']['change1_instant']) * contract_multiplier
    pnl2 = (current_data['c1']['change2'] - current_data['c2']['change2']) * contract_multiplier
    pnl5 = (current_data['c1']['change5'] - current_data['c2']['change5']) * contract_multiplier
    pnl10 = (current_data['c1']['change10'] - current_data['c2']['change10']) * contract_multiplier

    noise_100 = np.std(aligned_data['spread_change_1'].iloc[-100:])

    if noise_100 == 0:
        noise_100 = np.nan

    spread_carry_output = sc.generate_spread_carry_sheet_4date(report_date=date_to)

    if spread_carry_output['success']:
        spread_report = spread_carry_output['spread_report']
        selected_line = spread_report[(spread_report['ticker1']==ticker_list[0])&(spread_report['ticker2']==ticker_list[1])]
        if not selected_line.empty:
            reward_risk = selected_line['reward_risk'].iloc[0]
            q_carry = selected_line['q_carry'].iloc[0]
            butterfly_q = selected_line['butterfly_q'].iloc[0]
            butterfly_z = selected_line['butterfly_z'].iloc[0]
            butterfly_q10 = selected_line['butterfly_q10'].iloc[0]
            butterfly_q25 = selected_line['butterfly_q25'].iloc[0]
            butterfly_q35 = selected_line['butterfly_q35'].iloc[0]
            butterfly_q50 = selected_line['butterfly_q50'].iloc[0]
            butterfly_q65 = selected_line['butterfly_q65'].iloc[0]
            butterfly_q75 = selected_line['butterfly_q75'].iloc[0]
            butterfly_q90 = selected_line['butterfly_q90'].iloc[0]
            butterfly_mean = selected_line['butterfly_mean'].iloc[0]
            butterfly_noise = selected_line['butterfly_noise'].iloc[0]
            ticker1L = selected_line['ticker1L'].iloc[0]
            ticker2L = selected_line['ticker2L'].iloc[0]

    return {'success': True,
            'ticker1L': ticker1L,
            'ticker2L': ticker2L,
            'reward_risk': reward_risk,
            'q_carry': q_carry,
            'butterfly_q': butterfly_q,
            'butterfly_z': butterfly_z,
            'spread_price': spread_price_current,
            'butterfly_q10': butterfly_q10,
            'butterfly_q25': butterfly_q25,
            'butterfly_q35': butterfly_q35,
            'butterfly_q50': butterfly_q50,
            'butterfly_q65': butterfly_q65,
            'butterfly_q75': butterfly_q75,
            'butterfly_q90': butterfly_q90,
            'butterfly_mean': butterfly_mean,
            'butterfly_noise': butterfly_noise,
            'noise_100': noise_100,
            'dollar_noise_100': noise_100*contract_multiplier,
            'pnl1': pnl1,
            'pnl1_instant': pnl1_instant,
            'pnl2': pnl2,
            'pnl5': pnl5,
            'pnl10': pnl10}
