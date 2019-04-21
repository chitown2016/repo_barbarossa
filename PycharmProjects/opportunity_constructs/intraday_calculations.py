
import contract_utilities.expiration as exp
import opportunity_constructs.vcs as vcs
import signals.option_signals as ops
import contract_utilities.contract_meta_info as cmi
import signals.options_filters as of
import pandas as pd


def get_intraday_vcs(**kwargs):

    if 'report_date' in kwargs.keys():
        report_date = kwargs['report_date']
    else:
        report_date = exp.doubledate_shift_bus_days()

    atm_vol_ratio = kwargs['atm_vol_ratio']

    vcs_output = vcs.generate_vcs_sheet_4date(date_to=report_date)
    vcs_pairs = vcs_output['vcs_pairs']

    if 'id' in kwargs.keys():
        id = kwargs['id']
        ticker1 = vcs_pairs['ticker1'].iloc[id]
        ticker2 = vcs_pairs['ticker2'].iloc[id]
    else:
        ticker1 = kwargs['ticker1']
        ticker2 = kwargs['ticker2']

    ticker_head = cmi.get_contract_specs(ticker1)['ticker_head']
    ticker_class = cmi.ticker_class[ticker_head]

    vcs_output = ops.get_vcs_signals(ticker_list=[ticker1, ticker2],settle_date=report_date,atm_vol_ratio=atm_vol_ratio)

    q = vcs_output['q']
    q1 = vcs_output['q1']

    filter_out = of.get_vcs_filters(data_frame_input=pd.DataFrame.from_dict({'tickerHead': [ticker_head],
                                                                             'tickerClass': [ticker_class],
                                                                              'Q': [q], 'Q1': [q1],
                                                                             'fwdVolQ': vcs_output['fwd_vol_q'],
                                                                             'atmVolRatio':vcs_output['atm_vol_ratio'],
                                                                             'downside': vcs_output['downside'],
                                                                             'upside': vcs_output['upside']}), filter_list=['long2', 'short2'])

    if filter_out['selected_frame'].empty:
        validQ = False
    else:
        validQ = True

    return {'ticker1': ticker1, 'ticker2': ticker2, 'Q': q, 'Q1': q1, 'validQ':validQ}






