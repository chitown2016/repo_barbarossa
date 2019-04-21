__author__ = 'kocat_000'

import sys
sys.path.append(r'C:\Users\kocat_000\quantFinance\PycharmProjects')
import get_price.get_futures_price as gfp
import contract_utilities.contract_meta_info as cmi
import matplotlib.pyplot as plt
import opportunity_constructs.futures_butterfly as fb
import contract_utilities.expiration as exp
import opportunity_constructs.utilities as opUtil
import shared.calendar_utilities as cu
import signals.futures_signals as fs


def get_futures_curve_chart_4date(**kwargs):

    ticker_head = kwargs['ticker_head']
    settle_date = kwargs['settle_date']

    data2_plot = gfp.get_futures_price_preloaded(ticker_head=ticker_head, settle_date=settle_date)

    if 'tr_dte_limit' in kwargs.keys():
        data2_plot = data2_plot[data2_plot['tr_dte'] <= kwargs['tr_dte_limit']]

    ticker_year_short = data2_plot['ticker_year'] % 10
    month_letters = [cmi.letter_month_string[x-1] for x in data2_plot['ticker_month'].values]

    tick_labels = [month_letters[x]+str(ticker_year_short.values[x]) for x in range(len(month_letters))]

    plt.figure(figsize=(16, 7))
    plt.plot(range(len(data2_plot.index)),data2_plot['close_price'])
    plt.xticks(range(len(data2_plot.index)),tick_labels)
    plt.grid()
    plt.show()


def get_butterfly_panel_plot(**kwargs):

    report_date = kwargs['report_date']
    id = kwargs['id']

    bf_output = fb.generate_futures_butterfly_sheet_4date(date_to=report_date)
    butterflies = bf_output['butterflies']

    contract_list = [butterflies['ticker1'][id], butterflies['ticker2'][id], butterflies['ticker3'][id]]
    tr_dte_list = [butterflies['trDte1'][id], butterflies['trDte2'][id], butterflies['trDte3'][id]]

    if 'aggregation_method' in kwargs.keys():
        aggregation_method = kwargs['aggregation_method']
    else:
        aggregation_method = butterflies['agg'][id]

    if 'contracts_back' in kwargs.keys():
        contracts_back = kwargs['contracts_back']
    else:
        contracts_back = butterflies['cBack'][id]

    post_report_date = exp.doubledate_shift_bus_days(double_date=report_date,shift_in_days=-20)

    bf_signals_output = fs.get_futures_butterfly_signals(ticker_list=contract_list,
                                          tr_dte_list=tr_dte_list,
                                          aggregation_method=aggregation_method,
                                          contracts_back=contracts_back,
                                          date_to=post_report_date,
                                          contract_multiplier=butterflies['multiplier'][id],
                                          use_last_as_current=True)

    aligned_data = bf_signals_output['aligned_output']['aligned_data']

    new_index = list(range(len(aligned_data.index)))
    contract_change_indx = (aligned_data['c1']['ticker_year']-aligned_data['c1']['ticker_year'].shift(1)!=0).values
    front_contract_year = aligned_data['c1']['ticker_year'] % 10
    contract_change_indx[0] = False

    report_datetime = cu.convert_doubledate_2datetime(report_date)

    x_index = [x for x in new_index if aligned_data['settle_date'][x] == report_datetime][0]

    x_tick_locations = [x for x in new_index if contract_change_indx[x]]
    x_tick_locations.append(x_index)

    x_tick_values = [cmi.letter_month_string[aligned_data['c1']['ticker_month'].values[x]-1]+
                     str(front_contract_year.values[x]) for x in new_index if contract_change_indx[x]]
    x_tick_values.append('X')

    plt.figure(figsize=(16, 7))
    plt.plot(range(len(aligned_data.index)),aligned_data['residuals'])
    plt.xticks(x_tick_locations,x_tick_values)
    plt.grid()
    plt.title('Contracts: ' + str(contract_list) + ', weight2: ' + str(bf_signals_output['second_spread_weight_1'].round(2)))
    plt.show()

    return bf_signals_output


def get_butterfly_scatter_plot(**kwargs):

    report_date = kwargs['report_date']
    id = kwargs['id']

    bf_output = fb.generate_futures_butterfly_sheet_4date(date_to=report_date)
    butterflies = bf_output['butterflies']

    contract_list = [butterflies['ticker1'][id],butterflies['ticker2'][id],butterflies['ticker3'][id]]
    tr_dte_list = [butterflies['trDte1'][id],butterflies['trDte2'][id],butterflies['trDte3'][id]]

    if 'aggregation_method' in kwargs.keys():
        aggregation_method = kwargs['aggregation_method']
    else:
        aggregation_method = butterflies['agg'][id]

    if 'contracts_back' in kwargs.keys():
        contracts_back = kwargs['contracts_back']
    else:
        contracts_back = butterflies['cBack'][id]

    bf_signals_output = fs.get_futures_butterfly_signals(ticker_list=contract_list,
                                          tr_dte_list=tr_dte_list,
                                          aggregation_method=aggregation_method,
                                          contracts_back=contracts_back,
                                          date_to=report_date,
                                          contract_multiplier=butterflies['multiplier'][id])

    last5_years_indx = bf_signals_output['last5_years_indx']

    yield1 = bf_signals_output['yield1']
    yield2 = bf_signals_output['yield2']

    yield1_current = bf_signals_output['yield1_current']
    yield2_current = bf_signals_output['yield2_current']

    yield1_last5_years = yield1[last5_years_indx]
    yield2_last5_years = yield2[last5_years_indx]

    plt.figure(figsize=(16,7))
    plt.scatter(yield2, yield1, color='b')
    plt.scatter(yield2_last5_years,yield1_last5_years, color='k')
    plt.scatter(yield2_current, yield1_current, color='r')
    plt.legend(['old', 'recent', 'last'], frameon=False)
    plt.grid()
    plt.show()

    return bf_signals_output

