
import shared.calendar_utilities as cu
import get_price.curve_data as cd
import pandas as pd
import numpy as np
import shared.statistics as stats
import ta.strategy as ts
import os.path
import signals.futures_signals as fs


def get_curve_pca_report(**kwargs):

    ticker_head = kwargs['ticker_head']
    date_to = kwargs['date_to']

    if 'use_existing_filesQ' in kwargs.keys():
        use_existing_filesQ = kwargs['use_existing_filesQ']
    else:
        use_existing_filesQ = True

    output_dir = ts.create_strategy_output_dir(strategy_class='curve_pca', report_date=date_to)

    if os.path.isfile(output_dir + '/' + ticker_head + '.pkl') and use_existing_filesQ:
        pca_results = pd.read_pickle(output_dir + '/' + ticker_head + '.pkl')
        return {'pca_results': pca_results, 'success': True}

    date10_years_ago = cu.doubledate_shift(date_to, 10*365)
    datetime_to = cu.convert_doubledate_2datetime(date_to)

    if ticker_head == 'ED':

        num_contracts = 12

        rolling_data = cd.get_rolling_curve_data(ticker_head=ticker_head, num_contracts=num_contracts,
                                         front_tr_dte_limit=10,
                                        date_from=date10_years_ago,
                                        date_to=date_to)

        if datetime_to != rolling_data[0].index[-1].to_datetime():
            return {'pca_results': pd.DataFrame(), 'success': False}

        merged_data = pd.concat(rolling_data, axis=1, join='inner')
        total_range = list(range(len(rolling_data)))
        index_exclude = [len(total_range)-1]
        month_spread = [3]*(len(rolling_data)-1)

    elif ticker_head in ['CL', 'B']:

        num_monthly_contracts = 18

        if ticker_head == 'CL':
            num_semiannual_contracts = 6
        elif ticker_head == 'B':
            num_semiannual_contracts = 7

        rolling_data_monthly = cd.get_rolling_curve_data(ticker_head=ticker_head, num_contracts=num_monthly_contracts,
                                         front_tr_dte_limit=10,
                                        date_from=date10_years_ago,
                                        date_to=date_to)

        rolling_data_semiannual = cd.get_rolling_curve_data(ticker_head=ticker_head, num_contracts=num_semiannual_contracts,
                                         front_tr_dte_limit=10,
                                        date_from=date10_years_ago,
                                        month_separation=6,
                                        date_to=date_to)

        if datetime_to != rolling_data_monthly[0].index[-1].to_pydatetime() or datetime_to != rolling_data_semiannual[0].index[-1].to_pydatetime():
            return {'pca_results': pd.DataFrame(), 'success': False}

        rolling_data_merged = pd.concat(rolling_data_semiannual, axis=1)
        annual_select = rolling_data_merged['ticker_month'].iloc[-1] % 12 == 0
        rolling_data_annual = [rolling_data_semiannual[x] for x in range(len(rolling_data_semiannual)) if annual_select.values[x]]

        merged_data = pd.concat(rolling_data_monthly+rolling_data_semiannual+rolling_data_annual, axis=1, join='inner')

        total_range = list(range(len(rolling_data_monthly)+len(rolling_data_semiannual)+len(rolling_data_annual)))
        index_exclude = [len(rolling_data_monthly)-1,len(rolling_data_monthly)+len(rolling_data_semiannual)-1, len(total_range)-1]

        month_spread = [1]*(len(rolling_data_monthly)-1)+[6]*(len(rolling_data_semiannual)-1)+[12]*(len(rolling_data_annual)-1)

    yield_raw = [(merged_data['close_price'].iloc[:, x]-merged_data['close_price'].iloc[:, x+1]) /
                 merged_data['close_price'].iloc[:, x+1] for x in total_range if x not in index_exclude]
    yield_merged = pd.concat(yield_raw, axis=1)
    yield_data = 100*yield_merged.values

    change5_raw = [(merged_data['change5'].iloc[:, x]-merged_data['change5'].iloc[:, x+1]) for x in total_range
                   if x not in index_exclude]

    change5_merged = pd.concat(change5_raw, axis=1)
    change5_data = change5_merged.values

    change10_raw = [(merged_data['change10'].iloc[:, x]-merged_data['change10'].iloc[:, x+1]) for x in total_range
                   if x not in index_exclude]

    change10_merged = pd.concat(change10_raw, axis=1)
    change10_data = change10_merged.values

    change20_raw = [(merged_data['change20'].iloc[:, x]-merged_data['change20'].iloc[:, x+1]) for x in total_range
                   if x not in index_exclude]

    change20_merged = pd.concat(change20_raw, axis=1)
    change20_data = change20_merged.values

    tr_dte_raw = [merged_data['tr_dte'].iloc[:, x] for x in total_range if x not in index_exclude]
    tr_dte_merged = pd.concat(tr_dte_raw, axis=1)
    tr_dte_data = tr_dte_merged.values

    ticker_month_raw = [merged_data['ticker_month'].ix[:, x] for x in total_range if x not in index_exclude]
    ticker_month_merged = pd.concat(ticker_month_raw, axis=1)
    ticker_month_data = ticker_month_merged.values

    ticker1_list = [merged_data['ticker'].iloc[-1, x] for x in total_range if x not in index_exclude]
    ticker2_list = [merged_data['ticker'].iloc[-1, x+1] for x in total_range if x not in index_exclude]

    price_list = [(merged_data['close_price'].iloc[-1, x]-merged_data['close_price'].iloc[-1, x+1]) for x in total_range
                  if x not in index_exclude]

    pca_out = stats.get_pca(data_input=yield_data, n_components=2)

    residuals = yield_data-pca_out['model_fit']

    pca_results = pd.DataFrame.from_dict({'ticker1': ticker1_list,
                             'ticker2': ticker2_list,
                             'monthSpread': month_spread,
                             'tr_dte_front': tr_dte_data[-1],
                             'ticker_month_front': ticker_month_data[-1],
                             'residuals': residuals[-1],
                             'price': price_list,
                             'yield': yield_data[-1],
                             'z': (residuals[-1]-residuals.mean(axis=0))/residuals.std(axis=0),
                             'factor_load1': pca_out['loadings'][0],
                             'factor_load2': pca_out['loadings'][1],
                             'change5': change5_data[-1],
                             'change10': change10_data[-1],
                             'change20': change20_data[-1]})

    # notice that this date_to needs to me removed once we are done with backtesting
    seasonality_adjustment = fs.get_pca_seasonality_adjustments(ticker_head=ticker_head,date_to=date_to)

    pca_results = pd.merge(pca_results, seasonality_adjustment,how='left',on=['monthSpread','ticker_month_front'])
    pca_results['z2'] = pca_results['z']-pca_results['z_seasonal_mean']

    pca_results['residuals'] = pca_results['residuals'].round(3)
    pca_results['yield'] = pca_results['yield'].round(2)
    pca_results['z'] = pca_results['z'].round(2)
    pca_results['z2'] = pca_results['z2'].round(2)
    pca_results['z_seasonal_mean'] = pca_results['z_seasonal_mean'].round(2)
    pca_results['factor_load1'] = pca_results['factor_load1'].round(3)
    pca_results['factor_load2'] = pca_results['factor_load2'].round(3)

    pca_results.to_pickle(output_dir + '/' + ticker_head + '.pkl')

    return {'pca_results': pca_results, 'success': True}


