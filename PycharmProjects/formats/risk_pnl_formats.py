
import risk.historical_risk as hr
import shared.directory_names as dn
import contract_utilities.expiration as exp
import ta.portfolio_manager as tpm
import pandas as pd


def generate_historic_risk_report(**kwargs):

    if 'as_of_date' in kwargs.keys():
        as_of_date = kwargs['as_of_date']
    else:
        as_of_date = exp.doubledate_shift_bus_days()
        kwargs['as_of_date'] = as_of_date

    ta_output_dir = dn.get_dated_directory_extension(folder_date=as_of_date, ext='ta')

    historic_risk_output = hr.get_historical_risk_4open_strategies(**kwargs)

    strategy_risk_frame = historic_risk_output['strategy_risk_frame']
    ticker_head_risk_frame = historic_risk_output['ticker_head_risk_frame']

    writer = pd.ExcelWriter(ta_output_dir + '/risk.xlsx', engine='xlsxwriter')

    strategy_risk_frame.to_excel(writer, sheet_name='strategies')
    ticker_head_risk_frame.to_excel(writer, sheet_name='tickerHeads')

    worksheet_strategies = writer.sheets['strategies']
    worksheet_ticker_heads = writer.sheets['tickerHeads']

    worksheet_strategies.freeze_panes(1, 0)
    worksheet_ticker_heads.freeze_panes(1, 0)

    worksheet_strategies.autofilter(0, 0, len(strategy_risk_frame.index),
                              len(strategy_risk_frame.columns))

    worksheet_ticker_heads.autofilter(0, 0, len(ticker_head_risk_frame.index),
                                   len(ticker_head_risk_frame.columns))


def generate_portfolio_pnl_report(**kwargs):

    if 'as_of_date' in kwargs.keys():
        as_of_date = kwargs['as_of_date']
    else:
        as_of_date = exp.doubledate_shift_bus_days()
        kwargs['as_of_date'] = as_of_date

    ta_output_dir = dn.get_dated_directory_extension(folder_date=as_of_date, ext='ta')

    daily_pnl_frame = tpm.get_daily_pnl_snapshot(**kwargs)

    writer = pd.ExcelWriter(ta_output_dir + '/pnl_' + kwargs['name'] + '.xlsx', engine='xlsxwriter')
    daily_pnl_frame.to_excel(writer, sheet_name='strategies')
    worksheet_strategies = writer.sheets['strategies']

    worksheet_strategies.set_column('B:B', 30)
    worksheet_strategies.freeze_panes(1, 0)
    worksheet_strategies.autofilter(0, 0, len(daily_pnl_frame.index),
                              len(daily_pnl_frame.columns))

    writer.save()



