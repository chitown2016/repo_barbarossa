
import os

root_home = r'C:\Users\kocat_000\quantFinance'
root_work = r'C:\Research'
root_quantgo = r'D:\Research'
root_work_dropbox = r'C:\Users\ekocatulum\Dropbox'
tt_fill_directory = r'C:\tt\datfiles\Export'


extension_dict = {'presaved_futures_data': '/data/futures_data',
                  'book_snapshot_data': '/data/book_snapshot',
                  'config': '/config',
                  'ib_data': '/ib_data',
                  'drop_box_trading':'/trading',
                  'commitments_of_traders_data': '/data/fundamental_data/cot_data',
                  'fundamental_data': '/data/fundamental_data',
                  'intraday_ttapi_data': '/data/intraday_data/tt_api',
                  'intraday_ttapi_data_fixed_interval': '/data/intraday_data/tt_api_fixed_interval',
                  'raw_options_data': '/data/options_data_raw',
                  'coinbase_data': '/data/coinbase',
                  'options_backtesting_data': '/data/options_backtesting_data',
                  'option_model_test_data': '/data/option_model_tests',
                  'aligned_time_series_output': '/data/alignedTimeSeriesOutputTemp',
                  'ta': '/ta',
                  'admin': '/admin',
                  'test_data': '/data/test_data',
                  'stock_data': '/data/stock_data',
                  'iex_stock_data': '/data/iex_stock_data',
                  'binance': '/data/binance',
                  'strategy_output': '/strategy_output',
                  'optimization':  '/strategy_output/optimization',
                  'backtest_results': '/backtest_results',
                  'daily': '/daily',
                  'log': '/logs',
                   'man_positions': '/man_positions',
                  'python_file': '/PycharmProjects'}


def get_directory_name(**kwargs):

    computer_name = os.environ['COMPUTERNAME']

    ext = kwargs['ext']

    if computer_name == '601-TREKW71' or computer_name == '601-TREKW72' or computer_name=='601-TREKW74':
        if ext in ['ib_data', 'drop_box_trading']:
            root_dir = root_work_dropbox
        else:
            root_dir = root_work
    elif computer_name == 'WIN-3G1R7L5NT4H':
        root_dir = root_quantgo
    else:
        root_dir = root_home

    directory_name = root_dir + extension_dict[ext]

    if not os.path.exists(directory_name):
        os.makedirs(directory_name)

    return directory_name