
import my_sql_routines.my_sql_utilities as msu
import get_price.get_options_price as gop
import get_price.get_futures_price as gfp
import option_models.quantlib_option_models as qom
import interest_curve.get_rate_from_stir as grfs
import option_models.utils as oput
import contract_utilities.expiration as exp
import contract_utilities.contract_meta_info as cmi
import pandas as pd


def cal_greeks_4option_maturity(**kwargs):

    option_prices = gop.get_options_price_from_db(**kwargs)

    if option_prices.empty:
        return pd.DataFrame()

    option_prices = option_prices[option_prices['strike'] > 0]
    option_prices.reset_index(drop=True, inplace=True)

    contract_specs_out = cmi.get_contract_specs(kwargs['ticker'])
    exercise_type = cmi.get_option_exercise_type(**contract_specs_out)

    underlying_ticker = oput.get_option_underlying(**kwargs)

    futures_price_output = gfp.get_futures_price_preloaded(ticker=underlying_ticker, settle_date=kwargs['settle_date'])

    if futures_price_output.empty:
        return pd.DataFrame()

    underlying_price = futures_price_output['close_price'].iloc[0]

    expiration_datetime = exp.get_expiration_from_db(instrument='options', **kwargs)
    expiration_date = int(expiration_datetime.strftime('%Y%m%d'))

    interest_rate = grfs.get_simple_rate(as_of_date=kwargs['settle_date'], date_to=expiration_date)['rate_output']

    option_greeks = [qom.get_option_greeks(underlying=underlying_price,
                                           option_price=float(option_prices['close_price'].iloc[x]),
                                           strike=float(option_prices['strike'].iloc[x]),
                                           risk_free_rate=interest_rate,
                                           expiration_date=expiration_date,
                                           calculation_date=kwargs['settle_date'],
                                           option_type=option_prices['option_type'].iloc[x],
                                           exercise_type=exercise_type) for x in range(len(option_prices.index))]

    greek_frame = pd.DataFrame(option_greeks)

    return pd.concat([greek_frame[['delta', 'gamma', 'implied_vol', 'theta', 'vega']], option_prices], axis=1)










