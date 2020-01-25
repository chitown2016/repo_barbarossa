
import QuantLib as ql
import shared.calendar_utilities as cu
import math as m
import numpy as np

day_count_obj = ql.Actual365Fixed()
calendar_obj = ql.UnitedStates()

def get_option_greeks(**kwargs):

    # risk_free_rate and volatility in whole points not percentage points
    # for example enter 0.02 for 2 percent interest rate
    # enter 0.25 for 25 percent annual volatility

    underlying = kwargs['underlying']
    strike = kwargs['strike']
    risk_free_rate = kwargs['risk_free_rate']
    expiration_date = kwargs['expiration_date']
    calculation_date = kwargs['calculation_date']
    option_type = kwargs['option_type'].upper()
    exercise_type = kwargs['exercise_type'].upper()
    dividend_rate = 0

    expiration_datetime = cu.convert_doubledate_2datetime(expiration_date)
    calculation_datetime = cu.convert_doubledate_2datetime(calculation_date)

    expiration_date_obj = ql.Date(expiration_datetime.day, expiration_datetime.month, expiration_datetime.year)
    calculation_date_obj = ql.Date(calculation_datetime.day, calculation_datetime.month, calculation_datetime.year)

    cal_dte = day_count_obj.dayCount(calculation_date_obj, expiration_date_obj)

    nan_greeks = {'option_price': np.NaN,
            'implied_vol': np.NaN,
            'delta': np.NaN,
            'vega': np.NaN,
            'theta': np.NaN,
            'cal_dte': cal_dte,
            'gamma': np.NaN}

    if 'option_price' in kwargs.keys():
        if option_type == 'C':
            if kwargs['option_price']+strike-underlying <= 10**(-12):
                nan_greeks['delta'] = 1
                return nan_greeks
        elif option_type == 'P':
            if kwargs['option_price']-strike+underlying <= 10**(-12):
                nan_greeks['delta'] = -1
                return nan_greeks

    if cal_dte == 0:
        if option_type == 'C':
            if strike <= underlying:
                nan_greeks['delta'] = 1
            else:
                nan_greeks['delta'] = 0
        elif option_type == 'P':
            if strike >= underlying:
                nan_greeks['delta'] = -1
            else:
                nan_greeks['delta'] = 0

        return nan_greeks

    if 'implied_vol' in kwargs.keys():
        implied_vol = kwargs['implied_vol']
    else:
        implied_vol = 0.15

    if 'engine_name' in kwargs.keys():
        engine_name = kwargs['engine_name']
    else:
        engine_name = 'baw'

    if option_type == 'C':
        option_type_obj = ql.Option.Call
    elif option_type == 'P':
        option_type_obj = ql.Option.Put

    ql.Settings.instance().evaluationDate = calculation_date_obj

    if exercise_type == 'E':
        exercise_obj = ql.EuropeanExercise(expiration_date_obj)
    elif exercise_type == 'A':
        exercise_obj = ql.AmericanExercise(calculation_date_obj, expiration_date_obj)

    #print('years to expitation: ' + str(day_count_obj.yearFraction(calculation_date_obj, expiration_date_obj)))

    #print('spot: ' + str(underlying/m.exp(day_count_obj.yearFraction(calculation_date_obj, expiration_date_obj)*risk_free_rate)))

    #underlying_obj = ql.QuoteHandle(ql.SimpleQuote(underlying/m.exp(day_count_obj.yearFraction(calculation_date_obj, expiration_date_obj)*risk_free_rate)))
    underlying_obj = ql.QuoteHandle(ql.SimpleQuote(underlying))

    flat_ts_obj = ql.YieldTermStructureHandle(ql.FlatForward(calculation_date_obj, risk_free_rate, day_count_obj))

    dividend_yield_obj = ql.YieldTermStructureHandle(ql.FlatForward(calculation_date_obj, dividend_rate, day_count_obj))
    flat_vol_ts_obj = ql.BlackVolTermStructureHandle(ql.BlackConstantVol(calculation_date_obj, calendar_obj, implied_vol, day_count_obj))

    #bsm_process = ql.BlackScholesMertonProcess(underlying_obj, dividend_yield_obj, flat_ts_obj, flat_vol_ts_obj)
    bsm_process = ql.BlackProcess(underlying_obj, flat_ts_obj, flat_vol_ts_obj)

    payoff = ql.PlainVanillaPayoff(option_type_obj, strike)
    option_obj = ql.VanillaOption(payoff, exercise_obj)

    if (engine_name == 'baw')&(exercise_type == 'A'):
        option_obj.setPricingEngine(ql.BaroneAdesiWhaleyEngine(bsm_process))
    elif (engine_name == 'fda')&(exercise_type == 'A'):
        option_obj.setPricingEngine(ql.FDAmericanEngine(bsm_process, 100, 100))
    elif exercise_type == 'E':
        option_obj.setPricingEngine(ql.AnalyticEuropeanEngine(bsm_process))
    option_price = option_obj.NPV()

    if 'option_price' in kwargs.keys():
        try:
            implied_vol = option_obj.impliedVolatility(targetValue=kwargs['option_price'], process=bsm_process,accuracy=0.00001)

            flat_vol_ts_obj = ql.BlackVolTermStructureHandle(ql.BlackConstantVol(calculation_date_obj, calendar_obj, implied_vol, day_count_obj))
            #bsm_process = ql.BlackScholesMertonProcess(underlying_obj, dividend_yield_obj, flat_ts_obj, flat_vol_ts_obj)
            bsm_process = ql.BlackProcess(underlying_obj, flat_ts_obj, flat_vol_ts_obj)

            if (engine_name == 'baw')&(exercise_type == 'A'):
                option_obj.setPricingEngine(ql.BaroneAdesiWhaleyEngine(bsm_process))
            elif (engine_name == 'fda')&(exercise_type == 'A'):
                option_obj.setPricingEngine(ql.FDAmericanEngine(bsm_process, 100, 100))
            elif exercise_type == 'E':
                option_obj.setPricingEngine(ql.AnalyticEuropeanEngine(bsm_process))

            option_price = option_obj.NPV()
        except Exception:
            return nan_greeks

    option_obj = ql.VanillaOption(payoff, ql.EuropeanExercise(expiration_date_obj))
    option_obj.setPricingEngine(ql.AnalyticEuropeanEngine(bsm_process))

    return {'option_price': option_price,
            'implied_vol': implied_vol,
            'delta': option_obj.delta(),
            'vega': option_obj.vega(),
            'theta': option_obj.thetaPerDay(),
            'cal_dte': cal_dte,
            'gamma': option_obj.gamma()}








