
import numpy as np

def calculate_volatility_based_outcomes(**kwargs):

    output = kwargs['data_frame_input']
    volatility_field = kwargs['volatility_field']

    if 'calculate_first_row_onlyQ' in kwargs.keys():
        calculate_first_row_onlyQ = kwargs['calculate_first_row_onlyQ']
    else:
        calculate_first_row_onlyQ = False

    output['long_outcome1'] = np.nan
    output['short_outcome1'] = np.nan

    output['long_outcome2'] = np.nan
    output['short_outcome2'] = np.nan

    for i in range(len(output.index) - 1):
        if ~np.isnan(output[volatility_field].iloc[i]):

            entry_price = output['close'].iloc[i]
            volatility = output[volatility_field].iloc[i]
            low_price = entry_price - 1.5 * volatility
            high_price = entry_price + volatility
            for j in range(i + 1, len(output.index)):
                if output['low'].iloc[j] <= low_price:
                    output['long_outcome1'].iloc[i] = -1
                    break
                elif output['high'].iloc[j] >= high_price:
                    output['long_outcome1'].iloc[i] = 1
                    break

            low_price = entry_price - volatility
            high_price = entry_price + 1.5 * volatility

            for j in range(i + 1, len(output.index)):
                if output['high'].iloc[j] >= high_price:
                    output['short_outcome1'].iloc[i] = -1
                    break
                elif output['low'].iloc[j] <= low_price:
                    output['short_outcome1'].iloc[i] = 1
                    break

            low_price = entry_price - 1.5*volatility
            high_price = entry_price + 1.5*volatility
            for j in range(i + 1, len(output.index)):
                if output['low'].iloc[j] <= low_price:
                    output['long_outcome2'].iloc[i] = -1
                    break
                elif output['high'].iloc[j] >= high_price:
                    output['long_outcome2'].iloc[i] = 1
                    break

            for j in range(i + 1, len(output.index)):
                if output['high'].iloc[j] >= high_price:
                    output['short_outcome2'].iloc[i] = -1
                    break
                elif output['low'].iloc[j] <= low_price:
                    output['short_outcome2'].iloc[i] = 1
                    break

        if calculate_first_row_onlyQ:
            break
    return output

