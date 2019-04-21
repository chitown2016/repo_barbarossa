
import get_price.get_futures_price as gfp
import shared.calendar_utilities as cu


def get_candlestick_patterns(**kwargs):

    ticker = kwargs['ticker']
    date_to = kwargs['date_to']

    output_dictionary = {}
    output_dictionary['bearish_big_shadowQ'] = 0
    output_dictionary['bullish_big_shadowQ'] = 0
    output['success'] = 0


    data_input = gfp.get_futures_price_4ticker(ticker=ticker, date_to=date_to)


    data_input.rename(columns={'open_price': 'open', 'high_price': 'high', 'low_price': 'low', 'close_price': 'close'}, inplace=True)

    high1 = data_input['high'].iloc[-1]
    low1 = data_input['low'].iloc[-1]
    open1 = data_input['open'].iloc[-1]
    close1 = data_input['close'].iloc[-1]

    high2 = data_input['high'].iloc[-2]
    low2 = data_input['low'].iloc[-2]
    open2 = data_input['open'].iloc[-2]
    close2 = data_input['close'].iloc[-2]

    data_input['range'] = data_input['high']-data_input['low']

    return data_input

    # checking for big shadow

    pattern_list = []

    if (high1>high2) and (low1<low2) and len(data_input.index)>=10 and (data_input['range'].iloc[-10:].max() <= data_input['range'].iloc[-1]):
        if(close2>open2) and (close1<0.75*low1+0.25*high1) and (data_input['high'].iloc[-2]-data_input['high'].iloc[-9:-2].max()>0):

            pattern_list.append('bearishBigShadow')

        if(close2<open2) and (close1>0.25*low1+0.75*high1) and (data_input['low'].iloc[-2]-data_input['low'].iloc[-9:-2].min()<0):
            pattern_list.append('bullishBigShadow')








