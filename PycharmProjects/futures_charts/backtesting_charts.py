
import backtesting.backtest_futures_butterfly as bfp
import matplotlib.pyplot as plt


def get_butterfly_backtesting_charts(**kwargs):

    summary_frame = bfp.get_individual_trade_path(**kwargs)

    if 'plot_underlyingQ' in kwargs.keys():
        plot_underlyingQ = kwargs['plot_underlyingQ']
    else:
        plot_underlyingQ = False

    fig, ax1 = plt.subplots(figsize=(16, 7))
    line1 = ax1.plot(range(len(summary_frame)), summary_frame['butterfly'], color='black', label='butterfly')
    ax2 = ax1.twinx()

    if plot_underlyingQ :
        line2 = ax2.plot(range(len(summary_frame)), summary_frame['c1'], color='red', label='c1')
        line3 = ax2.plot(range(len(summary_frame)), summary_frame['c2'], color='blue', label='c2')
        line4 = ax2.plot(range(len(summary_frame)), summary_frame['c3'], color='green', label='c3')
        lns = line1 + line2 + line3 + line4
    else:
        line2 = ax2.plot(range(len(summary_frame)), summary_frame['s1'], color='red', label='s1')
        line3 = ax2.plot(range(len(summary_frame)), summary_frame['s2'], color='blue', label='s2')
        lns = line1 + line2 + line3
    labs = [l.get_label() for l in lns]
    ax1.legend(lns, labs, loc=0)
    plt.show()

    return summary_frame

