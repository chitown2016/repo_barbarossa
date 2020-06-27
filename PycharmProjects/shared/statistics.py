__author__ = 'kocat_000'

import warnings
with warnings.catch_warnings():
    warnings.filterwarnings("ignore",category=FutureWarning)
    import statsmodels.api

import numpy as np
import statsmodels.api as sm


def get_stdev(**kwargs):

    x = kwargs['x']

    if 'clean_num_obs' in kwargs.keys():
        clean_num_obs = kwargs['clean_num_obs']
    else:
        clean_num_obs = round(3*len(x)/4)

    nan_indx = np.isnan(x)

    if len(x)-nan_indx.sum() < clean_num_obs:
        return float('NaN')

    return np.nanstd(x)

def get_mean(**kwargs):

    x = kwargs['x']

    if 'clean_num_obs' in kwargs.keys():
        clean_num_obs = kwargs['clean_num_obs']
    else:
        clean_num_obs = round(3*len(x)/4)

    nan_indx = np.isnan(x)

    if len(x)-nan_indx.sum() < clean_num_obs:
        return float('NaN')

    return np.nanmean(x)

def get_quantile_from_number(quantile_input):

    x = quantile_input['x']
    y = quantile_input['y']

    if np.isnan(x):
        return np.NAN

    if 'clean_num_obs' in quantile_input.keys():
        clean_num_obs = quantile_input['clean_num_obs']
    else:
        clean_num_obs = round(3*len(y)/4)

    nan_indx = np.isnan(y)

    if len(y)-nan_indx.sum() < clean_num_obs:
        return float('NaN')

    clean_y = [y[i] for i in range(len(y)) if not nan_indx[i]]

    w = np.percentile(clean_y, range(1, 100))

    return np.argmin(abs(x-w))+1


def get_number_from_quantile(**kwargs):

     y = kwargs['y']

     quantile_list = kwargs['quantile_list']

     if len(y)==0:
         return [float('NaN')] * len(quantile_list)

     if 'clean_num_obs' in kwargs.keys():
        clean_num_obs = kwargs['clean_num_obs']
     else:
        clean_num_obs = round(3*len(y)/4)

     nan_indx = np.isnan(y)

     if len(y)-nan_indx.sum() < clean_num_obs:
         return [float('NaN')]*len(quantile_list)

     clean_y = [y[i] for i in range(len(y)) if not nan_indx[i]]

     return np.percentile(clean_y, quantile_list)

def get_regression_results(regression_input):

    y = regression_input['y']
    x = regression_input['x']

    if 'clean_num_obs' in regression_input.keys():
        clean_num_obs = regression_input['clean_num_obs']
    else:
        clean_num_obs = round(3*len(y)/4)

    finite_indx_x = np.isfinite(x)
    finite_indx_y = np.isfinite(y)

    clean_y = [y[i] for i in range(len(y)) if finite_indx_x[i] and finite_indx_y[i]]
    clean_x = [x[i] for i in range(len(y)) if finite_indx_x[i] and finite_indx_y[i]]

    if len(clean_x) < clean_num_obs:
        nan_matrix = np.empty((2,2))
        nan_matrix[:] = np.NAN
        return {'alpha': np.NAN, 'beta': np.NAN, 'conf_int': nan_matrix ,'rsquared': np.NAN, 'residualstd': np.NAN,'zscore': np.NAN}

    x_current = clean_x[-1]
    y_current = clean_y[-1]

    if 'x_current' in regression_input.keys():
        x_current = regression_input['x_current']

    if 'y_current' in regression_input.keys():
        y_current = regression_input['y_current']

    results = sm.OLS(clean_y, sm.add_constant(clean_x), hasconst=True).fit()
    parameters = results.params

    if len(parameters)>1:
        zscore = (y_current-parameters[0]-parameters[1]*x_current)/np.sqrt(results.mse_resid)
        alpha = parameters[0]
        beta = parameters[1]
    else:
        zscore = np.nan
        alpha = parameters[0]
        beta = np.nan

    return {'alpha': alpha, 'beta': beta,
            'conf_int': results.conf_int(),
            'rsquared': 100*results.rsquared,
            'residualstd': np.sqrt(results.mse_resid),
            'zscore': zscore}

def get_pca(**kwargs):

    from sklearn.decomposition import PCA

    data_input = kwargs['data_input']
    n_components = kwargs['n_components']

    pca = PCA(n_components=n_components)
    scores = pca.fit_transform(data_input)
    inverse_data = pca.inverse_transform(scores)

    return {'scores': scores, 'loadings': pca.components_, 'model_fit' : inverse_data}









