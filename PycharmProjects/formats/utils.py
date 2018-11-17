
xls_file_names = {'futures_butterfly': 'butterflies',
                  'curve_pca': 'curve_pca',
                  'vcs': 'vcs',
                  'scv': 'scv',
                  'ifs': 'ifs',
                  'ocs': 'ocs'}


def get_xls_file_name(x):
    return xls_file_names.get(x, x)