

import shared.utils as su
import shared.directory_names_aux as dna

target_size_multiplier_dictionary = {'butterfly_first_bet': 53,
                                     'scv': 26,
                                     'butterfly_final_bet': 88,
                                     'vcs': 132,
                                     'carry_spread': 13,
                                     'pca': 20,
                                     'ocs': 2.9}

def get_strategy_target_size(**kwargs):

    config_output = su.read_text_file(file_name=dna.get_directory_name(ext='c#config') + '/BetSize.txt')
    bet_size = float(config_output[0])

    return bet_size*target_size_multiplier_dictionary[kwargs['strategy_class']]