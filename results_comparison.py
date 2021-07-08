import pandas as pd
import numpy as np
import matching
import df_imports
import time
import datetime
from functools import reduce

class attribute_matching_params:
    def __init__(self, matching_attribute, matching_method, hash_type='none', hash_weight='none', shingle_size=0, bands_number=5, signature_size=50, attribute_threshold=0, attribute_weight=1):
        self.matching_attribute = matching_attribute
        self.matching_method = matching_method
        self.attribute_threshold = attribute_threshold
        self.attribute_weight = attribute_weight
        self.hash_type = hash_type# token, shingle
        self.shingle_size = shingle_size# 1,2
        self.hash_weight = hash_weight# 'normal', 'frequency', 'weighted'
        self.bands_number = bands_number
        self.signature_size = signature_size

def import_labeled_data():
    labeled_data = "~/Dropbox/Botva/TUM/Master_Thesis/object-identification/labeled_data_2.csv"
    df_labeled_data = pd.read_csv(labeled_data, sep=';', error_bad_lines=False)
    df_labeled_data = df_labeled_data.dropna(subset=['is_duplicate'])
    df_labeled_data = df_labeled_data[['id_x', 'id_y', 'is_duplicate']]

    labeled_positive = df_labeled_data['is_duplicate'].loc[df_labeled_data['is_duplicate'] == 2].count()
    labeled_negative = df_labeled_data['is_duplicate'].loc[(df_labeled_data['is_duplicate'] == 0) | (df_labeled_data['is_duplicate'] == 1)].count()
    labeled_matches_count = df_labeled_data['is_duplicate'].count()

    return df_labeled_data, labeled_positive, labeled_negative

def add_results_estimation(df_matches_estimation, labeled_positive, labeled_negative):
    matches_with_labels_count = df_matches_estimation['is_duplicate'].count()
    true_positive = df_matches_estimation['is_duplicate'].loc[df_matches_estimation['is_duplicate'] == 2].count()
    false_positive = df_matches_estimation['is_duplicate'].loc[
        (df_matches_estimation['is_duplicate'] == 0) | (df_matches_estimation['is_duplicate'] == 1)].count()

    false_negative = labeled_positive - true_positive
    true_negative = labeled_negative - false_positive

    return false_positive, false_negative, true_positive, true_negative

def add_experiments_params(matching_params, dataset_size, df_all_matches, all_time, attribute_threshold, false_positive, true_negative, true_positive, false_negative):
    experiment = {'dataset_size': dataset_size,
                  'matching_attribute': matching_params.matching_attribute,  # [0]should be fixed
                  'attribute_weight': matching_params.attribute_weight,
                  'attribute_threshold': attribute_threshold,
                  'matching_method': matching_params.matching_method,
                  'hash_type': matching_params.hash_type,
                  'shingles_size': matching_params.shingle_size,
                  'hash_weight': matching_params.hash_weight,
                  'signature_size': matching_params.signature_size,
                  'bands_number': matching_params.bands_number,
                  'total_time': all_time,
                  'number_of_matches': len(df_all_matches),  # 'minhash_time', 'sign_creation_time',
                  'false_pos_rate': false_positive / (
                          false_positive + true_negative),
                  'false_neg_rate': false_negative / (
                          false_negative + true_positive),
                  'true_pos_rate': true_positive / (true_positive + false_negative),
                  'true_neg_rate': true_negative / (true_negative + false_positive)
                  }
    return experiment

def experiments_performance():
    return 0

def finding_best_methods_for_atts(df, df_results, df_labeled_data, labeled_positive, labeled_negative):
    '''
    iterating through all matching methods for attributes for finding the best methods for each attribute
    '''
    #test_mode
    dataset_sizes = [100000]
    #dataset_sizes = [100, 1000, 10000]
    #matching_attributes = ['name_clean']
    matching_attributes = ['url_clean', 'name_clean']
    attribute_thresholds = [0.5]
    #matching_methods = ['minhash', 'fuzzy', 'exact']
    matching_methods = ['minhash']
    #hash_types = ['token']
    hash_types = ['token', 'shingle']
    hash_weights = ['weighted']
    #hash_weights = ['normal', 'frequency', 'weighted']
    #shingle_sizes = [2, 3, 4]
    shingle_sizes = [2, 3, 4]
    bands_numbers = [5]
    signature_sizes = [50]

    for dataset_size in dataset_sizes:
        for matching_attribute in matching_attributes:
            for matching_method in matching_methods:
                if matching_method != 'minhash':
                    hash_types = [0]
                    hash_weights = [0]
                    shingle_sizes = [0]
                    bands_numbers = [0]
                    signature_sizes = [0]
                else:
                    for hash_type in hash_types:
                        if hash_type == 'token':
                            shingle_sizes = [0]
                for hash_type in hash_types:
                    for hash_weight in hash_weights:
                        for shingle_size in shingle_sizes:
                            for bands_number in bands_numbers:
                                for signature_size in signature_sizes:
                                    matching_params = [attribute_matching_params(matching_attribute, matching_method, hash_type, hash_weight, shingle_size, bands_number, signature_size)]

                                    df_all_matches, all_time = matching.main(df, dataset_size, matching_params)

                                    df_matches_estimation = pd.merge(df_all_matches, df_labeled_data, how='left', left_on=['doc_1', 'doc_2'], right_on=['id_x', 'id_y'])
                                    for attribute_threshold in attribute_thresholds:
                                        false_positive, false_negative, true_positive, true_negative = add_results_estimation(
                                                    df_matches_estimation[df_matches_estimation.match_score > attribute_threshold], labeled_positive, labeled_negative)

                                        experiment = add_experiments_params(matching_params[0], dataset_size, df_all_matches,
                                                                       all_time, attribute_threshold, false_positive,
                                                                       true_negative, true_positive, false_negative)

                                        df_results = df_results.append(experiment, ignore_index=True)

    df_results.to_csv("df_results_{}.csv".format(str(datetime.datetime.now())))
    return df_results

def finding_best_combinations(df, df_results, df_labeled_data, labeled_positive, labeled_negative):
    #here we should use the top matching combindations from the finding_best_methods_for_atts function
    dataset_size = 100000
    threshold = 0.5
    attribute_weights = [1, 2, 3]

    for att_weight in attribute_weights:
        matching_params = [attribute_matching_params(matching_attribute='name_clean', matching_method='minhash', hash_type='token', hash_weight='weighted', attribute_threshold=0.5, attribute_weight=att_weight),
            attribute_matching_params(matching_attribute='url_clean', matching_method='minhash', hash_type='shingle', hash_weight='weighted', shingle_size=3, attribute_threshold=0.5, attribute_weight=att_weight)]

        df_all_matches, all_time = matching.main(df, dataset_size, matching_params)
        df_matches_estimation = pd.merge(df_all_matches, df_labeled_data, how='left', left_on=['doc_1', 'doc_2'],
                                         right_on=['id_x', 'id_y'])
        false_positive, false_negative, true_positive, true_negative = add_results_estimation(df_matches_estimation[df_matches_estimation.match_score > threshold], labeled_positive, labeled_negative)
        
        #experiment = add_experiments_params(matching_params, dataset_size, df_all_matches, all_time, 0.5, false_positive, true_negative, true_positive, false_negative)
                                    
        experiment = {'dataset_size': dataset_size,
                      'matching_attribute': str(reduce(lambda x, y: x.matching_attribute+"-"+y.matching_attribute, matching_params)),
                      'attribute_weight': str(reduce(lambda x, y: str(x.attribute_weight)+"-"+str(y.attribute_weight), matching_params)),
                      'attribute_threshold': str(reduce(lambda x, y: str(x.attribute_threshold)+"-"+str(y.attribute_threshold), matching_params)),
                      'matching_method': str(reduce(lambda x, y: x.matching_method+"-"+y.matching_method, matching_params)),
                      'hash_type': str(reduce(lambda x, y: x.hash_type+"-"+y.hash_type, matching_params)),
                      'shingles_size': str(reduce(lambda x, y: str(x.shingle_size)+"-"+str(y.shingle_size), matching_params)),
                      'hash_weight': str(reduce(lambda x, y: str(x.hash_weight)+"-"+str(y.hash_weight), matching_params)),
                      'signature_size': str(reduce(lambda x, y: str(x.signature_size)+"-"+str(y.signature_size), matching_params)),
                      'bands_number': str(reduce(lambda x, y: str(x.bands_number)+"-"+str(y.bands_number), matching_params)),
                      'total_time': all_time,
                      'number_of_matches': len(df_all_matches),
                      'false_pos_rate': false_positive / (
                              false_positive + true_negative),
                      'false_neg_rate': false_negative / (
                              false_negative + true_positive),
                      'true_pos_rate': true_positive / (true_positive + false_negative),
                      'true_neg_rate': true_negative / (true_negative + false_positive)
                      }

        df_results = df_results.append(experiment, ignore_index=True)
    return df_results

if __name__ == "__main__":
    dataset_size = 1000
    start_time = time.time()
    print('------------------------------------------------')
    print('Started downloading datasets')
    df = df_imports.df_import(dataset_size)
    print("Importing datasets took --- %s seconds ---" % (time.time() - start_time))
    print('------------------------------------------------')
    print('')

    column_names = ['dataset_size', 'matching_attribute', 'attribute_weight', 'attribute_threshold', 'matching_method', 'hash_type', 'shingles_size',
                    'hash_weight', 'signature_size', 'bands_number', 'total_time', 'number_of_matches', 'minhash_time', 'sign_creation_time', 'false_pos_rate',
                    'false_neg_rate', 'true_pos_rate', 'true_neg_rate']
    df_results = pd.DataFrame(columns=column_names)

    df_labeled_data, labeled_positive, labeled_negative = import_labeled_data()

    df_results = finding_best_combinations(df, df_results, df_labeled_data, labeled_positive, labeled_negative)
    df_results = finding_best_methods_for_atts(df, df_results, df_labeled_data, labeled_positive, labeled_negative)

print('end')
