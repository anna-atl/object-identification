import pandas as pd
import numpy as np
import main_joint
import time
import datetime

class normal_matching_params:
    def __init__(self, matching_attribute, matching_method, attribute_weight=1):
        self.matching_attribute = matching_attribute
        self.matching_method = matching_method#jaccard etc?
        self.attribute_weight = attribute_weight

class minhash_matching_params:
    def __init__(self, matching_attribute, hash_type, hash_weight, matching_method='minhash', attribute_weight=1, shingle_size=2, bands_number=5, signature_size=50):
        self.matching_attribute = matching_attribute
        self.matching_method = matching_method
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
    return df_labeled_data

def add_results_estimation(df_all_matches, df_labeled_data):
    labeled_positive = df_labeled_data['is_duplicate'].loc[df_labeled_data['is_duplicate'] == 2].count()
    labeled_negative = df_labeled_data['is_duplicate'].loc[
        (df_labeled_data['is_duplicate'] == 0) | (df_labeled_data['is_duplicate'] == 1)].count()
    labeled_matches_count = df_labeled_data['is_duplicate'].count()

    df_matches_estimation = pd.merge(df_all_matches, df_labeled_data, how='left',
                                     left_on=['doc_1', 'doc_2'], right_on=['id_x', 'id_y'])
    #df_matches_estimation = df_matches_estimation.drop(['id_x_x', 'id_x_y', 'id_y_x', 'id_y_y'], axis=1)

    # test_mode
    # df_matches_estimation = df_all_matches
    # df_matches_estimation['is_duplicate'] = 0

    matches_with_labels_count = df_matches_estimation['is_duplicate'].count()
    true_positive = df_matches_estimation['is_duplicate'].loc[df_matches_estimation['is_duplicate'] == 2].count()
    false_positive = df_matches_estimation['is_duplicate'].loc[
        (df_matches_estimation['is_duplicate'] == 0) | (df_matches_estimation['is_duplicate'] == 1)].count()

    false_negative = labeled_positive - true_positive
    true_negative = labeled_negative - false_positive

    return false_positive, false_negative, true_positive, true_negative

def all_options:
    '''
    ALL OPTIONS MODE

    '''
    #test_mode
    #dataset_sizes = [100, 1000, 10000]
    dataset_sizes = [1000000]
    matching_attributes = ['name_clean']
    #matching_attributes = ['name_clean', 'url_clean']
    attribute_weights = [1]
    thresholds = [0.5]
    #thresholds = [0.5, 0.7, 0.9]
    #matching_methods = ['minhash', 'fuzzy', 'exact']
    matching_methods = ['minhash']
    #hash_types = ['token', 'shingle']
    hash_types = ['token']
    hash_weights = ['weighted']
    #hash_weights = ['normal', 'frequency', 'weighted']
    #shingle_sizes = [2, 3, 4]
    shingle_sizes = [2, 3]

    column_names = ['dataset_size', 'matching_attribute', 'attribute_weights', 'threshold', 'matching_method', 'hash_type', 'shingles_size',
                    'hash_weight', 'signature_size', 'bands_number',
                    'total_time', 'number_of_matches', 'minhash_time', 'sign_creation_time', 'false_pos_rate',
                    'false_neg_rate', 'true_pos_rate', 'true_neg_rate'
                    ]
    df_results = pd.DataFrame(columns=column_names)
    for dataset_size in dataset_sizes:
        for matching_attributes in multiple_matching_attributes:
            for attribute_weight in attribute_weights:
                for threshold in thresholds:
                    for matching_method in matching_methods:
                        if matching_params[0].matching_method == 'minhash':
                            for hash_type in hash_types:
                                for hash_weight in hash_weights:
                                    if hash_type == 'shingle':
                                        for k in shingle_sizes:
                                            matching_params = [minhash_matching_params(matching_attributes, hash_type, hash_weight, shingle_size=k)]
                                            df_all_matches, all_time = main_joint.main(dataset_size, threshold, matching_method, matching_params)
                                            false_positive, false_negative, true_positive, true_negative = add_results_estimation(df_all_matches, df_labeled_data)
                                            experiment = {'dataset_size': dataset_size,
                                                          'matching_attribute': matching_params[0].matching_attribute,# [0]should be fixed
                                                          'threshold': threshold,
                                                          'matching_method': matching_params[0].matching_method,
                                                          'hash_type': matching_params[0].hash_type,
                                                          'shingles_size': matching_params[0].shingle_size,
                                                          'hash_weight': matching_params[0].hash_weight,
                                                          'signature_size': matching_params[0].signature_size,
                                                          'bands_number': matching_params[0].bands_number,
                                                          'total_time': all_time,
                                                          'number_of_matches': len(df_all_matches),# 'minhash_time', 'sign_creation_time',
                                                          'false_pos_rate': false_positive / (
                                                                      false_positive + true_negative),
                                                          'false_neg_rate': false_negative / (
                                                                      false_negative + true_positive),
                                                          'true_pos_rate': true_positive / (true_positive + false_negative),
                                                          'true_neg_rate': true_negative / (true_negative + false_positive)
                                                          }
                                            df_results = df_results.append(experiment, ignore_index=True)
                                    elif hash_type == 'token':
                                        matching_params = [minhash_matching_params(matching_attribute, hash_type, hash_weight)]
                                        df_all_matches, all_time = main_joint.main(dataset_size, threshold, matching_method, matching_params)

                                        false_positive, false_negative, true_positive, true_negative = add_results_estimation(
                                            df_all_matches, df_labeled_data)
                                        experiment = {'dataset_size': dataset_size,
                                                      'matching_attribute': matching_params[0].matching_attribute,# [0]should be fixed
                                                      'threshold': threshold,
                                                      'matching_method': matching_params[0].matching_method,
                                                      'hash_type': matching_params[0].hash_type,
                                                      'shingles_size': matching_params[0].shingle_size,
                                                      'hash_weight': matching_params[0].hash_weight,
                                                      'signature_size': matching_params[0].signature_size,
                                                      'bands_number': matching_params[0].bands_number,
                                                      'total_time': all_time,
                                                      'number_of_matches': len(df_all_matches),# 'minhash_time', 'sign_creation_time',
                                                      'false_pos_rate': false_positive / (
                                                              false_positive + true_negative),
                                                      'false_neg_rate': false_negative / (
                                                              false_negative + true_positive),
                                                      'true_pos_rate': true_positive / (true_positive + false_negative),
                                                      'true_neg_rate': true_negative / (true_negative + false_positive)
                                                      }
                                        df_results = df_results.append(experiment, ignore_index=True)
                                #else: continue
    df_results.to_csv("df_results_{}.csv".format(str(datetime.datetime.now())))
    return df_results

def results():
    ps1 = [minhash_matching_params('name_clean', 1, ['tokens'], 'weighted')
        , minhash_matching_params('url_clean', 3, ['shingles', 3], 'normal')
        , normal_matching_params('country_clean', 0.3, 'exact')
        , normal_matching_params('zip', 0.6, 'exact')
        , normal_matching_params('city_clean', 0.4, 'exact')
        , normal_matching_params('street_clean', 0.8, 'exact')]

    dataset_sizes = [100000]
    multiple_matching_attributes = [['name_clean'], ['name_clean', 'url_clean']]
    attribute_weights = [1, 2, 3]
    thresholds = [0.5, 0.7, 0.9]
    matching_methods = ['minhash', 'fuzzy', 'exact']
    #matching_methods = ['minhash']
    #hash_types = ['token', 'shingle']
    hash_types = ['token', 'shingle']
    hash_weights = ['weighted', 'normal']
    #hash_weights = ['normal', 'frequency', 'weighted']
    #shingle_sizes = [2, 3, 4]
    shingle_sizes = [2, 3]

if __name__ == "__main__":

    matches_dfs = []

    df_labeled_data = import_labeled_data()



print('end')
