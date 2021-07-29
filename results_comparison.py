import pandas as pd
import numpy as np
import matching
import df_imports
import time
import datetime
from functools import reduce
from matching import attribute_matching_params

def import_labeled_data():
    labeled_data = "~/Dropbox/Botva/TUM/Master_Thesis/object-identification/labeled_data_final_2.csv"
    df_labeled_data = pd.read_csv(labeled_data, sep=';', error_bad_lines=False)
    df_labeled_data = df_labeled_data.dropna(subset=['is_duplicate'])
    df_labeled_data = df_labeled_data[['id_x', 'id_y', 'is_duplicate']] ##this is correct! (id_x, not doc_1 indexes)

    return df_labeled_data

def add_results_estimation(df_matches_estimation, labeled_positive, labeled_negative):
    matches_with_labels_count = df_matches_estimation['is_duplicate'].count()

    #true_positive = df_matches_estimation['is_duplicate'].loc[(df_matches_estimation['is_duplicate'] == 1) | (df_matches_estimation['is_duplicate'] == 2)].count()
    true_positive = df_matches_estimation['is_duplicate'].loc[df_matches_estimation['is_duplicate'] == 2].count()
    false_positive = df_matches_estimation['is_duplicate'].loc[df_matches_estimation['is_duplicate'] == 0].count()
    #false_positive = df_matches_estimation['is_duplicate'].loc[(df_matches_estimation['is_duplicate'] == 0) | (df_matches_estimation['is_duplicate'] == 1)].count()

    false_negative = labeled_positive - true_positive
    true_negative = labeled_negative - false_positive

    return false_positive, false_negative, true_positive, true_negative

#populate the attributes to the dfs
def create_df_with_attributes(df_matches, df):
    df_matches_full = pd.merge(df_matches, df,  how='left', left_on=['doc_1'], right_on=['id'])
    df_matches_full = df_matches_full.drop(['id'], axis=1)
    df_matches_full = pd.merge(df_matches_full, df,  how='left', left_on=['doc_2'], right_on=['id'])
    df_matches_full = df_matches_full.drop(['id'], axis=1)
    return df_matches_full

def finding_best_methods_for_atts(df, df_results, df_labeled_data, labeled_positive, labeled_negative):
    '''
    iterating through all matching methods for attributes for finding the best methods for each attribute
    '''
    #test_mode
    #dataset_sizes = [100, 1000, 10000]
    dataset_sizes = [1000000]
    matching_attributes = ['name_clean']
    #matching_attributes = ['url_clean']
    #matching_attributes = ['name_clean', 'url_clean']
    #matching_attributes = ['url_clean', 'name_clean']
    #attribute_thresholds = [0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 0.98, 0.99]
    #attribute_thresholds = [0.5, 0.8, 0.9, 0.95, 0.98, 0.99]
    attribute_thresholds = [0]
    #attribute_thresholds = [0.99, 0.5, 1.0]
    #matching_methods = ['minhash', 'fuzzywuzzy', 'exact']
    matching_methods = ['minhash']

    for dataset_size in dataset_sizes:
        for matching_attribute in matching_attributes:
            for matching_method in matching_methods:
                hash_types = ['shingle']
                #hash_types = ['token']
                #hash_types = ['token', 'shingle']
                bands_numbers = [5]
                signature_sizes = [50]
                #hash_weights = ['weighted']
                hash_weights = ['normal']
                #hash_weights = ['weighted', 'normal', 'frequency']

                if matching_method != 'minhash':
                    hash_types = [0]
                    hash_weights = [0]
                    shingle_sizes = [0]
                    bands_numbers = [0]
                    signature_sizes = [0]
                for hash_type in hash_types:
                    #shingle_sizes = [2, 3, 4]
                    shingle_sizes = [2]

                    if hash_type == 'token':
                        shingle_sizes = [0]
                    for hash_weight in hash_weights:
                        for shingle_size in shingle_sizes:
                            for bands_number in bands_numbers:
                                for signature_size in signature_sizes:
                                    attribute = attribute_matching_params(matching_attribute, matching_method, hash_type, shingle_size, hash_weight, bands_number, signature_size)

                                    start_time_all = time.time()
                                    print('')
                                    print('Started overall matching for the dataset ({} size):'.format(dataset_size))
                                    print(attribute)
                                    df_all_matches, signatures_creation_time, buckets_creation_time, finding_matches_time = matching.main(df, dataset_size, attribute)
                                    df_all_matches['match_score'] = df_all_matches['match_score_{}'.format(attribute.matching_attribute)]

                                    all_time = round(time.time() - start_time_all, 6)
                                    print('------------------------------------------------')
                                    print("The whole matching algorithm took (for the {} dataset size) --- {} seconds ---".format(dataset_size, all_time))
                                    print('------------------------------------------------')

                                    print("Started adding matches attributes...")
                                    df_all_matches = create_df_with_attributes(df_all_matches, df)
                                    print("...Added matches attributes")

                                    print("Started joining the result with the labeled data...")
                                    df_matches_estimation = pd.merge(df_all_matches, df_labeled_data, how='left', left_on=['doc_1', 'doc_2'], right_on=['id_x', 'id_y'])
                                    df_matches_estimation.to_csv("df_matches_full_{}_{}.csv".format(attribute.matching_attribute, str(datetime.datetime.now())))
                                    print("...Joint the result with the labeled data")

                                    df_labeled_data_not_in_df = pd.merge(df_labeled_data, df_all_matches, how='left', left_on=['id_x', 'id_y'], right_on=['doc_1', 'doc_2'])
                                    df_labeled_data_not_in_df = df_labeled_data_not_in_df[df_labeled_data_not_in_df['doc_1'].isnull()]
                                    df_labeled_data_not_in_df = df_labeled_data_not_in_df[['id_x', 'id_y', 'is_duplicate']]
                                    df_labeled_data_not_in_df.to_csv("df_labeled_data_not_in_df_{}_{}.csv".format(attribute.matching_attribute, str(datetime.datetime.now())))

                                    df_not_labeled = pd.merge(df_labeled_data_not_in_df, df, how='left', left_on=['id_x'], right_on=['id'])
                                    df_not_labeled = pd.merge(df_not_labeled, df, how='left', left_on=['id_y'], right_on=['id'])
                                    df_not_labeled.to_csv("df_not_labeled_{}_{}.csv".format(attribute.matching_attribute, str(datetime.datetime.now())))

                                    for attribute_threshold in attribute_thresholds:
                                        start_time = time.time()
                                        print("Started calculating results accuracy...")
                                        df_matches_estimation_for_thresholds = df_matches_estimation[df_matches_estimation.match_score > attribute_threshold]
                                        false_positive, false_negative, true_positive, true_negative = add_results_estimation(df_matches_estimation_for_thresholds, labeled_positive, labeled_negative)
                                        results_calculation = round(time.time() - start_time, 6)
                                        print("Calculating results accuracy took {} sec".format(results_calculation))

                                        experiment = {'dataset_size': dataset_size,
                                                      'matching_attribute': attribute.matching_attribute,
                                                      'attribute_weight': 1,
                                                      'attribute_threshold': attribute_threshold,
                                                      'matching_method': attribute.matching_method,
                                                      'hash_type': attribute.hash_type,
                                                      'shingles_size': attribute.shingle_size,
                                                      'hash_weight': attribute.hash_weight,
                                                      'signature_size': attribute.signature_size,
                                                      'bands_number': attribute.bands_number,
                                                      'total_time': all_time,
                                                      'signatures_creation_time': signatures_creation_time,
                                                      'buckets_creation_time': buckets_creation_time,
                                                      'finding_matches_time': finding_matches_time,
                                                      'number_of_matches': len(df_matches_estimation_for_thresholds),
                                                      'false_pos': false_positive,
                                                      'false_neg': false_negative,
                                                      'true_pos': true_positive,
                                                      'true_neg': true_negative,
                                                      'false_pos_rate': false_positive / (false_positive + true_negative),
                                                      'false_neg_rate': false_negative / (false_negative + true_positive),
                                                      'true_pos_rate': true_positive / (true_positive + false_negative),
                                                      'true_neg_rate': true_negative / (true_negative + false_positive)
                                                      }

                                        df_results = df_results.append(experiment, ignore_index=True)
                                        print('------------------------------------------------')
                                        print('------------------------------------------------')
                                        del df_matches_estimation_for_thresholds
            df_results.to_csv("df_results_{}_{}.csv".format(matching_attribute, str(datetime.datetime.now())))

    return df_results

def finding_best_combinations(df, df_results, df_labeled_data, labeled_positive, labeled_negative):
    #here we should use the top matching combindations from the finding_best_methods_for_atts function
    dataset_size = 100000
    threshold = 0
    attribute_weights = [1, 2, 3]

    matching_params = [
        attribute_matching_params(matching_attribute='name_clean', matching_method='minhash', hash_type='token',
                                  hash_weight='weighted', attribute_threshold=0.5),
        attribute_matching_params(matching_attribute='url_clean', matching_method='minhash', hash_type='shingle', shingle_size=3,
                                  hash_weight='frequency', attribute_threshold=0.5)]
    matches_dfs = []

    start_time_all = time.time()
    print('Started overall matching for the dataset ({} size):'.format(dataset_size))
    for attribute in matching_params:
        df_matches_full, signatures_creation_time, buckets_creation_time, finding_matches_time = matching.main(df, attribute)
        matches_dfs.append(df_matches_full)
    print('------------------------------------------------')
    all_time = round(time.time() - start_time_all, 6)
    print("The whole matching algorithm took (for the {} dataset size) --- {} seconds ---".format(dataset_size, all_time))
    print('------------------------------------------------')

    df_all_matches = reduce(lambda df1, df2: pd.merge(df1, df2, how='outer', on=['doc_1', 'doc_2']), matches_dfs)

    print("Started adding matches attributes...")
    df_all_matches = create_df_with_attributes(df_all_matches, df)

    df_all_matches['match_score'] = 0

    for att1_weight in attribute_weights:
        for att2_weight in attribute_weights:
            if (att1_weight != att2_weight) or (att1_weight == 1 and att2_weight == 1):
                for attribute in matching_params:
                    try:
                        df_all_matches['match_score_{}'.format(attribute.matching_attribute)] = df_all_matches['match_score_{}'.format(attribute.matching_attribute)].fillna(0)*att1_weight
                        df_all_matches['match_score'] = df_all_matches['match_score'] + df_all_matches['match_score_{}'.format(attribute.matching_attribute)]*att2_weight
                    except:
                        print('No matches for the {} attribute'.format(attribute.matching_attribute))

                df_all_matches = df_all_matches.sort_values(by='match_score', ascending=False)

                df_matches_estimation = pd.merge(df_all_matches, df_labeled_data, how='left', left_on=['doc_1', 'doc_2'],
                                                 right_on=['id_x', 'id_y'])
                df_matches_estimation.to_csv(
                    "df_matches_full_{}_{}.csv".format(len(df), str(datetime.datetime.now())))

                false_positive, false_negative, true_positive, true_negative = add_results_estimation(df_matches_estimation[df_matches_estimation.match_score > threshold], labeled_positive, labeled_negative)

                # experiment = add_experiments_params(matching_params, dataset_size, df_all_matches, all_time, 0.5, false_positive, true_negative, true_positive, false_negative)

                experiment = {'dataset_size': dataset_size,
                              'matching_attribute': str(
                                  reduce(lambda x, y: x.matching_attribute + "-" + y.matching_attribute, matching_params)),
                              'attribute_weight': str(att1_weight) + "-" + str(att2_weight),
                              'attribute_threshold': str(
                                  reduce(lambda x, y: str(x.attribute_threshold) + "-" + str(y.attribute_threshold),
                                         matching_params)),
                              'matching_method': str(
                                  reduce(lambda x, y: x.matching_method + "-" + y.matching_method, matching_params)),
                              'hash_type': str(reduce(lambda x, y: x.hash_type + "-" + y.hash_type, matching_params)),
                              'shingles_size': str(
                                  reduce(lambda x, y: str(x.shingle_size) + "-" + str(y.shingle_size), matching_params)),
                              'hash_weight': str(
                                  reduce(lambda x, y: str(x.hash_weight) + "-" + str(y.hash_weight), matching_params)),
                              'signature_size': str(
                                  reduce(lambda x, y: str(x.signature_size) + "-" + str(y.signature_size), matching_params)),
                              'bands_number': str(
                                  reduce(lambda x, y: str(x.bands_number) + "-" + str(y.bands_number), matching_params)),
                              'total_time': all_time,
                              'signatures_creation_time': signatures_creation_time,
                              'buckets_creation_time': buckets_creation_time,
                              'finding_matches_time': finding_matches_time,
                              'number_of_matches': len(df_all_matches),
                              'false_pos': false_positive,
                              'false_neg': false_negative,
                              'true_pos': true_positive,
                              'true_neg': true_negative,
                              'false_pos_rate': false_positive / (false_positive + true_negative),
                              'false_neg_rate': false_negative / (false_negative + true_positive),
                              'true_pos_rate': true_positive / (true_positive + false_negative),
                              'true_neg_rate': true_negative / (true_negative + false_positive)
                              }

                df_results = df_results.append(experiment, ignore_index=True)
                print('------------------------------------------------')
                print('------------------------------------------------')

    return df_results

if __name__ == "__main__":
    dataset_size = 200000

    df_labeled_data = import_labeled_data()
    b = df_labeled_data.loc[df_labeled_data['id_x'] < df_labeled_data['id_y']] #should be fixed later
    c = df_labeled_data.loc[df_labeled_data['id_x'] > df_labeled_data['id_y']]
    c = c.rename(columns={'id_x': 'id_y', 'id_y': 'id_x'}, inplace=False)
    c = c[['id_x', 'id_y', 'is_duplicate']]
    df_labeled_data = b.append(c)

    start_time = time.time()
    print('------------------------------------------------')
    print('Started downloading datasets')
    df = df_imports.df_import(dataset_size)
    print("Importing datasets took --- %s seconds ---" % (time.time() - start_time))

    number_of_tries = 1 #how many random datasets should be created

    dataset_size = 100000
    df = df.dropna(subset=['url_clean', 'name_clean'])

    for a in range(number_of_tries):
        try:
            df = df.sample(n=dataset_size)
        except:
            continue

        df_labeled_data_in_df = pd.merge(df_labeled_data, df,  how='left', left_on=['id_x'], right_on=['id'])
        df_labeled_data_in_df = df_labeled_data_in_df.dropna(subset=['id'])
        df_labeled_data_in_df = df_labeled_data_in_df[['id_x', 'id_y', 'is_duplicate']]
        df_labeled_data_in_df = pd.merge(df_labeled_data_in_df, df,  how='left', left_on=['id_y'], right_on=['id'])
        df_labeled_data_in_df = df_labeled_data_in_df.dropna(subset=['id'])
        df_labeled_data_in_df = df_labeled_data_in_df[['id_x', 'id_y', 'is_duplicate']]

        #labeled_positive = df_labeled_data_in_df['is_duplicate'].loc[(df_labeled_data_in_df['is_duplicate'] == 1) | (df_labeled_data_in_df['is_duplicate'] == 2)].count()
        labeled_positive = df_labeled_data_in_df['is_duplicate'].loc[df_labeled_data_in_df['is_duplicate'] == 2].count()
        labeled_negative = df_labeled_data_in_df['is_duplicate'].loc[df_labeled_data_in_df['is_duplicate'] == 0].count()
        #labeled_negative = df_labeled_data_in_df['is_duplicate'].loc[(df_labeled_data_in_df['is_duplicate'] == 0) | (df_labeled_data_in_df['is_duplicate'] == 1)].count()
        labeled_matches_count = df_labeled_data_in_df['is_duplicate'].count()

        column_names = ['dataset_size', 'matching_attribute', 'attribute_weight', 'attribute_threshold', 'matching_method', 'hash_type', 'shingles_size',
                        'hash_weight', 'signature_size', 'bands_number', 'total_time', 'signatures_creation_time', 'buckets_creation_time', 'finding_matches_time', 'number_of_matches', 'false_pos', 'false_neg', 'true_pos', 'true_neg', 'false_pos_rate','false_neg_rate', 'true_pos_rate', 'true_neg_rate']
        df_results = pd.DataFrame(columns=column_names)

        df_results = finding_best_methods_for_atts(df, df_results, df_labeled_data_in_df, labeled_positive, labeled_negative)
        df_results.to_csv("df_results_final_{}.csv".format(str(datetime.datetime.now())))

        #df_results = finding_best_combinations(df, df_results, df_labeled_data, labeled_positive, labeled_negative)

print('end')
