import pandas as pd
import numpy as np
import time
import datetime
import string

def add_results_estimation(df_matches_estimation, labeled_positive, labeled_negative):
    matches_with_labels_count = df_matches_estimation['is_duplicate'].count()

    #true_positive = df_matches_estimation['is_duplicate'].loc[(df_matches_estimation['is_duplicate'] == 1) | (df_matches_estimation['is_duplicate'] == 2)].count()
    true_positive = df_matches_estimation['is_duplicate'].loc[df_matches_estimation['is_duplicate'] == 2].count()
    false_positive = df_matches_estimation['is_duplicate'].loc[df_matches_estimation['is_duplicate'] == 0].count()
    #false_positive = df_matches_estimation['is_duplicate'].loc[(df_matches_estimation['is_duplicate'] == 0) | (df_matches_estimation['is_duplicate'] == 1)].count()

    false_negative = labeled_positive - true_positive
    true_negative = labeled_negative - false_positive

    return false_positive, false_negative, true_positive, true_negative

def main(df_all_matches, df_with_attributes, df_labeled_data, matching_attribute):
    column_names = ['try_number', 'dataset_size', 'matching_attribute', 'hash_type', 'shingles_size',
                    'shingle_size', 'hash_weight', 'buckets_type', 'signature_size', 'bands_number',
                    'comparison_method', 'sum_score', 'attribute_threshold',
                    'total_time', 'signatures_creation_time', 'buckets_creation_time', 'finding_matches_time',
                    'number_of_matches', 'false_pos', 'false_neg', 'true_pos', 'true_neg',
                    'false_pos_rate', 'false_neg_rate', 'true_pos_rate', 'true_neg_rate']
    df_results_all_tries = pd.DataFrame(columns=column_names)

    print("Started joining the result with the labeled data...")
    df_matches_estimation = pd.merge(df_all_matches, df_labeled_data, how='left',
                                                                     left_on=['doc_1', 'doc_2'], right_on=['id_x', 'id_y'])
    df_matches_estimation.to_csv("df_matches_full_{}_{}.csv".format(matching_attribute,
                                                                           str(datetime.datetime.now())))
    print("...Joint the result with the labeled data")


    df_labeled_data_not_in_df = pd.merge(df_labeled_data, df_all_matches, how='left',
                                         left_on=['id_x', 'id_y'],
                                         right_on=['doc_1', 'doc_2'])
    df_labeled_data_not_in_df = df_labeled_data_not_in_df[
        df_labeled_data_not_in_df['doc_1'].isnull()]
    df_labeled_data_not_in_df = df_labeled_data_not_in_df[['id_x', 'id_y', 'is_duplicate']]

    df_not_labeled = pd.merge(df_labeled_data_not_in_df, df_with_attributes, how='left', left_on=['id_x'],
                              right_on=['id'])
    df_not_labeled = pd.merge(df_not_labeled, df_with_attributes, how='left', left_on=['id_y'],
                              right_on=['id'])
    df_not_labeled.to_csv("df_not_labeled_{}_{}.csv".format(matching_attribute, str(datetime.datetime.now())))
    start_time = time.time()
    print("Started calculating results accuracy...")
    df_matches_estimation_for_thresholds = df_matches_estimation[
        df_matches_estimation.match_score > attribute_threshold]

    false_positive, false_negative, true_positive, true_negative = add_results_estimation(
        df_matches_estimation_for_thresholds, labeled_positive, labeled_negative)
    results_calculation = round(time.time() - start_time, 6)
    print("Calculating results accuracy took {} sec".format(results_calculation))
