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

def main(df_matches_full, df_labeled_data, labeled_positive, labeled_negative):
    column_names = ['try_number', 'dataset_size', 'matching_attribute', 'hash_type', 'shingles_size',
                    'shingle_size', 'hash_weight', 'buckets_type', 'signature_size', 'bands_number',
                    'comparison_method', 'sum_score', 'attribute_threshold',
                    'total_time', 'signatures_creation_time', 'buckets_creation_time', 'finding_matches_time',
                    'number_of_matches', 'false_pos', 'false_neg', 'true_pos', 'true_neg',
                    'false_pos_rate', 'false_neg_rate', 'true_pos_rate', 'true_neg_rate']

    print("Started joining the result with the labeled data...")
    df_matches_estimation = pd.merge(df_matches_full, df_labeled_data, how='left',
                                     left_on=['id_x', 'id_y'], right_on=['id_x', 'id_y'])
    print("...Joint the result with the labeled data")

    df_labeled_data_not_in_df = pd.merge(df_labeled_data, df_matches_full, how='left',
                                         left_on=['id_x', 'id_y'],
                                         right_on=['id_x', 'id_y'])
    df_labeled_data_not_in_df = df_labeled_data_not_in_df[df_labeled_data_not_in_df['doc_1'].isnull()]
    df_labeled_data_not_in_df = df_labeled_data_not_in_df[df_labeled_data_not_in_df['doc_2'].isnull()]

    false_positive, false_negative, true_positive, true_negative = add_results_estimation(df_matches_estimation, labeled_positive, labeled_negative)

    return df_matches_estimation, false_positive, false_negative, true_positive, true_negative