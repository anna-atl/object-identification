import pandas as pd
import time
import numpy as np
import datetime
import string

import df_imports
import shingling
import creating_signatures
import comparison
import results_evaluation

pd.set_option('display.max_columns', None)

class attribute_matching_params:
    def __init__(self, matching_attribute, matching_method, hash_type='none', shingle_size=0, hash_weight='none', bands_number=5, signature_size=50, attribute_threshold=0):
        '''
        important to have the attribute_threshold in the end bc it's not used for the finding_best_methods_for_atts
        '''
        import
        self.matching_attribute = matching_attribute

        ??self.matching_method = matching_method

class attribute_matching_params:
    def __init__(self, matching_attribute, matching_method, hash_type='none', shingle_size=0,
                         hash_weight='none', bands_number=5, signature_size=50, attribute_threshold=0):

        shingling
        self.hash_type = hash_type# token, shingle
        self.shingle_size = shingle_size# 1,2
        self.hash_weight = hash_weight# 'normal', 'frequency', 'weighted'

class attribute_matching_params:
    def __init__(self, matching_attribute, matching_method, hash_type='none', shingle_size=0,
                         hash_weight='none', bands_number=5, signature_size=50, attribute_threshold=0):

        creating_signatures
        self.bands_number = bands_number
        self.signature_size = signature_size

class attribute_matching_params:
    def __init__(self, matching_attribute, matching_method, hash_type='none', shingle_size=0,
                         hash_weight='none', bands_number=5, signature_size=50, attribute_threshold=0):

        comparison
        matching_method
        self.attribute_threshold = attribute_threshold

    def __str__(self):
        return "matching_attribute: %s, matching_method: %s,  attribute_threshold: %s, hash_type: %s, shingle_size: %s, hash_weight: %s, bands_number: %s, signature_size: %s" % (self.matching_attribute, self.matching_method, self.attribute_threshold, self.hash_type, self.shingle_size, self.hash_weight, self.bands_number, self.signature_size)

def add_attributes_to_matches():
    print("Started joining the result with the mapping table...")
    df_matches_full = pd.merge(df_matches, docs_mapping, how='left', left_on=['doc_1'], right_on=['new_index'])
    b = df_matches_full[(df_matches_full['doc_1'] == 20236)]
    print(b)

    df_matches_full = df_matches_full.drop(['new_index', 'old_index', 'doc_1'], axis=1)
    df_matches_full = pd.merge(df_matches_full, docs_mapping, how='left', left_on=['doc_2'], right_on=['new_index'])
    df_matches_full = df_matches_full.drop(['new_index', 'old_index', 'doc_2'], axis=1)
    df_matches_full = df_matches_full.rename(columns={'id_x': 'doc_1', 'id_y': 'doc_2'}, inplace=False)
    b = df_matches_full[(df_matches_full['doc_1'] == 2490742) | (df_matches_full['doc_2'] == 3121092)]
    print(b)

    b = df_matches_full.loc[df_matches_full['doc_1'] < df_matches_full['doc_2']]
    c = df_matches_full.loc[df_matches_full['doc_1'] > df_matches_full['doc_2']]
    c = c.rename(columns={'doc_1': 'doc_2', 'doc_2': 'doc_1'}, inplace=False)
    c = c[['match_score_{}'.format(attribute.matching_attribute), 'doc_1', 'doc_2']]
    df_matches_full = b.append(c)

def create_df_with_attributes(df_matches, df):
    df_matches_full = pd.merge(df_matches, df,  how='left', left_on=['doc_1'], right_on=['id'])
    df_matches_full = df_matches_full.drop(['id'], axis=1)
    df_matches_full = pd.merge(df_matches_full, df,  how='left', left_on=['doc_2'], right_on=['id'])
    df_matches_full = df_matches_full.drop(['id'], axis=1)
    return df_matches_full



def main(df, dataset_size, attribute):
    signatures_creation_time = 0
    buckets_creation_time = 0
    finding_matches_time = 0

    number_of_tries = 1 #how many random datasets should be created
    dataset_size_to_import = 500
    dataset_size = 1000000

    matching_attribute = 'name_clean'
    matching_method = 'minhash'
    hash_type = 'shingle'
    bands_number = 5
    signature_size = 50
    hash_weight = 'weighted minhash'
    shingle_size = 3
    attribute_thresholds = [0, 0.5, 0.9, 0.99, 1.0]

    attribute = attribute_matching_params(matching_attribute, matching_method, hash_type,
                                          shingle_size, hash_weight, bands_number,
                                          signature_size)

    start_time = time.time()
    print('------------------------------------------------')
    print('Started downloading datasets')
    df, docs_mapping = df_imports.df_import(dataset_size_to_import)
    print("Importing datasets took --- %s seconds ---" % (time.time() - start_time))

    print("Started adding matches attributes...")
    df_all_matches = create_df_with_attributes(df_all_matches, df)
    print("...Added matches attributes")

    print('------------------------------------------------')
    print("Matching algorithm took for the {} attribute with {} and {} size --- {} seconds ---".format(
    attribute.matching_attribute, attribute.matching_method, len(docs), time.time() - start_time))




