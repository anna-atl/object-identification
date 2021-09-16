import pandas as pd
import time
import numpy as np
import datetime
import string

import df_imports
import shingling
import creating_buckets
import comparison
import results_evaluation

pd.set_option('display.max_columns', None)

class attribute_matching_params:
    def __init__(self, matching_attribute, hash_type='none', shingle_size=0, hash_weight='none', buckets_type='none', signature_size=50, bands_number=5, comparison_method='jaccard', sum_score='sum', attribute_threshold=0):
        '''
        important to have the attribute_threshold in the end bc it's not used for the finding_best_methods_for_atts
        '''
        self.matching_attribute = matching_attribute
        #shingling
        self.hash_type = hash_type# token, shingle, shingle words
        self.shingle_size = shingle_size# 1,2
        self.hash_weight = hash_weight# 'normal', 'frequency', 'weighted'
        #creating_signatures
        self.buckets_type = buckets_type #minhash, weighted minhash 1, weighted minhash 2,
        self.signature_size = signature_size
        self.bands_number = bands_number
        #comparison
        self.comparison_method = comparison_method #jaccard, weighted jaccard, fuzzy
        self.sum_score = sum_score #sum_scores, multiplication
        self.attribute_threshold = attribute_threshold

    def __str__(self):
        return "matching_attribute: %s, matching_method: %s,  attribute_threshold: %s, hash_type: %s, shingle_size: %s, hash_weight: %s, bands_number: %s, signature_size: %s" % (self.matching_attribute, self.matching_method, self.attribute_threshold, self.hash_type, self.shingle_size, self.hash_weight, self.bands_number, self.signature_size)

def add_attributes_to_matches(df_matches, df_with_attributes, docs_mapping):
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

    df_matches_full = pd.merge(df_matches, df,  how='left', left_on=['doc_1'], right_on=['id'])
    df_matches_full = df_matches_full.drop(['id'], axis=1)
    df_matches_full = pd.merge(df_matches_full, df,  how='left', left_on=['doc_2'], right_on=['id'])
    df_matches_full = df_matches_full.drop(['id'], axis=1)
    return df_matches_full

if __name__ == "__main__":
    number_of_tries = 1 #how many random datasets should be created
    dataset_size_to_import = 500
    dataset_size = 1000000

    matching_attribute = 'name_clean'
    hash_type = 'shingle'
    shingle_size = 3
    hash_weight = 'weighted minhash'
    buckets_type = 'minhash'
    signature_size = 50
    bands_number = 5
    comparison_method = 'jaccard'
    sum_score = 'sum'
    attribute_threshold = 0

    atts = attribute_matching_params(matching_attribute,
                                          hash_type, shingle_size, hash_weight,
                                          buckets_type, signature_size, bands_number,
                                          comparison_method, sum_score, attribute_threshold)

    start_time = time.time()
    print('------------------------------------------------')
    print('Started downloading datasets')
    df_with_attributes, docs_mapping, docs = df_imports.main(dataset_size_to_import, atts.matching_attribute, dataset_size)
    print("Importing datasets took --- %s seconds ---" % (time.time() - start_time))

    docs_shingled, shingles_weights_in_docs, hash_weights_list = shingling.main(docs, atts.hash_type, atts.shingle_size, atts.hash_weight)
    buckets_of_bands = creating_buckets.main(docs_shingled, hash_weights_list, shingles_weights_in_docs, atts.buckets_type, atts.signature_size, atts.bands_number)
    df_matches = comparison.main(buckets_of_bands, docs_shingled, atts.comparison_method, atts.sum_scores)

    print("Started adding matches attributes...")
    matches_with_attributes = add_attributes_to_matches(df_matches, df_with_attributes, docs_mapping)
    print("...Added matches attributes")
    results = results_evaluation.main(df_matches)

    print('------------------------------------------------')
    print("Matching algorithm took for the {} attribute with {} and {} size --- {} seconds ---".format(
    atts.matching_attribute, atts.matching_method, len(docs), time.time() - start_time))




