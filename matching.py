import pandas as pd
import time
import numpy as np
import datetime

import pandas as pd
import time
import math
import numpy as np
import string
import collections
import datetime
from functools import reduce
import random
import Levenshtein
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from multiset import Multiset
from sklearn import preprocessing


pd.set_option('display.max_columns', None)

class attribute_matching_params:
    def __init__(self, matching_attribute, matching_method, hash_type='none', shingle_size=0, hash_weight='none', bands_number=5, signature_size=50, attribute_threshold=0):
        '''
        important to have the attribute_threshold in the end bc it's not used for the finding_best_methods_for_atts
        '''
        self.matching_attribute = matching_attribute
        self.matching_method = matching_method
        self.hash_type = hash_type# token, shingle
        self.shingle_size = shingle_size# 1,2
        self.hash_weight = hash_weight# 'normal', 'frequency', 'weighted'
        self.bands_number = bands_number
        self.signature_size = signature_size
        self.attribute_threshold = attribute_threshold

    def __str__(self):
        return "matching_attribute: %s, matching_method: %s,  attribute_threshold: %s, hash_type: %s, shingle_size: %s, hash_weight: %s, bands_number: %s, signature_size: %s" % (self.matching_attribute, self.matching_method, self.attribute_threshold, self.hash_type, self.shingle_size, self.hash_weight, self.bands_number, self.signature_size)

def minhash(docs, attribute):
    start_time = time.time()
    print("Started creating hashes...")
    hash_weights_list, hashes_dict = create_hashes(docs, attribute.hash_type, attribute.shingle_size, attribute.hash_weight)
    print("Creating hashes took --- %s seconds ---" % (time.time() - start_time))

    start_time = time.time()
    print("Started converting docs to hashes...")
    docs_hashed, shingles_weights_in_docs = convert_docs_to_hashes(docs, attribute.hash_type, attribute.shingle_size, attribute.hash_weight, hash_weights_list, hashes_dict)
    print("Converting docs to hashes took --- %s seconds ---" % (time.time() - start_time))

    start_time = time.time()
    print("Started creating signatures...")
    signatures = create_signatures_array(docs_hashed, attribute.signature_size, hashes_dict, attribute.hash_weight, hash_weights_list, shingles_weights_in_docs)
    signatures_creation_time = round(time.time() - start_time, 6)
    print("Creating signatures took --- %s seconds ---" % (signatures_creation_time))

    start_time = time.time()
    print("Started creating buckets of potential matches...")
    buckets_of_bands = create_buckets(signatures, attribute.bands_number, shingles_weights_in_docs)
    buckets_creation_time = round(time.time() - start_time, 6)
    print("Creating buckets took --- %s seconds ---" % (buckets_creation_time))

    start_time = time.time()
    print("Started calculating jacc for potential matches in buckets...")
    matched_pairs = calculate_matches_ratios(buckets_of_bands, docs_hashed, attribute.hash_weight, hash_weights_list)
    finding_matches_time = round(time.time() - start_time, 6)
    print("Creating matches (jaccard) took --- %s seconds ---" % (finding_matches_time))

    return matched_pairs, signatures_creation_time, buckets_creation_time, finding_matches_time

def main(df, dataset_size, attribute):
    signatures_creation_time = 0
    buckets_creation_time = 0
    finding_matches_time = 0

    #docs = df[attribute.matching_attribute]
    docs = df[['id', attribute.matching_attribute]]
    docs = docs.dropna(subset=[attribute.matching_attribute])
    #docs = docs.dropna()
    #docs = docs.head(dataset_size)
    #docs = docs.sample(n=dataset_size)
    docs_mapping = docs
    docs = docs[attribute.matching_attribute]
    print(docs)

    print('------------------------------------------------')
    print('The attribute {} has {} records'.format(attribute.matching_attribute, len(docs)))

    #docs_mapping = df[['id', attribute.matching_attribute]]
    docs_mapping = docs_mapping.dropna(subset=[attribute.matching_attribute])
    docs_mapping['old_index'] = docs_mapping.index
    docs_mapping = docs_mapping.reset_index(drop=True)
    docs_mapping['new_index'] = docs_mapping.index
    docs_mapping = docs_mapping.drop([attribute.matching_attribute], axis=1)

    b = docs_mapping.loc[(docs_mapping['id'] == 2490742) | (docs_mapping['id'] == 3121092)]
    print(b)

    start_time = time.time()
    if attribute.matching_method == 'minhash':
        print('Started {} for the {} attribute with {} hash type:'.format(attribute.matching_method,
                                                                          attribute.matching_attribute,
                                                                          attribute.hash_type))
        matched_pairs, signatures_creation_time, buckets_creation_time, finding_matches_time = minhash(docs, attribute)

    else:
        matched_pairs = other_matching_methods(docs, attribute.matching_method)

    print("Started creating one match score column from tuples...")

    if len(matched_pairs) != 0:
        df_matches = pd.DataFrame.from_dict(matched_pairs, orient='index', columns=['match_score_{}'.format(attribute.matching_attribute)])  # oriend='index' for making keys as rows, not columns
        df_matches['matches_tuple'] = df_matches.index
        a = df_matches['matches_tuple'].tolist()
        df_matches[['doc_1', 'doc_2']] = pd.DataFrame(df_matches['matches_tuple'].tolist(), index=df_matches.index)
        b = df_matches[(df_matches['doc_1'] == 20236) | (df_matches['doc_2'] == 23657)]
        print(b)
        df_matches = df_matches.drop(['matches_tuple'], axis=1)
        df_matches = df_matches.reset_index(drop=True)
    else:
        column_names = ['match_score_{}'.format(attribute.matching_attribute), 'doc_1', 'doc_2']
        df_matches = pd.DataFrame(columns=column_names)

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

    print('------------------------------------------------')
    print("Matching algorithm took for the {} attribute with {} and {} size --- {} seconds ---".format(attribute.matching_attribute, attribute.matching_method, len(docs), time.time() - start_time))

    return df_matches_full, signatures_creation_time, buckets_creation_time, finding_matches_time

