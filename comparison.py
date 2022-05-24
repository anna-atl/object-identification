import pandas as pd
import numpy as np
import collections
import time
import datetime
from functools import reduce
import random
import Levenshtein
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from multiset import Multiset
from sklearn import preprocessing

def jaccard_comparison(doc_shingled_1, doc_shingled_2): #is not counting duplicate hashes
    intersection = Multiset(doc_shingled_1).intersection(doc_shingled_2)
    union = Multiset(doc_shingled_1).union(doc_shingled_2)
    return len(intersection)/len(union)

def calculate_matches_ratios(buckets_of_bands, docs_shingled, docs_mapping_old_new):
    matched_pairs = {} #keys - tuple of duplicate docs and values - jacc of docs(lists of shingles(numbers))
    for buckets_of_band in buckets_of_bands:
        for bucket, docs_in_bucket in buckets_of_band.items(): #values_list - doc indexes in one buckets
            for doc_index_1 in docs_in_bucket: #iterating through doc_indexes
                for doc_index_2 in docs_in_bucket:
                    if doc_index_2 > doc_index_1 and (doc_index_1, doc_index_2) not in matched_pairs:
                        matched_pairs.setdefault((doc_index_1, doc_index_2), []).append(
                            jaccard_comparison(docs_shingled[docs_mapping_old_new[doc_index_1]],
                                                   docs_shingled[docs_mapping_old_new[doc_index_2]]))
    return matched_pairs

def gen_jaccard_comparison(doc_shingled_1, doc_shingled_2, shingles_weights_in_doc_1, shingles_weights_in_doc_2): #is not counting duplicate hashes
    #intersection = Multiset(doc_shingled_1).intersection(doc_shingled_2)
    #union = Multiset(doc_shingled_1).union(doc_shingled_2)

    intersection = set(doc_shingled_1).intersection(doc_shingled_2)
    union = set(doc_shingled_1).union(doc_shingled_2)

    try:
        min_weight = sum([min(shingles_weights_in_doc_1[shingle], shingles_weights_in_doc_2[shingle]) for shingle in intersection])
    except:
        print('pizdec')
    try:
        max_weight = sum([max(shingles_weights_in_doc_1.get(shingle, 0), shingles_weights_in_doc_2.get(shingle, 0)) for shingle in union])
    except:
        print('pizdec')

    try:
        return min_weight/max_weight
        #for doc_index in union:
        
        #for shingle, weight in zip(doc_shingled_1, shingles_weights_in_doc_1):
        #return sum([min(shingles_weights_in_doc_1[i], shingles_weights_in_doc_2[i]) for i in zip(intersection, shingles_weights_in_doc_2))/sum([max(shingles_weights_in_doc_1.get(i, [0])[0], shingles_weights_in_doc_2.get(i, [0])[0]) for i in union])
    except:
        print('didnt work for list1 {}, list2 {}'.format(doc_shingled_1, doc_shingled_2))

def calculate_weighted_matches_ratios(buckets_of_bands, docs_shingled, shingles_weights_in_docs_dict, docs_mapping_old_new):
    matched_pairs = {} #keys - tuple of duplicate docs and values - jacc of docs(lists of shingles(numbers))
    for buckets_of_band in buckets_of_bands:
        for bucket, docs_in_bucket in buckets_of_band.items(): #values_list - doc indexes in one buckets
            for doc_index_1 in docs_in_bucket: #iterating through doc_indexes
                for doc_index_2 in docs_in_bucket:
                    if doc_index_2 > doc_index_1 and (doc_index_1, doc_index_2) not in matched_pairs:
                        try:
                            matched_pairs.setdefault((doc_index_1, doc_index_2), []).append(gen_jaccard_comparison(docs_shingled[docs_mapping_old_new[doc_index_1]], docs_shingled[docs_mapping_old_new[doc_index_2]], shingles_weights_in_docs_dict[docs_mapping_old_new[doc_index_1]], shingles_weights_in_docs_dict[docs_mapping_old_new[doc_index_2]]))
                        except:
                            print('didnt work')
                            #matched_pairs[(doc_index_1, doc_index_2)] = [0]
    return matched_pairs


def other_matching_methods(docs, matching_method):
    matched_pairs = {} #keys - tuple of duplicate docs and values - jacc of docs(lists of shingles(numbers))

    for doc_index_1, doc1 in enumerate(docs):
        for doc_index_2, doc2 in enumerate(docs):
            if doc_index_2 > doc_index_1:
                if matching_method == 'levenstein':
                    matched_pairs.setdefault((doc_index_1, doc_index_2), []).append(Levenshtein.ratio(doc1, doc2))
                elif matching_method == 'fuzzywuzzy':
                    matched_pairs.setdefault((doc_index_1, doc_index_2), []).append(fuzz.ratio(doc1, doc2))
                elif matching_method == 'jaccard':
                    intersection = set(doc1).intersection(doc2)
                    union = set(doc1 + doc2)
                    matched_pairs.setdefault((doc_index_1, doc_index_2), []).append(len(intersection) / len(union))
                elif matching_method == 'exact':
                    if doc1 == doc2:
                        matched_pairs.setdefault((doc_index_1, doc_index_2), []).append(1)
    return matched_pairs

def main(buckets_of_bands, docs_shingled, comparison_method, shingles_weights_in_docs_dict, matching_attribute, docs_mapping_old_new):

    start_time = time.time()
    print("----Started calculating comparison functions for potential matches in buckets...")
    if comparison_method == 'weighted jaccard':
        matched_pairs = calculate_weighted_matches_ratios(buckets_of_bands, docs_shingled, shingles_weights_in_docs_dict, docs_mapping_old_new)
    elif comparison_method == 'jaccard':
        matched_pairs = calculate_matches_ratios(buckets_of_bands, docs_shingled, docs_mapping_old_new)

    if len(matched_pairs) != 0:
        df_matches = pd.DataFrame.from_dict(matched_pairs, orient='index', columns=[
            'match_score_{}'.format(matching_attribute)])  # oriend='index' for making keys as rows, not columns
        df_matches['matches_tuple'] = df_matches.index
        df_matches[['doc_1', 'doc_2']] = pd.DataFrame(list(df_matches['matches_tuple']), index=df_matches.index)
        df_matches = df_matches.drop(['matches_tuple'], axis=1)
        df_matches = df_matches.reset_index(drop=True)
    else:
        column_names = ['match_score_{}'.format(matching_attribute), 'doc_1', 'doc_2']
        df_matches = pd.DataFrame(columns=column_names)

    finding_matches_time = round(time.time() - start_time, 6)
    print("----//Calculating comparison functions took --- %s seconds ---" % (finding_matches_time))

    return df_matches

