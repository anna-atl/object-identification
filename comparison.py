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

def jaccard_comparison(list1, list2, comparison_method, shingles_weights_in_doc_1, shingles_weights_in_doc_2): #is not counting duplicate hashes
    intersection = Multiset(list1).intersection(list2)
    union = Multiset(list1).union(list2)

    #intersection = set(list1).intersection(list2)
    #union = set(list1 + list2)

    #    union = set(list1).union(list2) #for multisets
    if comparison_method == 'jaccard':
        return len(intersection)/len(union)
    elif comparison_method == 'weighted jaccard':
        # weighted minhash - weight in the doc
        try:
            return sum(min((list1, shingles_weights_in_doc_1), (list2, shingles_weights_in_doc_2))) / sum(max(
                (list1, shingles_weights_in_doc_1), (list2, shingles_weights_in_doc_2)))
        except:
            print('didnt work for list1 {}, list2 {}'.format(list1, list2))
    else:
        try:
            return sum([shingles_weights_in_docs[i] for i in intersection])/sum([shingles_weights_in_docs[i] for i in union])
        except:
            print('didnt work for list1 {}, list2 {}'.format(list1, list2))

def calculate_matches_ratios(buckets_of_bands, docs_shingled, comparison_method, shingles_weights_in_docs):
    matched_pairs = {} #keys - tuple of duplicate docs and values - jacc of docs(lists of shingles(numbers))
    for buckets_of_band in buckets_of_bands:
        for bucket, docs_in_bucket in buckets_of_band.items(): #values_list - doc indexes in one buckets
            for doc_index_1 in docs_in_bucket: #iterating through doc_indexes
                for doc_index_2 in docs_in_bucket:
                    if doc_index_2 > doc_index_1 and (doc_index_1, doc_index_2) not in matched_pairs:
                        matched_pairs.setdefault((doc_index_1, doc_index_2), []).append(jaccard_comparison(docs_shingled[doc_index_1], docs_shingled[doc_index_2], comparison_method))
                        #to change here to weights........

    return matched_pairs

def gen_jaccard_comparison(doc_shingled_1, doc_shingled_2, shingles_weights_in_doc_1, shingles_weights_in_doc_2, comparison_method): #is not counting duplicate hashes
    intersection = Multiset(doc_shingled_1).intersection(doc_shingled_2)
    union = Multiset(doc_shingled_1).union(doc_shingled_2)

    # weighted minhash - weight in the doc
    try:
        #print(doc_shingled_1)
        #print(doc_shingled_2)
        #print(union)
        #print(intersection)
        #print([shingles_weights_in_doc_1.get(i, 0) for i in union])
        #print([shingles_weights_in_doc_2.get(i, 0) for i in union])
        #print([min(shingles_weights_in_doc_1[i][0], shingles_weights_in_doc_2[i][0]) for i in intersection])
        #print([max(shingles_weights_in_doc_1.get(i, [0])[0], shingles_weights_in_doc_2.get(i, [0])[0]) for i in union])
        #print(sum([min(shingles_weights_in_doc_1[i][0], shingles_weights_in_doc_2[i][0]) for i in intersection]))
        #print(sum([max(shingles_weights_in_doc_1.get(i, [0])[0], shingles_weights_in_doc_2.get(i, [0])[0]) for i in union]))
        return sum([min(shingles_weights_in_doc_1[i][0], shingles_weights_in_doc_2[i][0]) for i in intersection])/sum([max(shingles_weights_in_doc_1.get(i, [0])[0], shingles_weights_in_doc_2.get(i, [0])[0]) for i in union])
    except:
        print('didnt work for list1 {}, list2 {}'.format(doc_shingled_1, doc_shingled_2))

def calculate_weighted_matches_ratios(buckets_of_bands, docs_shingled, comparison_method, shingles_weights_in_docs, docs_mapping_old_new):
    matched_pairs = {} #keys - tuple of duplicate docs and values - jacc of docs(lists of shingles(numbers))
    for buckets_of_band in buckets_of_bands:
        for bucket, docs_in_bucket in buckets_of_band.items(): #values_list - doc indexes in one buckets
            for doc_index_1 in docs_in_bucket: #iterating through doc_indexes
                for doc_index_2 in docs_in_bucket:
                    if doc_index_2 > doc_index_1 and (doc_index_1, doc_index_2) not in matched_pairs:
                        try:
                            matched_pairs.setdefault((doc_index_1, doc_index_2), []).append(gen_jaccard_comparison(docs_shingled[docs_mapping_old_new[doc_index_1]], docs_shingled[docs_mapping_old_new[doc_index_2]], shingles_weights_in_docs[docs_mapping_old_new[doc_index_1]], shingles_weights_in_docs[docs_mapping_old_new[doc_index_2]], comparison_method))
                        except:
                            #print('didnt work')
                            matched_pairs[(doc_index_1, doc_index_2)] = [0]

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

def main(buckets_of_bands, docs_shingled, comparison_method, shingles_weights_in_docs, matching_attribute, docs_mapping_old_new):
    start_time = time.time()
    print("Started calculating jacc for potential matches in buckets...")
    matched_pairs = calculate_weighted_matches_ratios(buckets_of_bands, docs_shingled, comparison_method, shingles_weights_in_docs, docs_mapping_old_new)
    finding_matches_time = round(time.time() - start_time, 6)
    print("Creating matches (jaccard) took --- %s seconds ---" % (finding_matches_time))

    print("Started creating one match score column from tuples...")
    if len(matched_pairs) != 0:
        df_matches = pd.DataFrame.from_dict(matched_pairs, orient='index', columns=[
            'match_score_{}'.format(matching_attribute)])  # oriend='index' for making keys as rows, not columns
        df_matches['matches_tuple'] = df_matches.index
        a = df_matches['matches_tuple'].tolist()
        df_matches[['doc_1', 'doc_2']] = pd.DataFrame(df_matches['matches_tuple'].tolist(), index=df_matches.index)
        df_matches = df_matches.drop(['matches_tuple'], axis=1)
        df_matches = df_matches.reset_index(drop=True)
    else:
        column_names = ['match_score_{}'.format(matching_attribute), 'doc_1', 'doc_2']
        df_matches = pd.DataFrame(columns=column_names)
    print('------------------------------------------------')

    return df_matches

