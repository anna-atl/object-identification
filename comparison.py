import pandas as pd
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


def jaccard_weighted(list1, list2, hash_weight, hash_weights_list, shingles_weights_in_docs): #is not counting duplicate hashes
    intersection = Multiset(list1).intersection(list2)
    union = Multiset(list1).union(list2)

    #intersection = set(list1).intersection(list2)
    #union = set(list1 + list2)

    #    union = set(list1).union(list2) #for multisets
    if hash_weight == 'normal':
        return len(intersection)/len(union)
    elif hash_weight == 'weighted minhash' or hash_weight == 'weighted minhash 2':
        try:
            return sum([shingles_weights_in_docs[i] for i in intersection]) / sum([shingles_weights_in_docs[i] for i in union])
        except:
            print('didnt work for list1 {}, list2 {}, hash_weight {}'.format(list1, list2, hash_weight))
    else:
        try:
            return sum([hash_weights_list[i] for i in intersection])/sum([hash_weights_list[i] for i in union])
        except:
            print('didnt work for list1 {}, list2 {}, hash_weight {}'.format(list1, list2, hash_weight))

    #weighted minhash - weight in the doc
def calculate_matches_ratios(buckets_of_bands, docs_hashed, hash_weight, hash_weights_list, shingles_weights_in_docs):
    matched_pairs = {} #keys - tuple of duplicate docs and values - jacc of docs(lists of shingles(numbers))
    for buckets_of_band in buckets_of_bands:
        for bucket, docs_in_bucket in buckets_of_band.items(): #values_list - doc indexes in one buckets
            for doc_index_1 in docs_in_bucket: #iterating through doc_indexes
                for doc_index_2 in docs_in_bucket:
                    if doc_index_2 > doc_index_1 and (doc_index_1, doc_index_2) not in matched_pairs:
                        matched_pairs.setdefault((doc_index_1, doc_index_2), []).append(jaccard_weighted(docs_hashed[doc_index_1], docs_hashed[doc_index_2], hash_weight, hash_weights_list, shingles_weights_in_docs))
                        #to change here to weights........
    a = {key: item for key, item in matched_pairs.items() if 20236 in key}
    print(a)
    #if (doc_index_2 == 20236) | (doc_index_1 == 23657):
    #print(matched_pairs)

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
