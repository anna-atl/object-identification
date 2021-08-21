import pandas as pd
import time
import numpy as np
import string
import collections
import datetime
from functools import reduce
import random
import Levenshtein
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

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

def create_shingles(doc, k):
    if len(doc) >= k:
        shingles = [doc[i:i + k] for i in range(len(doc) - k + 1)]
    else:
        shingles = [doc + '_' * (k - len(doc))]
    return shingles

#creating shingles_dict
def create_hashes(docs, hash_type, shingle_size, hash_weight):
    '''
    This function creates a list and dict of shingles
    :param texts:one string column of a df (comp names), which is going to be divided in shingles
    :param k: shingle size
    :return: list of all shingles with order
    dict of shingles, shingle as a key, index of the shingle in the list as a value
    '''
    hashes_set = set()#hash - hashed token or shingle
    hash_weights_dict = {} #token - unique word, value - tokens index
    k = shingle_size

    for doc in docs:
        if hash_type == 'token':
            hashes = [word for word in doc.split()]
        elif hash_type == 'shingle':
            hashes = create_shingles(doc, k)
        for word_index, word in enumerate(reversed(hashes)):
            hashes_set.add(word)
            if hash_weight == 'weighted' or hash_weight == 'weighted minhash':
                hash_weights_dict.setdefault(word, []).append(word_index + 1)
    if hash_weight == 'weighted hash':
        avg = {key: sum(value)/len(value)/len(value) for key, value in hash_weights_dict.items()}
        hash_weights_dict.update(avg)
    if hash_weight == 'weighted minhash':
        avg = {key: 1/len(value) for key, value in hash_weights_dict.items()}
        hash_weights_dict.update(avg)

    hashes_dict = dict(zip(hashes_set, range(len(hashes_set))))

    return hash_weights_dict, hashes_dict

#converting docs to shingles
def convert_docs_to_hashes(docs, hash_type, shingle_size, hash_weight, hash_weights_dict, hashes_dict):
    '''
    '''
    docs_hashed = [[] for i in range(len(docs))]
    hash_weights_list = [0] * len(hashes_dict) #check because when a text is smaller than k, a splace in the end is added. so this shingle might stay with 0 weight
    k = shingle_size

    for doc_hashed, doc in zip(docs_hashed, docs):
        if hash_type == 'token':
            hashes = [word for word in doc.split()]
        elif hash_type == 'shingle':
            hashes = create_shingles(doc, k)
        for word in hashes:
            doc_hashed.append(hashes_dict[word])
            if hash_weight == 'weighted' or hash_weight == 'weighted minhash':
                hash_weights_list[hashes_dict[word]] = hash_weights_dict[word]
            elif hash_weight == 'frequency':
                hash_weights_list[hashes_dict[word]] += 1

    hash_weights_list_normalized = preprocessing.normalize([hash_weights_list])
    hash_weights_list_normalized = np.round(hash_weights_list_normalized * 1000, 0)
    hash_weights_list_normalized = hash_weights_list_normalized.astype(int)
    hash_weights_list_normalized = hash_weights_list_normalized[0].tolist()
    hash_weights_list_normalized = [i + 1 for i in hash_weights_list_normalized] #fix it, this is for not creating 0 random values

    return docs_hashed, hash_weights_list_normalized

#creating signatures array
def create_signatures_array(docs_hashed, signature_size, hashes_dict, hash_weight, hash_weights_list):
    signatures = np.zeros((signature_size, len(docs_hashed))) #create a df with # rows = signature_size and #columns = docs
    hashes_shuffled = [i for i in range(len(hashes_dict))]  #create list of hashes indexes for further randomizing

    hashes_randomized = [[] for i in range(len(hash_weights_list))]

    for signature in signatures:   #iterating through rows of the signatures df
        if hash_weight == 'weighted minhash':
            print(max(hash_weights_list)) #T%O FIX IT (larger dataset -> higher weights)
            for hash_index, hash_randomized in enumerate(hashes_randomized):
                randomhash = random.sample(range(0, 1000), hash_weights_list[hash_index])
                hashes_randomized[hash_index] = randomhash
            for doc_index, doc_hashed in enumerate(docs_hashed):
                doc_b = []
                for hash_position, word in enumerate(reversed(doc_hashed)):
                    doc_a = hashes_randomized[word]
                    doc_a = doc_a[:hash_position+1]
                    doc_b.append(min(doc_a))
                try:
                    signature[doc_index] = min(doc_b)  # saving the smallest number for this randomization for this signature
                except:
                    print('didnt work for docs_hashed {}'.format(docs_hashed[doc_index]))
        else:
            random.shuffle(hashes_shuffled)
            for doc_index, doc_hashed in enumerate(docs_hashed): #for iterating over indexes in list as well
                doc_a = [hashes_shuffled[i] for i in doc_hashed] # --check this-- recreating shingles list of a doc with randomization
                try:
                    signature[doc_index] = min(doc_a) #saving the smallest number for this randomization for this signature
                except:
                    print('didnt work for docs_hashed {}'.format(docs_hashed[doc_index]))

    return signatures

def create_buckets(signatures, bands_number):
    r = int(len(signatures)/bands_number) #number of rows per band
    buckets_of_bands = [{} for i in range(bands_number)] #create a list of dict which is number of buckets
    #buckets are going to be created (buckets - potential similar items)

    for band, buckets_of_band in zip(range(bands_number), buckets_of_bands): #buckets - dict:1,2,3,4,5. key - one bucket
        for doc_index in range(len(signatures[0])): #iterating through docs
            buckets_of_band.setdefault(tuple(signatures[band*r:band*r+r, doc_index]), []).append(doc_index)
            #setdefault(key, value) creates a key if it doesnt exist with the value (here - list)
            #if the signatures of this bucket are same then key is the signature and values: doc index
            #if unique signature then only one doc_index will be as a value
            if (doc_index == 20236) | (doc_index == 23657):
                print(signatures[band * r:band * r + r, doc_index])
                print(doc_index)

        filtered = {key: item for key, item in buckets_of_band.items() if len(item) > 1} #filter out where one value (one doc with this signature)
        buckets_of_band.clear()
        buckets_of_band.update(filtered)

        #a = {key: item for key, item in buckets_of_band.items() if len(set(item).intersection([20236, 23657])) >= 2} #filter out where one value (one doc with this signature)
        #print(a)

    return buckets_of_bands

def jaccard_weighted(list1, list2, hash_weight, hash_weights_list): #is not counting duplicate hashes
    #intersection = multiset(list1).intersection(list2)
    #union = multiset(list1 + list2)

    intersection = set(list1).intersection(list2)
    union = set(list1 + list2)

    #    union = set(list1).union(list2) #for multisets
    if hash_weight == 'normal':
        return len(intersection)/len(union)
    else:
        try:
            return sum([hash_weights_list[i] for i in intersection])/sum([hash_weights_list[i] for i in union])
        except:
            print('didnt work for list1 {}, list2 {}, hash_weight {}, hash_weights_list {}'.format(list1, list2, hash_weight, hash_weights_list))

def calculate_matches_ratios(buckets_of_bands, docs_hashed, hash_weight, hash_weights_list):
    matched_pairs = {} #keys - tuple of duplicate docs and values - jacc of docs(lists of shingles(numbers))
    for buckets_of_band in buckets_of_bands:
        for bucket, docs_in_bucket in buckets_of_band.items(): #values_list - doc indexes in one buckets
            for doc_index_1 in docs_in_bucket: #iterating through doc_indexes
                for doc_index_2 in docs_in_bucket:
                    if doc_index_2 > doc_index_1 and (doc_index_1, doc_index_2) not in matched_pairs:
                        matched_pairs.setdefault((doc_index_1, doc_index_2), []).append(jaccard_weighted(docs_hashed[doc_index_1], docs_hashed[doc_index_2], hash_weight, hash_weights_list))
    a = {key: item for key, item in matched_pairs.items() if 20236 in key}
    print(a)
    #if (doc_index_2 == 20236) | (doc_index_1 == 23657):
    #print(matched_pairs)

    return matched_pairs

def minhash(docs, attribute):
    start_time = time.time()
    print("Started creating hashes...")
    hash_weights_dict, hashes_dict = create_hashes(docs, attribute.hash_type, attribute.shingle_size, attribute.hash_weight)
    print("Creating hashes took --- %s seconds ---" % (time.time() - start_time))

    start_time = time.time()
    print("Started converting docs to hashes...")
    docs_hashed, hash_weights_list = convert_docs_to_hashes(docs, attribute.hash_type, attribute.shingle_size, attribute.hash_weight, hash_weights_dict, hashes_dict)
    print("Converting docs to hashes took --- %s seconds ---" % (time.time() - start_time))

    start_time = time.time()
    print("Started creating signatures...")
    signatures = create_signatures_array(docs_hashed, attribute.signature_size, hashes_dict, attribute.hash_weight, hash_weights_list)
    signatures_creation_time = round(time.time() - start_time, 6)
    print("Creating signatures took --- %s seconds ---" % (signatures_creation_time))

    start_time = time.time()
    print("Started creating buckets of potential matches...")
    buckets_of_bands = create_buckets(signatures, attribute.bands_number)
    buckets_creation_time = round(time.time() - start_time, 6)
    print("Creating buckets took --- %s seconds ---" % (buckets_creation_time))

    start_time = time.time()
    print("Started calculating jacc for potential matches in buckets...")
    matched_pairs = calculate_matches_ratios(buckets_of_bands, docs_hashed, attribute.hash_weight, hash_weights_list)
    finding_matches_time = round(time.time() - start_time, 6)
    print("Creating matches (jaccard) took --- %s seconds ---" % (finding_matches_time))

    return matched_pairs, signatures_creation_time, buckets_creation_time, finding_matches_time

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

