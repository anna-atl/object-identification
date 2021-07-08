import pandas as pd
import time
import numpy as np
import string
import collections
import datetime
from functools import reduce
import random
pd.set_option('display.max_columns', None)

class attribute_matching_params:
    def __init__(self, matching_attribute, matching_method, hash_type='none', hash_weight='none', shingle_size=2, bands_number=5, signature_size=50, attribute_threshold=0, attribute_weight=1):
        self.matching_attribute = matching_attribute
        self.matching_method = matching_method
        self.attribute_threshold = attribute_threshold
        self.hash_type = hash_type# token, shingle
        self.shingle_size = shingle_size# 1,2
        self.hash_weight = hash_weight# 'normal', 'frequency', 'weighted'
        self.bands_number = bands_number
        self.signature_size = signature_size

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
            if hash_weight == 'weighted':
                hash_weights_dict.setdefault(word, []).append(word_index + 1)
        if hash_weight == 'weighted':
            avg = {key: sum(value)/len(value)/len(value) for key, value in hash_weights_dict.items()}
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
            if hash_weight == 'weighted':
                hash_weights_list[hashes_dict[word]] = hash_weights_dict[word]
            elif hash_weight == 'frequency':
                hash_weights_list[hashes_dict[word]] += 1

    if hash_weight == 'frequency':
        hash_weights_list = [1 / hash_weight for hash_weight in hash_weights_list]

    return docs_hashed, hash_weights_list

#creating signatures array
def create_signatures_array(docs_hashed, signature_size, hashes_dict):
    signatures = np.zeros((signature_size, len(docs_hashed))) #create a df with # rows = signature_size and #columns = docs
    hashes_shuffled = [i for i in range(len(hashes_dict))]  #create list of hashes indexes for further randomizing

    for signature in signatures:   #iterating through rows of the signatures df
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
        filtered = {key: item for key, item in buckets_of_band.items() if len(item) > 1} #filter out where one value (one doc with this signature)
        buckets_of_band.clear()
        buckets_of_band.update(filtered)
    return buckets_of_bands

def jaccard_weighted(list1, list2, hash_weight, hash_weights_list): #is not counting duplicate hashes
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
    return matched_pairs


def minhash(docs, parameters):
    start_time = time.time()
    print("Started creating hashes...")
    hash_weights_dict, hashes_dict = create_hashes(docs, parameters.hash_type, parameters.shingle_size, parameters.hash_weight)
    print("Creating hashes took --- %s seconds ---" % (time.time() - start_time))

    start_time = time.time()
    print("Started converting docs to hashes...")
    docs_hashed, hash_weights_list = convert_docs_to_hashes(docs, parameters.hash_type, parameters.shingle_size, parameters.hash_weight, hash_weights_dict, hashes_dict)
    print("Converting docs to hashes took --- %s seconds ---" % (time.time() - start_time))

    for doc_index, doc_hashed in enumerate(docs_hashed):
        if len(doc_hashed) == 0:
            print(doc_hashed)
            print(docs[doc_index])
            print(doc_index)

    start_time = time.time()
    print("Started creating signatures...")
    signatures = create_signatures_array(docs_hashed, parameters.signature_size, hashes_dict)
    print("Creating signatures took --- %s seconds ---" % (time.time() - start_time))

    start_time = time.time()
    print("Started creating buckets of potential matches...")
    buckets_of_bands = create_buckets(signatures, parameters.bands_number)
    print("Creating buckets took --- %s seconds ---" % (time.time() - start_time))

    start_time = time.time()
    print("Started calculating jacc for potential matches in buckets...")
    matched_pairs = calculate_matches_ratios(buckets_of_bands, docs_hashed, parameters.hash_weight, hash_weights_list)
    print("Creating matches (jaccard) took --- %s seconds ---" % (time.time() - start_time))

    if len(matched_pairs) != 0:
        df_matches = pd.DataFrame.from_dict(matched_pairs, orient='index', columns=['match_score_{}'.format(parameters.matching_attribute)]) #oriend='index' for making keys as rows, not columns
        df_matches['matches_tuple'] = df_matches.index
        a = df_matches['matches_tuple'].tolist()
        df_matches[['doc_1', 'doc_2']] = pd.DataFrame(df_matches['matches_tuple'].tolist(), index=df_matches.index)
        df_matches = df_matches.drop(['matches_tuple'], axis=1)
        df_matches = df_matches.reset_index(drop=True)
    else:
        column_names = ['match_score_{}'.format(parameters.matching_attribute), 'doc_1', 'doc_2']
        df_matches = pd.DataFrame(columns=column_names)

    return df_matches

#if __name__ == "__main__":

def main(df, dataset_size, attribute):
    docs = df[attribute.matching_attribute]

    docs = docs.dropna()
    docs = docs.head(dataset_size)
    print('------------------------------------------------')
    print('The attribute {} has {} records'.format(attribute.matching_attribute, len(docs)))

    docs_mapping = df[['id', attribute.matching_attribute]]
    docs_mapping = docs_mapping.dropna(subset=[attribute.matching_attribute])
    docs_mapping['old_index'] = docs_mapping.index
    docs_mapping = docs_mapping.reset_index(drop=True)
    docs_mapping['new_index'] = docs_mapping.index
    docs_mapping = docs_mapping.drop([attribute.matching_attribute], axis=1)

    start_time = time.time()
    if attribute.matching_method == 'minhash':
        print('Started MinHash for the {} attribute:'.format(attribute.matching_attribute))
        df_matches = minhash(docs, attribute)

        df_matches_full = pd.merge(df_matches, docs_mapping, how='left', left_on=['doc_1'], right_on=['new_index'])
        df_matches_full = df_matches_full.drop(['new_index', 'old_index', 'doc_1'], axis=1)
        df_matches_full = pd.merge(df_matches_full, docs_mapping, how='left', left_on=['doc_2'], right_on=['new_index'])
        df_matches_full = df_matches_full.drop(['new_index', 'old_index', 'doc_2'], axis=1)
        df_matches_full = df_matches_full.rename(columns={'id_x': 'doc_1', 'id_y': 'doc_2'}, inplace=False)

    print('------------------------------------------------')
    print("Matching algorithm took for the {} attribute with {} and {} size --- {} seconds ---".format(attribute.matching_attribute, attribute.matching_method, len(docs), time.time() - start_time))

    #df_all_matches.to_csv("df_matches_full_{}_{}.csv".format(len(df), str(datetime.datetime.now())))
    return df_matches_full

