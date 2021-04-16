import pandas as pd
import time
import numpy as np
import string
import collections
#import binascii
import datetime
import df_imports

import random
pd.set_option('display.max_columns', None)

class minhash_params:
    def __init__(self, matching_attribute, hash_type, weights_method, bands_number=5, signature_size=50):
        self.matching_attribute = matching_attribute
        self.hash_type = hash_type #[tokens], [shingles, shingle_size], [exact]
        self.weights_method = weights_method # 'normal', 'frequency', 'weighted'
        self.bands_number = bands_number
        self.signature_size = signature_size
"""
class scenario_params:
    df_size = 1000
    time_spent = 1
    df_matches_output =

    def __init__(self, df_size):
        pass
"""

#creating shingles_dict
def create_hashes(docs, hash_type, weights_method):
    '''
    This function creates a list and dict of shingles
    :param texts:one string column of a df (comp names), which is going to be divided in shingles
    :param k: shingle size
    :return: list of all shingles with order
    dict of shingles, shingle as a key, index of the shingle in the list as a value
    '''
    hashes_set = set()#hash - hashed token or shingle
    hash_weights_dict = {} #token - unique word, value - tokens index

    if hash_type[0] == 'tokens':
        for doc in docs:
            words = [word for word in doc.split()]
            for word_index, word in enumerate(reversed(words)):
                hashes_set.add(word)
                if weights_method == 'weighted':
                    hash_weights_dict.setdefault(word, []).append(word_index + 1)
        if weights_method == 'weighted':
            avg = {key: sum(value)/len(value)/len(value) for key, value in hash_weights_dict.items()}
            hash_weights_dict.update(avg)

    elif hash_type[0] == 'shingles':
        k = hash_type[1]
        for doc in docs:
            if len(doc) >= k:
                shingles = [doc[i:i + k] for i in range(len(doc) - k + 1)]
            else:
                shingles = [doc + '_' * (k - len(doc))]
            for shingle in shingles:
                hashes_set.add(shingle)
                if weights_method == 'weighted':
                    continue
                    #hash_weights_dict.setdefault(shingle, []).append(word_index + 1)
    #how the row above???
    hashes_dict = dict(zip(hashes_set, range(len(hashes_set))))

    return hash_weights_dict, hashes_set, hashes_dict


#converting docs to shingles
def create_doc_tokens(docs, hash_type, weights_method, hash_weights_dict, hashes_dict):
    '''
    '''
    docs_hashed = [[] for i in range(len(docs))]
    hash_weights_list = [0] * len(hashes_dict) #check because when a text is smaller than k, a splace in the end is added. so this shingle might stay with 0 weight

    if hash_type[0] == 'tokens':
        for doc_hashed, doc in zip(docs_hashed, docs):
            words = [word for word in doc.split()]
            for word in words:
                doc_hashed.append(hashes_dict[word])
                if weights_method == 'weighted':
                    hash_weights_list[hashes_dict[word]] = hash_weights_dict[word]
                elif weights_method == 'frequency':
                    hash_weights_list[hashes_dict[word]] += 1

    elif hash_type[0] == 'shingles':
        k = hash_type[1]
        for doc_hashed, doc in zip(docs_hashed, docs):
            if len(doc) >= k:
                shingles = [doc[i:i + k] for i in range(len(doc) - k + 1)]
            else:
                shingles = [doc + '_' * (k - len(doc))]
            for shingle in shingles:
                doc_hashed.append(hashes_dict[shingle])
                if weights_method == 'frequency':
                    hash_weights_list[hashes_dict[shingle]] += 1

    if weights_method == 'frequency':
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
            signature[doc_index] = min(doc_a) #saving the smallest number for this randomization for this signature
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

def jaccard_weighted(list1, list2, weights_method, hash_weights_list): #is not counting duplicate hashes
    intersection = set(list1).intersection(list2)
    union = set(list1 + list2)
    #    union = set(list1).union(list2) #for multisets
    if weights_method == 'normal':
        return len(intersection)/len(union)
    else:
        return sum([hash_weights_list[i] for i in intersection])/sum([hash_weights_list[i] for i in union])

def calculate_matches_ratios(buckets_of_bands, docs_hashed, weights_method, hash_weights_list):
    matched_pairs = {} #keys - tuple of duplicate docs and values - jacc of docs(lists of shingles(numbers))
    for buckets_of_band in buckets_of_bands:
        for bucket, docs_in_bucket in buckets_of_band.items(): #values_list - doc indexes in one buckets
            for doc_index_1 in docs_in_bucket: #iterating through doc_indexes
                for doc_index_2 in docs_in_bucket:
                    if doc_index_2 > doc_index_1 and (doc_index_1, doc_index_2) not in matched_pairs:
                        matched_pairs.setdefault((doc_index_1, doc_index_2), []).append(jaccard_weighted(docs_hashed[doc_index_1], docs_hashed[doc_index_2], weights_method, hash_weights_list))
    return matched_pairs

#generate df with all potential matches
def create_df_with_attributes(matches,df):
    print("Started adding matches attributes...")
    df_matches = pd.DataFrame.from_dict(matches, orient='index')
    df_matches['matches_tuple'] = df_matches.index
    df_matches[['doc_1', 'doc_2']] = pd.DataFrame(df_matches['matches_tuple'].tolist(), index=df_matches.index)
    df_matches = df_matches.drop(['matches_tuple'], axis=1)
    df_matches = df_matches.reset_index(drop=True)
    df['index'] = df.index

    df_matches_full = pd.merge(df_matches, df,  how='left', left_on=['doc_1'], right_on = ['index'])
    df_matches_full = df_matches_full.drop(['index'], axis=1)
    df_matches_full = pd.merge(df_matches_full, df,  how='left', left_on=['doc_2'], right_on = ['index'])
    df_matches_full = df_matches_full.drop(['index'], axis=1)
    return df_matches_full

def minhash(df, parameters):
    docs = df[parameters.matching_attribute] #which column to use for minhash
    docs = docs.dropna()

    start_time = time.time()
    print("Started creating hashes...")
    hash_weights_dict, hashes_set, hashes_dict = create_hashes(docs, parameters.hash_type, parameters.weights_method)
    print("Creating hashes took --- %s seconds ---" % (time.time() - start_time))

    start_time = time.time()
    print("Started converting docs to hashes...")
    docs_hashed, hash_weights_list = create_doc_tokens(docs, parameters.hash_type, parameters.weights_method, hash_weights_dict, hashes_dict)
    print("Converting docs to hashes took --- %s seconds ---" % (time.time() - start_time))

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
    matched_pairs = calculate_matches_ratios(buckets_of_bands, docs_hashed, parameters.weights_method, hash_weights_list)
    print("Creating matches (jaccard) took --- %s seconds ---" % (time.time() - start_time))

    print(matched_pairs)
    return matched_pairs

if __name__ == "__main__":
    datasets_size = [1000000]

    ps1 = minhash_params('name_clean', ['tokens'], 'weighted')
    ps2 = minhash_params('name_clean', ['tokens'], 'normal')
    ps3 = minhash_params('name_clean', ['tokens'], 'frequency')
    ps4 = minhash_params('name_clean', ['shingles', 3], 'normal')
    ps5 = minhash_params('name_clean', ['shingles', 3], 'frequency')
    ps6 = minhash_params('url_clean', ['shingles', 3], 'normal')
#    print(ps1.matching_attribute, ps1.split_method, ps1.weights_method, ps1.shingle_size, ps1.signature_size)

    start_time = time.time()
    df = df_imports.df_import()
    print("Importing datasets took --- %s seconds ---" % (time.time() - start_time))

    for n in datasets_size:
        start_time = time.time()
        print('Started MinHash with the dataset (%s size):' % n)
        matched_pairs = minhash(df.head(n), ps6)
        print("MinHash algorithm took --- %s seconds ---" % (time.time() - start_time))

        df_matches_full = create_df_with_attributes(matched_pairs, df)
        df_matches_full.to_csv("df_matches_full_{}_{}.csv".format(n, str(datetime.datetime.now())))

    '''
        time_spent.append(a)
        df_matches_outputs.append(df_matches_full)
        print("Whole algorithm took --- %s seconds ---" % (time.time() - start_time))


    for a, n in zip(time_spent, datasets_size):
        print('for {} dataset size it took {} seconds'.format(n, a))

    '''
    print('end')