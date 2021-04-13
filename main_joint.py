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


class attribute_match_params:
#    hash_type = 1 #1 - tokens, 2 - shingles
#    weights_method = 1 # 1 - no weights, 2 - frequency weights, 3 - sophisticated weights

    def __init__(self, matching_attribute, hash_type, weights_method, bands_number = 5, signature_size = 50, shingle_size = 3):
        self.matching_attribute = matching_attribute
        self.hash_type = hash_type
        self.weights_method = weights_method
        self.bands_number = bands_number
        self.shingle_size = shingle_size
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
def create_hashes(docs, hash_type):
    '''
    This function creates a list and dict of shingles
    :param texts:one string column of a df (comp names), which is going to be divided in shingles
    :param k: shingle size
    :return: list of all shingles with order
    dict of shingles, shingle as a key, index of the shingle in the list as a value
    '''
    hashes_set = set()#hash - hashed token or shingle
    hash_weights_dict = {} #token - unique word, value - tokens index

    if hash_type == 'tokens':
        for doc in docs:
            words = [word for word in doc.split()]
            for word_index, word in enumerate(reversed(words)):
                hash_weights_dict.setdefault(word, []).append(word_index + 1)
                hashes_set.add(word)
        avg = {key: sum(value)/len(value)/len(value) for key, value in hash_weights_dict.items()}
        hash_weights_dict.update(avg)

    elif hash_type == 'shingles':
        for doc in docs:
            if len(doc) >= k:
                shingles = [doc[i:i + k] for i in range(len(doc) - k + 1)]
            else:
                shingles = [doc + '_' * (k - len(doc))]
            for shingle in shingles:
                hashes_set.add(shingle)
                hash_weights_dict.setdefault(shingle, []).append(word_index + 1)
#how K and how the row above???
    hashes_dict = dict(zip(hashes_set, range(len(hashes_set))))

    return hash_weights_dict, hashes_set, hashes_dict


#converting docs to shingles
def create_doc_tokens(docs, hash_type, hash_weights_dict, hashes_dict):
    '''
    '''
    docs_hashed = [[] for i in range(len(docs))]
    hash_weights_list = [0] * len(hashes_dict) #check because when a text is smaller than k, a splace in the end is added. so this shingle might stay with 0 weight

    if hash_type == 'tokens':
        for doc_hashed, doc in zip(docs_hashed, docs):
            words = [word for word in doc.split()]
            for word in words:
                doc_hashed.append(hashes_dict[word])
                hash_weights_list[hashes_dict[word]] = hash_weights_dict[word]

    elif hash_type == 'shingles':
        for doc_hashed, doc in zip(docs_hashed, docs):
            if len(doc) >= k:
                shingles = [doc[i:i + k] for i in range(len(doc) - k + 1)]
            else:
                shingles = [doc + '_' * (k - len(doc))]
            for shingle in shingles:
                doc_hashed.append(hashes_dict[shingle])

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

def jaccard_weighted(list1, list2, token_weights): #is not counting duplicate shingles
    intersection = set(list1).intersection(list2)
    union = set(list1 + list2)
#    union = set(list1).union(list2) #for multisets
    return sum([token_weights[i] for i in intersection])/sum([token_weights[i] for i in union])

def create_matches(buckets_bands, docs, token_weights):
    matches = {} #keys - tuple of duplicate docs and values - jacc of docs(lists of shingles(numbers))
    for buckets in buckets_bands:
        for key, values_list in buckets.items(): #values_list - doc indexes in one buckets
            for value_1 in values_list: #iterating through doc_indexes
                for value_2 in values_list:
                    if value_2 > value_1 and (value_1, value_2) not in matches:
                        matches.setdefault((value_1, value_2), []).append(jaccard_weighted(docs[value_1], docs[value_2], token_weights)) #docs[value_1] - shingle numbers
    return matches

def main(df, parameters):
    docs = df[parameters.matching_attribute] #which column to use for minhash
    start_time = time.time()
    print("Started creating hashes...")
    hash_weights_dict, hashes_set, hashes_dict = create_hashes(docs, parameters.hash_type)
    print("Creating hashes took --- %s seconds ---" % (time.time() - start_time))

    start_time = time.time()
    print("Started converting docs to hashes...")
    docs_hashed, hash_weights_list = create_doc_tokens(docs, parameters.hash_type, hash_weights_dict, hashes_dict)
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
    matches = create_matches(buckets_of_bands, docs_hashed, hash_weights_list)
    print("Creating matches (jaccard) took --- %s seconds ---" % (time.time() - start_time))

    print(matches)
    return matches


if __name__ == "__main__":
    datasets_size = [1000000]

    ps1 = attribute_match_params('name_clean', 'tokens':, 1)
#    print(ps1.matching_attribute, ps1.split_method, ps1.weights_method, ps1.shingle_size, ps1.signature_size)

    start_time = time.time()
    df = df_imports.df_import()
    print("Importing datasets took --- %s seconds ---" % (time.time() - start_time))

    for n in datasets_size:
        start_time = time.time()
        print('Started working with the dataset (%s size):' % n)
        matches = main(df.head(n), ps1)
        print("Whole algorithm took --- %s seconds ---" % (time.time() - start_time))



        df_matches_full = create_df_with_attributes(matches, df)

        df_matches_full.to_csv("df_matches_full_{}_{}.csv".format(n, datetime.date))
        time_spent.append(a)
        df_matches_outputs.append(df_matches_full)
        print("Whole algorithm took --- %s seconds ---" % (time.time() - start_time))


    for a, n in zip(time_spent, datasets_size):
        print('for {} dataset size it took {} seconds'.format(n, a))
        print('end')
