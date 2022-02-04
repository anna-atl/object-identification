import pandas as pd
import math
import numpy as np
import string
import collections
from functools import reduce
import random
import time
from sklearn import preprocessing

class hashes_parameters:
    def __init__(self, r1, r2, b1, b2, c):
        self.r1 = r1
        self.r2 = r2
        self.b1 = b1
        self.b2 = b2
        self.c = c

def create_signatures_array(docs_shingled, buckets_type, signature_size, all_shingles_weights, all_shingles_weights_only_weight, shingles_weights_in_docs_dict):
    signatures = np.zeros((signature_size, len(docs_shingled)), dtype=tuple) #create a df with # rows = signature_size and #columns = docs
    shingles_shuffled = [i for i in range(len(all_shingles_weights))]  #create list of shingles indexes for further randomizing

    random_numbers_of_shingles = [[] for i in range(len(all_shingles_weights))]

    hash_weights_random = [i for i in range(len(all_shingles_weights))]

    for signature in signatures:   #iterating through rows of the signatures array
        if buckets_type == 'minhash':
            random.shuffle(shingles_shuffled)
            for doc_index, doc_shingled in enumerate(docs_shingled):
                try:
                    doc_a = [shingles_shuffled[i] for i in doc_shingled] # --check this-- recreating shingles list of a doc with randomization
                except:
                    print(doc_shingled, shingles_shuffled)
                try:
                    signature[doc_index] = min(doc_a) #saving the smallest number for this randomization for this signature
                except:
                    print('didnt work for docs_shingled {}'.format(docs_shingled[doc_index]))

        elif buckets_type == 'cws':
            overall_shingles_max_weight = max([max(v) for k, v in all_shingles_weights_only_weight.items()])
            for shingle in all_shingles_weights.keys(): #creating random numbers for all shingles for this permutation
                shingle_max_weight = max([max(i[1]) for i in all_shingles_weights[shingle]])
                random_numbers_of_shingles[shingle] = random.sample(range(0, overall_shingles_max_weight), shingle_max_weight) #this is [vk(x), vk(x)...], k the same, x changes
            for doc_index, shingles_in_doc in enumerate(docs_shingled):
                minnumber = 1000000
                for shingle in shingles_in_doc:
                    min_shingle_weight_in_doc = min(shingles_weights_in_docs_dict[doc_index][shingle])
                    min_shingle_random_number = min(random_numbers_of_shingles[shingle][:min_shingle_weight_in_doc])
                    if min_shingle_random_number < minnumber:
                        minnumber = min_shingle_random_number
                        minshingle = shingle
                signature[doc_index] = (minshingle, minnumber)

        elif buckets_type == 'i2cws':
            for hash_index, hash_weight in enumerate(all_shingles_weights):
                r1 = random.gammavariate(2, 1)
                r2 = random.gammavariate(2, 1)
                b1 = random.uniform(0, 1)
                b2 = random.uniform(0, 1)
                c = random.gammavariate(2, 1)
                hash_weights_random[hash_index] = hashes_parameters(r1, r2, b1, b2, c) #class here is ok
            for doc_index, doc_hashed in enumerate(docs_shingled):  # for iterating over indexes in list as well
                minvalue = 1000000
                for hash_position, hash_index in enumerate(reversed(doc_hashed)):
                    hash_in_doc_weight = shingles_weights_in_docs[hash_index]
                    lny2 = hash_weights_random[hash_index].r2*(math.floor(math.log(hash_in_doc_weight)/hash_weights_random[hash_index].r2 + hash_weights_random[hash_index].b2) - hash_weights_random[hash_index].b2)
                    z2 = math.exp(lny2) * math.exp(hash_weights_random[hash_index].r2)
                    a = hash_weights_random[hash_index].c / z2
                    if a < minvalue:
                        minvalue = a #a - min random number for this shingle
                        minindex = hash_index
                        minposition = hash_position
                k = minindex
                #for the I2CWS:
                hash_in_doc_weight = (minposition + 1) / len(doc_hashed) * shingles_weights_dict[k] #this won't be needed
                hash_in_doc_weight = round(hash_in_doc_weight, 0) #no rounding
                t1 = math.floor(math.log(hash_in_doc_weight) / hash_weights_random[k].r1 + hash_weights_random[k].b1)
                signature[doc_index] = (k, t1)

    return signatures

def create_buckets(signatures, bands_number, docs_mapping):
    r = int(len(signatures)/bands_number) #number of rows per band
    buckets_of_bands = [{} for i in range(bands_number)] #create a list of dict which is number of buckets
    #buckets are going to be created (buckets - potential similar items)

    for band, buckets_of_band in zip(range(bands_number), buckets_of_bands): #buckets - dict:1,2,3,4,5. key - one bucket
        for doc_index in range(len(signatures[0])): #iterating through docs
            buckets_of_band.setdefault(tuple(signatures[band*r:band*r+r, doc_index]), []).append(docs_mapping[doc_index])
            #setdefault(key, value) creates a key if it doesnt exist with the value (here - list)
            #if the signatures of this bucket are same then key is the signature and values: doc index
            #if unique signature then only one doc_index will be as a value

        filtered = {key: item for key, item in buckets_of_band.items() if len(item) > 1} #filter out where one value (one doc with this signature)
        buckets_of_band.clear()
        buckets_of_band.update(filtered)

    return buckets_of_bands

def main(docs_shingled, all_shingles_weights, all_shingles_weights_only_weight, shingles_weights_in_docs_dict, buckets_type, signature_size, bands_number, docs_mapping):
    start_time = time.time()
    print("----Started creating signatures...")
    signatures = create_signatures_array(docs_shingled, buckets_type, signature_size,
                                         all_shingles_weights, all_shingles_weights_only_weight, shingles_weights_in_docs_dict)
    signatures_creation_time = round(time.time() - start_time, 6)
    print("----//Creating signatures took --- %s seconds ---" % (signatures_creation_time))

    start_time = time.time()
    print("----Started creating buckets of potential matches...")
    buckets_of_bands = create_buckets(signatures, bands_number, docs_mapping)
    buckets_creation_time = round(time.time() - start_time, 6)
    print("----//Creating buckets took --- %s seconds ---" % (buckets_creation_time))
    del signatures

    return buckets_of_bands
