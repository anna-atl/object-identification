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

#creating signatures array
def create_signatures_array(docs_hashed, buckets_type, signature_size, hash_weights_list, shingles_weights_in_docs):
    signatures = np.zeros((signature_size, len(docs_hashed)), dtype=tuple) #create a df with # rows = signature_size and #columns = docs
    hashes_shuffled = [i for i in range(len(hash_weights_list))]  #create list of hashes indexes for further randomizing

    hashes_randomized = [[] for i in range(len(hash_weights_list))]
    hash_weights_random = [i for i in range(len(hash_weights_list))]

    for signature in signatures:   #iterating through rows of the signatures df
        if buckets_type == 'minhash':
            random.shuffle(hashes_shuffled)
            for doc_index, doc_hashed in enumerate(docs_hashed): #for iterating over indexes in list as well
                doc_a = [hashes_shuffled[i] for i in doc_hashed] # --check this-- recreating shingles list of a doc with randomization
                try:
                    signature[doc_index] = min(doc_a) #saving the smallest number for this randomization for this signature
                except:
                    print('didnt work for docs_hashed {}'.format(docs_hashed[doc_index]))
        elif buckets_type == 'weighted minhash 1':
            #print(max(hash_weights_list))
            #print(min(hash_weights_list))
            for hash_index, hash_randomized in enumerate(hashes_randomized):
                randomhash = random.sample(range(0, 1000), hash_weights_list[hash_index]+1) #this is [vk(x), vk(x)...], k the same, x changes
                hashes_randomized[hash_index] = randomhash
            for doc_index, doc_hashed in enumerate(docs_hashed):
                minvalue = 1000000
                shingles_weights_in_doc = shingles_weights_in_docs[doc_index]
                for hash_position, hash_index in enumerate(doc_hashed):
                    #print(hash_index)
                    doc_a = hashes_randomized[hash_index]
                    #print(doc_a)
                    hash_in_doc_weight = shingles_weights_in_doc[hash_index]
                    hash_in_doc_weight = np.round(hash_in_doc_weight[0], 0) #[0] hard coded, should be fixed
                    hash_in_doc_weight = hash_in_doc_weight.astype(int)
                    doc_a = doc_a[:hash_in_doc_weight+1]
                    #print(shingles_weights_in_doc[hash_index])
                    #print(doc_a)
                    a = min(doc_a)
                    #print(a)
                    if a < minvalue:
                        minvalue = a
                        minindex = hash_index
                    #print(minvalue)
                    #print(minindex)
                #print((minindex, minvalue))
                signature[doc_index] = (minindex, minvalue)
        elif buckets_type == 'weighted minhash 2':
            for hash_index, hash_weight in enumerate(hash_weights_list):
                r1 = random.gammavariate(2, 1)
                r2 = random.gammavariate(2, 1)
                b1 = random.uniform(0, 1)
                b2 = random.uniform(0, 1)
                c = random.gammavariate(2, 1)
                hash_weights_random[hash_index] = hashes_parameters(r1, r2, b1, b2, c) #class here is ok
            for doc_index, doc_hashed in enumerate(docs_hashed):  # for iterating over indexes in list as well
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
                hash_in_doc_weight = (minposition + 1) / len(doc_hashed) * hash_weights_list[k] #this won't be needed
                hash_in_doc_weight = round(hash_in_doc_weight, 0) #no rounding
                t1 = math.floor(math.log(hash_in_doc_weight) / hash_weights_random[k].r1 + hash_weights_random[k].b1)
                signature[doc_index] = (k, t1)

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

def main(docs_shingled, hash_weights_list, shingles_weights_in_docs, buckets_type, signature_size, bands_number):
    start_time = time.time()
    print("Started creating signatures...")
    signatures = create_signatures_array(docs_shingled, buckets_type, signature_size,
                                         hash_weights_list, shingles_weights_in_docs)
    signatures_creation_time = round(time.time() - start_time, 6)
    print("Creating signatures took --- %s seconds ---" % (signatures_creation_time))

    start_time = time.time()
    print("Started creating buckets of potential matches...")
    buckets_of_bands = create_buckets(signatures, bands_number)
    buckets_creation_time = round(time.time() - start_time, 6)
    print("Creating buckets took --- %s seconds ---" % (buckets_creation_time))

    return buckets_of_bands
