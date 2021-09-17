import pandas as pd
import numpy as np
import string
import datetime
from sklearn import preprocessing
import time

pd.set_option('display.max_columns', None)

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
        elif hash_type == 'shingle words':
            words = [word for word in doc.split()]
            hashes = []
            for word in words:
                hashes1 = create_shingles(word, k)
                hashes.extend(hashes1)
        for word_index, word in enumerate(reversed(hashes)):
            hashes_set.add(word)
            if hash_weight == 'weighted':
                hash_weights_dict.setdefault(word, []).append(word_index + 1)
    if hash_weight == 'weighted hash':
        avg = {key: sum(value)/len(value)/len(value) for key, value in hash_weights_dict.items()}
        hash_weights_dict.update(avg)
    elif hash_weight == 'weighted':
        avg = {key: 1/len(value) for key, value in hash_weights_dict.items()}
        hash_weights_dict.update(avg)

    hashes_dict = dict(zip(hashes_set, range(len(hashes_set))))
    hashes_list = [k for k in hashes_dict.keys()]

    #should change shingles in hash_weights_dict to their indexes
    #hash_weights_dict_new = {y: x for x, y in hash_weights_dict.items()} #to switch shingles and their indexes
    #hash_weights_list = [hash_weights_dict[hash] if key in hashes_dict.keys() else 0 for hash in hashes_list]
    hash_weights_list = [hash_weights_dict[hash] for hash in hashes_list]

    #put it in diff function
    hash_weights_list_normalized = preprocessing.normalize([hash_weights_list])
    hash_weights_list_normalized = np.round(hash_weights_list_normalized * 1000, 0)
    hash_weights_list_normalized = hash_weights_list_normalized.astype(int)
    hash_weights_list_normalized = hash_weights_list_normalized[0].tolist()
    hash_weights_list_normalized = [i + 1 for i in hash_weights_list_normalized] #fix it, this is for not creating 0 random values

    return hash_weights_list_normalized, hashes_dict

#converting docs to shingles
def convert_docs_to_hashes(docs, hash_type, shingle_size, hash_weight, hash_weights_list, hashes_dict):
    '''
    '''
    docs_shingled = [[] for i in range(len(docs))]
    shingles_weights_in_docs = [{} for i in range(len(docs))]
    k = shingle_size

    for doc_shingled, doc, shingles_weights_in_doc in zip(docs_shingled, docs, shingles_weights_in_docs):
        if hash_type == 'token':
            hashes = [word for word in doc.split()]
        elif hash_type == 'shingle':
            hashes = create_shingles(doc, k)
        elif hash_type == 'shingle words':
            words = [word for word in doc.split()]
            hashes = []
            for word in words:
                hashes1 = create_shingles(word, k)
                hashes.extend(hashes1)
        for word in hashes:
            doc_shingled.append(hashes_dict[word])

        # and create weights of shingles in docs - list of dictionaries, key - shingle(index), value - weight (sum the weights if its several times)
        if hash_weight == 'weighted':
            for hash_position, hash_index in enumerate(reversed(doc_shingled)):
                hash_in_doc_weight = (hash_position + 1)/len(doc_shingled) * hash_weights_list[hash_index] #check if list is already created
                shingles_weights_in_doc.setdefault(hash_index, []).append(hash_in_doc_weight)
            shingles_weights_in_doc = {k: sum(v) for k, v in shingles_weights_in_doc.items()}

    return docs_shingled, shingles_weights_in_docs

def main(docs, hash_type, shingle_size, hash_weight):
    start_time = time.time()
    print("Started creating shingles...")
    hash_weights_list, hashes_dict = create_hashes(docs, hash_type, shingle_size, hash_weight)
    print("Creating shingles took --- %s seconds ---" % (time.time() - start_time))

    start_time = time.time()
    print("Started converting docs to hashes...")
    docs_shingled, shingles_weights_in_docs = convert_docs_to_hashes(docs, hash_type, shingle_size, hash_weight, hash_weights_list, hashes_dict)
    print("Converting docs to hashes took --- %s seconds ---" % (time.time() - start_time))

    return docs_shingled, shingles_weights_in_docs, hash_weights_list