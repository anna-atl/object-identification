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
def create_hashes(docs, hash_type, shingle_size, shingle_weight):
    '''
    This function creates a list and dict of shingles
    :param texts:one string column of a df (comp names), which is going to be divided in shingles
    :param k: shingle size
    :return: list of all shingles with order
    dict of shingles, shingle as a key, index of the shingle in the list as a value
    '''
    hashes_set = set()#hash - hashed token or shingle
    shingles_weights_dict = {} #token - unique word, value - tokens index
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
            if shingle_weight == 'weighted':
                shingles_weights_dict.setdefault(word, []).append(word_index + 1)
    if shingle_weight == 'weighted':
        avg = {key: sum(value)/len(value)/len(value) for key, value in shingles_weights_dict.items()}
        shingles_weights_dict.update(avg)
    elif shingle_weight == 'weighted':
        avg = {key: 1/len(value) for key, value in shingles_weights_dict.items()}
        shingles_weights_dict.update(avg)

    hashes_dict = dict(zip(hashes_set, range(len(hashes_set))))
    hashes_list = [k for k in hashes_dict.keys()]

    shingles_weights_list = [shingles_weights_dict[hash] for hash in hashes_list]

    #put it in diff function
    shingles_weights_list_normalized = preprocessing.normalize([shingles_weights_list])
    shingles_weights_list_normalized = np.round(shingles_weights_list_normalized * 1000, 0)
    shingles_weights_list_normalized = shingles_weights_list_normalized.astype(int)
    shingles_weights_list_normalized = shingles_weights_list_normalized[0].tolist()
    shingles_weights_list_normalized = [i + 1 for i in shingles_weights_list_normalized] #fix it, this is for not creating 0 random values

    return shingles_weights_list_normalized, hashes_dict

#converting docs to shingles
def convert_docs_to_hashes(docs, hash_type, shingle_size, shingle_weight, shingles_weights_list, hashes_dict):
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
        if shingle_weight == 'weighted':
            for hash_position, hash_index in enumerate(reversed(doc_shingled)):
                hash_in_doc_weight = (hash_position + 1)/len(doc_shingled) * shingles_weights_list[hash_index] #check if list is already created
                shingles_weights_in_doc.setdefault(hash_index, []).append(hash_in_doc_weight)
            shingles_weights_in_doc = {k: sum(v) for k, v in shingles_weights_in_doc.items()}

    return docs_shingled, shingles_weights_in_docs

def main(docs, hash_type, shingle_size, shingle_weight):
    start_time = time.time()
    print("Started creating shingles...")
    shingles_weights_list, hashes_dict = create_hashes(docs, hash_type, shingle_size, shingle_weight)
    print("Creating shingles took --- %s seconds ---" % (time.time() - start_time))

    start_time = time.time()
    print("Started converting docs to hashes...")
    docs_shingled, shingles_weights_in_docs = convert_docs_to_hashes(docs, hash_type, shingle_size, shingle_weight, shingles_weights_list, hashes_dict)
    print("Converting docs to hashes took --- %s seconds ---" % (time.time() - start_time))

    return docs_shingled, shingles_weights_in_docs, shingles_weights_list