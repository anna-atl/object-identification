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

def create_normalized_shingles_weights(shingles_weights_dict, shingles_dict):
    shingles_weights_list = [shingles_weights_dict[shingle] for shingle in shingles_dict]
    try:
        shingles_weights_list_normalized = preprocessing.normalize([shingles_weights_list])
        shingles_weights_list_normalized = np.round(shingles_weights_list_normalized * 1000, 0)
        shingles_weights_list_normalized = shingles_weights_list_normalized.astype(int)
        shingles_weights_list_normalized = shingles_weights_list_normalized[0].tolist()
        shingles_weights_list_normalized = [i + 1 for i in shingles_weights_list_normalized] #fix it, this is for not creating 0 random values
    except:
        shingles_weights_list_normalized = []

    shingles_weights_dict_normalized = {v: shingles_weights_list_normalized[v] for k, v in shingles_dict.items()}

    return shingles_weights_dict_normalized

def create_shingles_dict(docs, shingle_type, shingle_size, shingle_weight):
    '''
    This function creates a list and dict of shingles
    :param texts:one string column of a df (comp names), which is going to be divided in shingles
    :param k: shingle size
    :return: list of all shingles with order
    dict of shingles, shingle as a key, index of the shingle in the list as a value
    '''
    shingles_set = set()
    shingles_weights_dict = {} #key - shingle, value - shingle weight
    k = shingle_size

    for doc in docs:
        if shingle_type == 'token':
            shingles = [word for word in doc.split()]
        elif shingle_type == 'shingle':
            shingles = create_shingles(doc, k)
        elif shingle_type == 'shingle words':
            words = [word for word in doc.split()]
            shingles = []
            for word in words:
                shingles_of_word = create_shingles(word, k)
                shingles.extend(shingles_of_word)
        for shingles_index_from_end, shingle in enumerate(reversed(shingles)):
            shingles_set.add(shingle)
            if shingle_weight == 'weighted 1' or shingle_weight == 'weighted 2':
                shingles_weights_dict.setdefault(shingle, []).append(shingles_index_from_end + 1)

    shingles_dict = dict(zip(shingles_set, range(len(shingles_set)))) #key - shingle, value - shingle index

    if shingle_weight == 'weighted 1':
        avg = {key: sum(value)/len(value)/len(value) for key, value in shingles_weights_dict.items()}
        shingles_weights_dict.update(avg)
        shingles_weights_dict = create_normalized_shingles_weights(shingles_weights_dict, shingles_dict)
    elif shingle_weight == 'weighted 2':
        avg = {key: 1/len(value) for key, value in shingles_weights_dict.items()}
        shingles_weights_dict.update(avg)
        shingles_weights_dict = create_normalized_shingles_weights(shingles_weights_dict, shingles_dict)
    elif shingle_weight == 'normal':
        shingles_weights_dict = {v: 1 for k, v in shingles_dict.items()}

    return shingles_dict, shingles_weights_dict

def create_shingled_docs(docs, shingle_type, shingle_size, shingle_weight, shingles_weights_dict, shingles_dict):
    docs_shingled = [[] for i in range(len(docs))]
    shingles_weights_in_docs = [{} for i in range(len(docs))]
    k = shingle_size

    for doc_shingled, doc, shingles_weights_in_doc in zip(docs_shingled, docs, shingles_weights_in_docs):
        if shingle_type == 'token':
            shingles = [word for word in doc.split()]
        elif shingle_type == 'shingle':
            shingles = create_shingles(doc, k)
        elif shingle_type == 'shingle words':
            words = [word for word in doc.split()]
            shingles = []
            for word in words:
                shingles_of_word = create_shingles(word, k)
                shingles.extend(shingles_of_word)
        for shingle in shingles:
            doc_shingled.append(shingles_dict[shingle])

        # and create weights of shingles in docs - list of dictionaries, key - shingle(index), value - weight (sum the weights if its several times)
        for shingle_position_from_end, shingle_index in enumerate(reversed(doc_shingled)):
            if shingle_weight == 'weighted 1' or shingle_weight == 'weighted 2':
                shingle_in_doc_weight = (shingle_position_from_end + 1)/len(doc_shingled) * shingles_weights_dict[shingle_index] #check if list is already created
                shingles_weights_in_doc.setdefault(shingle_index, []).append(shingle_in_doc_weight)
            elif shingle_weight == 'normal':
                shingles_weights_in_doc[shingle_index] = [1]

    return docs_shingled, shingles_weights_in_docs

def main(docs, shingle_type, shingle_size, shingle_weight):
    start_time = time.time()
    print("Started creating shingles...")
    shingles_dict, shingles_weights_dict = create_shingles_dict(docs, shingle_type, shingle_size, shingle_weight)
    print("Creating shingles took --- %s seconds ---" % (time.time() - start_time))

    start_time = time.time()
    print("Started converting docs to shingles...")
    docs_shingled, shingles_weights_in_docs = create_shingled_docs(docs, shingle_type, shingle_size, shingle_weight, shingles_weights_dict, shingles_dict)
    print("Converting docs to shingles took --- %s seconds ---" % (time.time() - start_time))

    return docs_shingled, shingles_weights_dict, shingles_weights_in_docs