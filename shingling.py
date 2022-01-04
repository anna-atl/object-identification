import pandas as pd
import numpy as np
import string
import datetime
from sklearn import preprocessing
import time
import statistics

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

def create_shingled_docs(docs, shingle_type, shingle_size, shingle_weight, experiment_mode):
    '''
    This function creates a list and dict of shingles
    :param texts:one string column of a df (comp names), which is going to be divided in shingles
    :param k: shingle size
    :return: list of all shingles with order
    dict of shingles, shingle as a key, index of the shingle in the list as a value
    '''
    k = shingle_size
    shingles_dict = {}
    all_shingles_docs = {}
    all_shingles_and_their_docs = {}
    all_shingles_positions_in_docs = {}
    all_shingles_positions_in_docs_from_end = {}
    common_shingle_position = {}
    common_shingle_position_from_end = {}
    idf_shingles = {}
    all_shingles_weights = {}

    docs_shingled = [[] for i in range(len(docs))]
    shingles_positions_in_docs = [[] for i in range(len(docs))]
    shingles_positions_in_docs_from_end = [[] for i in range(len(docs))]
    shingles_frequency_in_docs = [[] for i in range(len(docs))]
    tf_shingles_in_docs = [[] for i in range(len(docs))]
    cp_shingles_in_docs = [[] for i in range(len(docs))]
    cp_shingles_in_docs_from_end = [[] for i in range(len(docs))]
    cp_shingles_in_docs_final = [[] for i in range(len(docs))]
    shingles_weights_in_docs = [[] for i in range(len(docs))]

    for doc_index, doc in enumerate(docs):
        if shingle_type == 'token':
            shingles_in_doc = [word for word in doc.split()]
        elif shingle_type == 'shingle':
            shingles_in_doc = create_shingles(doc, k)
        elif shingle_type == 'shingle words':
            words = [word for word in doc.split()]
            shingles_in_doc = []
            for word in words:
                shingles_in_word = create_shingles(word, k)
                shingles_in_doc.extend(shingles_in_word)
        docs_shingled[doc_index] = shingles_in_doc

        for shingle_index_in_doc, shingle_in_doc in enumerate(shingles_in_doc):
            all_shingles_docs.setdefault(shingle_in_doc, []).append(doc_index)

    shingles_dict = dict(zip(all_shingles_docs.keys(), range(len(all_shingles_docs))))  # key - shingle, value - shingle overall index
    docs_shingled = [[shingles_dict[shingle] for shingle in doc_shingled] for doc_shingled in docs_shingled]

    if shingle_weight != "none":
        for doc_index, shingles_in_doc in enumerate(docs_shingled):
            shingles_positions_in_docs[doc_index] = list(range(len(shingles_in_doc)))
            shingles_positions_in_docs_from_end[doc_index] = list(range(len(shingles_in_doc)))
            for shingle_index_in_doc, shingle_in_doc in enumerate(shingles_in_doc):
                all_shingles_and_their_docs.setdefault(shingle_in_doc, []).append(doc_index)
                shingles_positions_in_docs[doc_index][shingle_index_in_doc] = shingle_index_in_doc
                shingles_positions_in_docs_from_end[doc_index][shingle_index_in_doc] = len(shingles_in_doc) - shingle_index_in_doc
                #shingles_positions_in_doc.append(shingle_index_in_doc)
                #shingles_positions_in_doc_from_end.append(len(shingles_in_doc) - shingle_index_in_doc)
                all_shingles_positions_in_docs.setdefault(shingle_in_doc, []).append(shingle_index_in_doc)
                all_shingles_positions_in_docs_from_end.setdefault(shingle_in_doc, []).append(len(shingles_in_doc) - shingle_index_in_doc)
        total_number_of_docs = len(docs)
        for shingle, docs_with_this_shingle in all_shingles_and_their_docs.items():
            number_of_docs_with_this_shingle = len(docs_with_this_shingle)
            idf_shingles[shingle] = total_number_of_docs / number_of_docs_with_this_shingle
            common_shingle_position[shingle] = statistics.median(all_shingles_positions_in_docs[shingle])
            common_shingle_position_from_end[shingle] = statistics.median(all_shingles_positions_in_docs_from_end[shingle])

        for doc_index, shingles_in_doc in enumerate(docs_shingled):
            total_number_of_shingles_in_doc = len(shingles_in_doc)

            shingles_frequency_in_docs[doc_index] = list(range(len(shingles_in_doc)))
            tf_shingles_in_docs[doc_index] = list(range(len(shingles_in_doc)))
            cp_shingles_in_docs[doc_index] = list(range(len(shingles_in_doc)))
            cp_shingles_in_docs_from_end[doc_index] = list(range(len(shingles_in_doc)))
            cp_shingles_in_docs_final[doc_index] = list(range(len(shingles_in_doc)))
            shingles_weights_in_docs[doc_index] = list(range(len(shingles_in_doc)))

            for shingle_index_in_doc, shingle_in_doc in enumerate(shingles_in_doc):
                shingles_frequency_in_docs[doc_index][shingle_index_in_doc] = all_shingles_and_their_docs[shingle_in_doc].count(doc_index)

                tf_shingles_in_docs[doc_index][shingle_index_in_doc] = shingles_frequency_in_docs[doc_index][shingle_index_in_doc]/total_number_of_shingles_in_doc

                #FIX THIS cp calculation, weight should be != 0, thats why hardcoded +1
                cp_shingles_in_docs[doc_index][shingle_index_in_doc] = abs(shingles_positions_in_docs[doc_index][shingle_index_in_doc] - common_shingle_position[shingle]) + 1
                cp_shingles_in_docs_from_end[doc_index][shingle_index_in_doc] = abs(shingles_positions_in_docs_from_end[doc_index][shingle_index_in_doc] - common_shingle_position_from_end[shingle]) + 1
                cp_shingles_in_docs_final[doc_index][shingle_index_in_doc] = min(cp_shingles_in_docs[doc_index][shingle_index_in_doc], cp_shingles_in_docs_from_end[doc_index][shingle_index_in_doc])

                shingles_weights_in_docs[doc_index][shingle_index_in_doc] = tf_shingles_in_docs[doc_index][shingle_index_in_doc] * idf_shingles[shingle_in_doc] * cp_shingles_in_docs_final[doc_index][shingle_index_in_doc]
                shingles_weights_in_docs[doc_index][shingle_index_in_doc] = np.round(shingles_weights_in_docs[doc_index][shingle_index_in_doc], 0)
                shingles_weights_in_docs[doc_index][shingle_index_in_doc] = shingles_weights_in_docs[doc_index][shingle_index_in_doc].astype(int)

                all_shingles_weights.setdefault(shingle_in_doc, []).append((doc_index, shingles_weights_in_docs[doc_index][shingle_index_in_doc]))

    if experiment_mode == 'test':
        df_shingles_weights = pd.DataFrame.from_dict(all_shingles_weights, orient='index')

    return docs_shingled, shingles_weights_in_docs, all_shingles_weights

def main(docs, shingle_type, shingle_size, shingle_weight, experiment_mode):
    start_time = time.time()
    print("----Started converting docs to shingles...")
    docs_shingled, shingles_weights_in_docs, all_shingles_weights = create_shingled_docs(docs, shingle_type, shingle_size, shingle_weight, experiment_mode)
    print("----Converting docs to shingles took --- %s seconds ---" % (time.time() - start_time))

    return docs_shingled, all_shingles_weights, shingles_weights_in_docs #all_shingled_weights: keys - shingles, values - all the weights, shingles_weigths_in_docs = [[weight of shingle in doc,],]