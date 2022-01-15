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

    docs_shingled = [[] for i in range(len(docs))]

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
        docs_shingled[doc_index] = shingles_in_doc #every doc as a list of shingle

        for shingle_index_in_doc, shingle_in_doc in enumerate(shingles_in_doc):
            all_shingles_docs.setdefault(shingle_in_doc, []).append(doc_index) # key - shingle, value - doc index

    #shingle overall index - shingle index through all documents, shingle index/position - shingle position/index in the doc
    shingles_dict = dict(zip(all_shingles_docs.keys(), range(len(all_shingles_docs))))  # key - shingle, value - shingle overall index
    all_shingles = dict(zip(range(len(all_shingles_docs)), all_shingles_docs.keys()))  # key - shingle overall index, value - shingle
    docs_shingled = [[shingles_dict[shingle] for shingle in doc_shingled] for doc_shingled in docs_shingled] #every doc as a list of shingle overall indexes

    return docs_shingled, all_shingles, all_shingles_docs

def create_weights(docs_shingled, all_shingles_docs, shingle_weight, experiment_mode):
    all_shingles_positions_in_docs = {}
    all_shingles_positions_in_docs_from_end = {}
    common_shingle_position = {}
    common_shingle_position_from_end = {}
    idf_shingles = {}
    all_shingles_weights = {}

    shingles_positions_in_docs = [[] for i in range(len(docs_shingled))]
    shingles_positions_in_docs_from_end = [[] for i in range(len(docs_shingled))]
    shingles_frequency_in_docs = [[] for i in range(len(docs_shingled))]
    tf_shingles_in_docs = [[] for i in range(len(docs_shingled))]
    cp_shingles_in_docs = [[] for i in range(len(docs_shingled))]
    cp_shingles_in_docs_from_end = [[] for i in range(len(docs_shingled))]
    cp_shingles_in_docs_final = [[] for i in range(len(docs_shingled))]
    shingles_weights_in_docs = [[] for i in range(len(docs_shingled))]

    shingles_weights_in_docs_dict = [{} for i in range(len(docs_shingled))]




    if shingle_weight == 'tf-idf-cp-0':
        for doc_index, shingles_in_doc in enumerate(docs_shingled):
            shingles_positions_in_docs[doc_index] = list(range(len(shingles_in_doc)))
            shingles_positions_in_docs_from_end[doc_index] = list(range(len(shingles_in_doc)))
            for shingle_index_in_doc, shingle_in_doc in enumerate(shingles_in_doc):

                shingles_positions_in_docs[doc_index][shingle_index_in_doc] = shingle_index_in_doc
                shingles_positions_in_docs_from_end[doc_index][shingle_index_in_doc] = len(shingles_in_doc) - shingle_index_in_doc
                all_shingles_positions_in_docs.setdefault(shingle_in_doc, []).append(shingle_index_in_doc)
                all_shingles_positions_in_docs_from_end.setdefault(shingle_in_doc, []).append(len(shingles_in_doc) - shingle_index_in_doc)
        total_number_of_docs = len(docs_shingled)

        for shingle, docs_with_this_shingle in all_shingles_docs.items():
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
                shingles_frequency_in_docs[doc_index][shingle_index_in_doc] = all_shingles_docs[shingle_in_doc].count(doc_index)

                tf_shingles_in_docs[doc_index][shingle_index_in_doc] = shingles_frequency_in_docs[doc_index][shingle_index_in_doc]/total_number_of_shingles_in_doc

                #FIX THIS cp calculation, weight should be != 0, thats why hardcoded +1
                cp_shingles_in_docs[doc_index][shingle_index_in_doc] = abs(shingles_positions_in_docs[doc_index][shingle_index_in_doc] - common_shingle_position[shingle]) + 1
                cp_shingles_in_docs_from_end[doc_index][shingle_index_in_doc] = abs(shingles_positions_in_docs_from_end[doc_index][shingle_index_in_doc] - common_shingle_position_from_end[shingle]) + 1
                cp_shingles_in_docs_final[doc_index][shingle_index_in_doc] = min(cp_shingles_in_docs[doc_index][shingle_index_in_doc], cp_shingles_in_docs_from_end[doc_index][shingle_index_in_doc])

                shingles_weights_in_docs[doc_index][shingle_index_in_doc] = tf_shingles_in_docs[doc_index][shingle_index_in_doc] * idf_shingles[shingle_in_doc] * cp_shingles_in_docs_final[doc_index][shingle_index_in_doc]
                shingles_weights_in_docs[doc_index][shingle_index_in_doc] = np.round(shingles_weights_in_docs[doc_index][shingle_index_in_doc], 0)
                shingles_weights_in_docs[doc_index][shingle_index_in_doc] = shingles_weights_in_docs[doc_index][shingle_index_in_doc].astype(int)

                shingles_weights_in_docs_dict[doc_index].setdefault(shingle_in_doc, []).append(shingles_weights_in_docs[doc_index][shingle_index_in_doc])

                all_shingles_weights.setdefault(shingle_in_doc, []).append((doc_index, shingles_weights_in_docs[doc_index][shingle_index_in_doc]))

    if experiment_mode == 'test':
        df_shingles_weights = pd.DataFrame.from_dict(all_shingles_weights, orient='index') #to see all shingles weights in docs overall

    return shingles_weights_in_docs, shingles_weights_in_docs_dict, all_shingles_weights

def main(docs, shingle_type, shingle_size, shingle_weight, experiment_mode):
    start_time = time.time()
    print("----Started converting docs to shingles...")
    docs_shingled, all_shingles, all_shingles_docs = create_shingled_docs(docs, shingle_type, shingle_size, shingle_weight, experiment_mode)
    print("----//Converting docs to shingles took --- %s seconds ---" % (time.time() - start_time))

    if shingle_weight != "none":
        print("----Started calculating weights of shingles...")
        shingles_weights_in_docs, shingles_weights_in_docs_dict, all_shingles_weights = create_weights(docs_shingled, all_shingles_docs, shingle_weight, experiment_mode)
        print("----//Calculating weights of shingles took --- %s seconds ---" % (time.time() - start_time))

    return docs_shingled, all_shingles_weights, shingles_weights_in_docs, shingles_weights_in_docs_dict #all_shingled_weights: keys - shingles, values - all the weights, shingles_weigths_in_docs = [[weight of shingle in doc,],]