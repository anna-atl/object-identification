import pandas as pd
import time
import operator
import functools

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
    all_shingles_docs_dict = dict(zip(all_shingles.keys(), all_shingles_docs.values()))

    return docs_shingled, all_shingles, all_shingles_docs_dict

def create_weights(docs_shingled, all_shingles_docs, shingle_weight, experiment_mode):
    idf_shingles = {}
    all_shingles_weights = {}

    tf_shingles_in_docs = [{} for i in range(len(docs_shingled))]
    shingles_weights_in_docs_dict = [{} for i in range(len(docs_shingled))]

    if shingle_weight == 'tf-idf-0':
        for (doc_index, shingles_in_doc), tf_shingles_in_doc in zip(enumerate(docs_shingled), tf_shingles_in_docs):
            for shingle_in_doc in shingles_in_doc:
                tf_shingles_in_doc.setdefault(shingle_in_doc, []).append(all_shingles_docs[shingle_in_doc].count(doc_index)/len(shingles_in_doc))
                idf_shingles[shingle_in_doc] = len(docs_shingled) / all_shingles_docs[shingle_in_doc].count(doc_index)

                shingle_weight_in_doc = functools.reduce(operator.mul, tf_shingles_in_docs[doc_index][shingle_in_doc], idf_shingles[shingle_in_doc])
                shingles_weights_in_docs_dict[doc_index].setdefault(shingle_in_doc, []).append(shingle_weight_in_doc)
                all_shingles_weights.setdefault(shingle_in_doc, []).append((doc_index, shingles_weights_in_docs_dict[doc_index][shingle_in_doc]))
    elif shingle_weight == 'tf-idf-cp-0':
        for (doc_index, shingles_in_doc), tf_shingles_in_doc in zip(enumerate(docs_shingled), tf_shingles_in_docs):
            for shingle_in_doc in shingles_in_doc:
                tf_shingles_in_doc.setdefault(shingle_in_doc, []).append(all_shingles_docs[shingle_in_doc].count(doc_index)/len(shingles_in_doc))
                idf_shingles[shingle_in_doc] = len(docs_shingled) / all_shingles_docs[shingle_in_doc].count(doc_index)

                shingle_weight_in_doc = functools.reduce(operator.mul, tf_shingles_in_docs[doc_index][shingle_in_doc], idf_shingles[shingle_in_doc])
                shingles_weights_in_docs_dict[doc_index].setdefault(shingle_in_doc, []).append(shingle_weight_in_doc)
                all_shingles_weights.setdefault(shingle_in_doc, []).append((doc_index, shingles_weights_in_docs_dict[doc_index][shingle_in_doc]))
                #CP SHOULD BE ADDED HERE
    if experiment_mode == 'test':
        df_shingles_weights = pd.DataFrame.from_dict(all_shingles_weights, orient='index') #to see all shingles weights in docs overall

    return shingles_weights_in_docs_dict, all_shingles_weights

def main(docs, shingle_type, shingle_size, shingle_weight, experiment_mode):
    start_time = time.time()
    print("----Started converting docs to shingles...")
    docs_shingled, all_shingles, all_shingles_docs = create_shingled_docs(docs, shingle_type, shingle_size, shingle_weight, experiment_mode)
    print("----//Converting docs to shingles took --- %s seconds ---" % (time.time() - start_time))

    if shingle_weight != "none":
        print("----Started calculating weights of shingles...")
        shingles_weights_in_docs_dict, all_shingles_weights = create_weights(docs_shingled, all_shingles_docs, shingle_weight, experiment_mode)
        print("----//Calculating weights of shingles took --- %s seconds ---" % (time.time() - start_time))

    return docs_shingled, all_shingles_weights, shingles_weights_in_docs_dict #all_shingled_weights: keys - shingles, values - all the weights, shingles_weigths_in_docs = [[weight of shingle in doc,],]