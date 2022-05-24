import pandas as pd
import time
import operator
import functools
import math

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
    all_shingles_docs_dict = dict(zip(all_shingles.keys(), all_shingles_docs.values())) #key - shingle overall index, value - doc indexes

    return docs_shingled, all_shingles, all_shingles_docs_dict

def create_weights(docs_shingled, all_shingles_docs_dict, shingle_weight, experiment_mode, all_shingles_weights, all_shingles_weights_only_weight, all_shingles):
    shingles_weights_in_docs_dict = [{} for i in range(len(docs_shingled))]
    #test_mode
    all_shingles_weights_only_weight_test = {}
    if shingle_weight == 'tf-0':
        for (doc_index, shingles_in_doc), shingles_weights_in_doc_dict in zip(enumerate(docs_shingled), shingles_weights_in_docs_dict):
            shingle_counts = {}
            for shingle_in_doc in shingles_in_doc:
                shingle_counts.setdefault(shingle_in_doc, 0) #dict for each doc. key - shingle index, value - number of shingle in the doc
                shingle_counts[shingle_in_doc] += 1  # can be used for cp, instead of +=1 can be the position
            for shingle_in_doc in shingle_counts:
                tf_shingle_in_doc = shingle_counts[shingle_in_doc] / len(shingles_in_doc)
                shingle_weight_in_doc = int(round(tf_shingle_in_doc *100, 0))
                shingles_weights_in_docs_dict[doc_index][shingle_in_doc] = shingle_weight_in_doc #each doc has dict. key - shingle index, value - shingle's weight in doc

                all_shingles_weights.setdefault(shingle_in_doc, []).append((doc_index, shingles_weights_in_doc_dict[shingle_in_doc])) #key - shingle, value - all weights of the shingle
                all_shingles_weights_only_weight.setdefault(shingle_in_doc, []).append(shingle_weight_in_doc)
                if experiment_mode == 'test':
                    all_shingles_weights_only_weight_test.setdefault(all_shingles[shingle_in_doc], []).append(
                        shingles_weights_in_docs_dict[doc_index][shingle_in_doc])

    elif shingle_weight == 'idf-0':
        idf_shingles = {}
        for shingle_in_doc in all_shingles_docs_dict:
            idf_shingles[shingle_in_doc] = len(docs_shingled) / len(set(all_shingles_docs_dict[shingle_in_doc]))
        for (doc_index, shingles_in_doc), shingles_weights_in_doc_dict in zip(enumerate(docs_shingled), shingles_weights_in_docs_dict):
            shingle_counts = {}
            for shingle_in_doc in shingles_in_doc:
                shingle_weight_in_doc = int(round(idf_shingles[shingle_in_doc], 0))
                shingles_weights_in_docs_dict[doc_index][shingle_in_doc] = shingle_weight_in_doc #each doc has dict. key - shingle index, value - shingle's weight in doc

                all_shingles_weights.setdefault(shingle_in_doc, []).append((doc_index, shingles_weights_in_doc_dict[shingle_in_doc])) #key - shingle, value - all weights of the shingle
                all_shingles_weights_only_weight.setdefault(shingle_in_doc, []).append(shingle_weight_in_doc)
                if experiment_mode == 'test':
                    all_shingles_weights_only_weight_test.setdefault(all_shingles[shingle_in_doc], []).append(
                        shingles_weights_in_docs_dict[doc_index][shingle_in_doc])

    elif shingle_weight == 'idf-log':
        idf_shingles = {}
        for shingle_in_doc in all_shingles_docs_dict:
            idf_shingles[shingle_in_doc] = math.log(len(docs_shingled) / len(set(all_shingles_docs_dict[shingle_in_doc])))
        for (doc_index, shingles_in_doc), shingles_weights_in_doc_dict in zip(enumerate(docs_shingled), shingles_weights_in_docs_dict):
            shingle_counts = {}
            for shingle_in_doc in shingles_in_doc:
                shingle_weight_in_doc = int(round(idf_shingles[shingle_in_doc]*100, 0))
                shingles_weights_in_docs_dict[doc_index][shingle_in_doc] = shingle_weight_in_doc #each doc has dict. key - shingle index, value - shingle's weight in doc

                all_shingles_weights.setdefault(shingle_in_doc, []).append((doc_index, shingles_weights_in_doc_dict[shingle_in_doc])) #key - shingle, value - all weights of the shingle
                all_shingles_weights_only_weight.setdefault(shingle_in_doc, []).append(shingle_weight_in_doc)
                if experiment_mode == 'test':
                    all_shingles_weights_only_weight_test.setdefault(all_shingles[shingle_in_doc], []).append(
                        shingles_weights_in_docs_dict[doc_index][shingle_in_doc])

    elif shingle_weight == 'tf-idf-0':
        idf_shingles = {}
        for shingle_in_doc in all_shingles_docs_dict:
            idf_shingles[shingle_in_doc] = len(docs_shingled) / len(set(all_shingles_docs_dict[shingle_in_doc]))
        for (doc_index, shingles_in_doc), shingles_weights_in_doc_dict in zip(enumerate(docs_shingled), shingles_weights_in_docs_dict):
            shingle_counts = {}
            for shingle_in_doc in shingles_in_doc:
                shingle_counts.setdefault(shingle_in_doc, 0) #dict for each doc. key - shingle index, value - number of shingle in the doc
                shingle_counts[shingle_in_doc] += 1  # can be used for cp, instead of +=1 can be the position
            for shingle_in_doc in shingle_counts:
                tf_shingle_in_doc = shingle_counts[shingle_in_doc] / len(shingles_in_doc)
                shingle_weight_in_doc = int(round(tf_shingle_in_doc * idf_shingles[shingle_in_doc]*1000, 0))
                shingles_weights_in_docs_dict[doc_index][shingle_in_doc] = shingle_weight_in_doc #each doc has dict. key - shingle index, value - shingle's weight in doc

                all_shingles_weights.setdefault(shingle_in_doc, []).append((doc_index, shingles_weights_in_doc_dict[shingle_in_doc])) #key - shingle, value - all weights of the shingle
                all_shingles_weights_only_weight.setdefault(shingle_in_doc, []).append(shingle_weight_in_doc)
                if experiment_mode == 'test':
                    all_shingles_weights_only_weight_test.setdefault(all_shingles[shingle_in_doc], []).append(
                        shingles_weights_in_docs_dict[doc_index][shingle_in_doc])

    elif shingle_weight == 'tf-0-idf-log':
        idf_shingles = {}
        for shingle_in_doc in all_shingles_docs_dict:
            idf_shingles[shingle_in_doc] = math.log(len(docs_shingled) / len(set(all_shingles_docs_dict[shingle_in_doc])))
        for (doc_index, shingles_in_doc), shingles_weights_in_doc_dict in zip(enumerate(docs_shingled), shingles_weights_in_docs_dict):
            shingle_counts = {}
            for shingle_in_doc in shingles_in_doc:
                shingle_counts.setdefault(shingle_in_doc, 0) #dict for each doc. key - shingle index, value - number of shingle in the doc
                shingle_counts[shingle_in_doc] += 1  # can be used for cp, instead of +=1 can be the position
            for shingle_in_doc in shingle_counts:
                tf_shingle_in_doc = shingle_counts[shingle_in_doc] / len(shingles_in_doc)
                shingle_weight_in_doc = int(round(tf_shingle_in_doc * idf_shingles[shingle_in_doc]*1000, 0))
                shingles_weights_in_docs_dict[doc_index][shingle_in_doc] = shingle_weight_in_doc #each doc has dict. key - shingle index, value - shingle's weight in doc

                all_shingles_weights.setdefault(shingle_in_doc, []).append((doc_index, shingles_weights_in_doc_dict[shingle_in_doc])) #key - shingle, value - all weights of the shingle
                all_shingles_weights_only_weight.setdefault(shingle_in_doc, []).append(shingle_weight_in_doc)
                if experiment_mode == 'test':
                    all_shingles_weights_only_weight_test.setdefault(all_shingles[shingle_in_doc], []).append(
                        shingles_weights_in_docs_dict[doc_index][shingle_in_doc])

    elif shingle_weight == 'tf-idf-cp-0':
        idf_shingles = {}
        shingle_positions = {}
        shingle_positions_from_end = {}
        for shingle_in_doc, doc_index in all_shingles_docs_dict.items():
            idf_shingles[shingle_in_doc] = len(docs_shingled) / len(set(all_shingles_docs_dict[shingle_in_doc]))

        for doc_index, shingles_in_doc in enumerate(docs_shingled):
            shingle_counts = {}
            for shingle_position, shingle_in_doc in enumerate(shingles_in_doc):
                shingle_counts.setdefault(shingle_in_doc, 0) #dict for each doc. key - shingle index, value - number of shingles in the doc
                shingle_counts[shingle_in_doc] += 1  # can be used for cp
                shingle_positions.setdefault(shingle_in_doc, shingle_position)
                shingle_positions_from_end.setdefault(shingle_in_doc, len(shingles_in_doc) - shingle_position)

                for shingle in shingle_counts:
                    common_shingle_position = statistics.median(shingle_positions[shingle])
                    common_shingle_position_from_end = statistics.median(shingle_positions_from_end[shingle])

                    cp_shingles = min(abs(shingle_positions[shingle] - common_shingle_position) + 1, abs(shingle_positions_from_end[shingle] - common_shingle_position_from_end) + 1)

                    tf_shingle_in_doc = shingle_counts[shingle_in_doc] / len(shingles_in_doc)

                    shingle_weight_in_doc = tf_shingle_in_doc * idf_shingles[shingle_in_doc] * cp_shingles
                    shingles_weights_in_docs_dict[doc_index][shingle_in_doc] = shingle_weight_in_doc #each doc has dict. key - shingle index, value - shingle's weight in doc

                    all_shingles_weights.setdefault(shingle_in_doc, []).append((doc_index, shingles_weights_in_docs_dict[doc_index][shingle_in_doc])) #key - shingle, value - all weights of the shingle
                    if experiment_mode == 'test':
                        all_shingles_weights_only_weight_test.setdefault(all_shingles[shingle_in_doc], []).append(
                            shingles_weights_in_docs_dict[doc_index][shingle_in_doc])

    if experiment_mode == 'test':
        all_shingles_weights_max_weights = {}
        all_shingles_weights_min_weights = {}
        for shingle in all_shingles_weights_only_weight_test:
            all_shingles_weights_max_weights.setdefault(shingle, max(all_shingles_weights_only_weight_test[shingle]))
            all_shingles_weights_min_weights.setdefault(shingle, min(all_shingles_weights_only_weight_test[shingle]))

        df_shingles_weights_min = pd.DataFrame.from_dict(all_shingles_weights_min_weights,
                                                     orient='index')  # to see all shingles weights in docs overall
        df_shingles_weights_max = pd.DataFrame.from_dict(all_shingles_weights_max_weights,
                                                     orient='index')  # to see all shingles weights in docs overall
        df_shingles_weights_min = df_shingles_weights_min.sort_values(by=0, ascending=True)
        df_shingles_weights_max = df_shingles_weights_max.sort_values(by=0, ascending=False)

    return shingles_weights_in_docs_dict, all_shingles_weights, all_shingles_weights_only_weight

def main(docs, shingle_type, shingle_size, shingle_weight, experiment_mode):
    start_time = time.time()

    print("----Started converting docs to shingles...")
    docs_shingled, all_shingles, all_shingles_docs_dict = create_shingled_docs(docs, shingle_type, shingle_size, shingle_weight, experiment_mode)
    print("----//Converting docs to shingles took --- %s seconds ---" % (time.time() - start_time))

    all_shingles_weights = {}
    all_shingles_weights_only_weight = {}
    if shingle_weight != "none":
        print("----Started calculating weights of shingles...")
        shingles_weights_in_docs_dict, all_shingles_weights, all_shingles_weights_only_weight = create_weights(docs_shingled, all_shingles_docs_dict, shingle_weight, experiment_mode, all_shingles_weights, all_shingles_weights_only_weight, all_shingles)
        print("----//Calculating weights of shingles took --- %s seconds ---" % (time.time() - start_time))
    else:
        for doc_index, shingles_in_doc in enumerate(docs_shingled):
            for shingle_in_doc in shingles_in_doc:
                all_shingles_weights.setdefault(shingle_in_doc, []).append((doc_index, 1))
        shingles_weights_in_docs_dict = {}

    return docs_shingled, all_shingles_weights, all_shingles_weights_only_weight, shingles_weights_in_docs_dict #all_shingled_weights: keys - shingles, values - all the weights, shingles_weigths_in_docs = [[weight of shingle in doc,],]