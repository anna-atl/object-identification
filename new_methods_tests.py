
#converting docs to shingles and with frequency
def create_doc_shingles_test(texts,k,shingles_dict):
    '''
    :param texts:
    :param k:
    :param shingles_dict:
    :return:
    '''
    start_time = time.time()
    print("Started creating shingles for docs...")
    docs = [[] for i in range(len(texts))]
    shingles_frequency = [0] * len(shingles_dict)
    texts = [x.strip(' ') for x in texts] #remove spaces

    for doc, text in zip(docs, texts):
        if len(text) >= k:
            shingles = [text[i:i + k] for i in range(len(text) - k + 1)]
        else:
            shingles = [text + ' ' * (k - len(text))]
        for shingle in shingles:
            doc.append(shingles_dict[shingle]) #doc - list of shingles(numbers)
            shingles_frequency[shingles_dict[shingle]] += 1

    print("Converting docs to shingles took --- %s seconds ---" % (time.time() - start_time))
    return docs, shingles_frequency

def create_shingles_weights(shingles_frequency):
    shingles_weights = [1/shingle_frequency for shingle_frequency in shingles_frequency]
    #keeping floats because python floats are faster than int
    return shingles_weights

def jaccard_sum(list1, list2, shingles_weights):
    intersection = set(list1).intersection(list2)
    union = set(list1 + list2)
    return sum([shingles_weights[i] for i in intersection])/sum([shingles_weights[i] for i in union])


def create_matches_sum(buckets_bands, docs, shingles_weights):
    start_time = time.time()
    print("Started creating matches...")
    matches = {} #keys - tuple of duplicate docs and values - jacc of docs(lists of shingles(numbers))
    for buckets in buckets_bands:
        for key, values_list in buckets.items(): #values_list - doc indexes in one buckets
            for value_1 in values_list: #iterating through doc_indexes
                for value_2 in values_list:
                    if value_2 > value_1 and (value_1, value_2) not in matches:
                        matches.setdefault((value_1, value_2), []).append(jaccard_sum(docs[value_1], docs[value_2], shingles_weights)) #docs[value_1] - shingle numbers
    print("Creating matches (jaccard) took --- %s seconds ---" % (time.time() - start_time))
    return matches

docs, shingles_frequency = create_doc_shingles_test(texts, k, shingles_dict)
shingles_weights = create_shingles_weights(shingles_frequency)
matches_sum = create_matches_sum(buckets_bands, docs, shingles_weights)
df_matches_sum_full = create_df_with_attributes(matches_sum, df)
