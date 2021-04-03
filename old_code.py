
def levenstein_distance(texts_1,texts_2):
    start_time = time.time()
    lvn_array = np.zeros((len(texts_1),len(texts_2)))
    print(lvn_array)
    text_1_num = 0
    text_2_num = 0

    for text_1 in texts_1:
        for text_2 in texts_2:
            lvn_array[text_1_num,text_2_num] = Levenshtein.ratio(text_1,text_2)
            text_2_num += 1
        text_2_num = 0
        text_1_num += 1
    print("Calculating levenstein distance took --- %s seconds ---" % (time.time() - start_time))
    return lvn_array

#creating shingles_dict
def create_shingles_dict(texts,k):
    '''
    This function creates a list and dict of shingles
    :param texts:one string column of a df (comp names), which is going to be divided in shingles
    :param k: shingle size
    :return: list of all shingles with order
    dict of shingles, shingle as a key, index of the shingle in the list as a value
    '''
    start_time = time.time()
    print("Started creating shingles...")
    shingles_set = set()

    for text in texts:
        if len(text) >= k:
            shingles = [text[i:i + k] for i in range(len(text) - k + 1)]
        else:
            shingles = [text + '_' * (k - len(text))]
        for shingle in shingles:
            shingles_set.add(shingle)

    shingles_set = sorted(shingles_set)
    shingles_dict = dict(zip(shingles_set, range(len(shingles_set))))
    print("Creating shingles took --- %s seconds ---" % (time.time() - start_time))
    return shingles_set, shingles_dict


#converting docs to shingles
def create_doc_shingles(texts,k,shingles_dict):
    '''
    :param texts:
    :param k:
    :param shingles_dict:
    :return:
    '''
    start_time = time.time()
    print("Started creating shingles for docs...")
    docs = [[] for i in range(len(texts))]
    texts = [x.strip(' ') for x in texts] #remove spaces

    for doc, text in zip(docs, texts):
        if len(text) >= k:
            shingles = [text[i:i + k] for i in range(len(text) - k + 1)]
        else:
            shingles = [text + '_' * (k - len(text))]
        for shingle in shingles:
            doc.append(shingles_dict[shingle])
    print("Converting docs to shingles took --- %s seconds ---" % (time.time() - start_time))
    return docs


def jaccard(list1, list2): #is not counting duplicate shingles
    intersection = len(set(list1).intersection(list2))
    union = (len(set(list1)) + len(set(list2))) - intersection
    return intersection/union

def create_matches(buckets_bands, docs):
    start_time = time.time()
    print("Started creating matches...")
    matches = {} #keys - tuple of duplicate docs and values - jacc of docs(lists of shingles(numbers))
    for buckets in buckets_bands:
        for key, values_list in buckets.items(): #values_list - doc indexes in one buckets
            for value_1 in values_list: #iterating through doc_indexes
                for value_2 in values_list:
                    if value_2 > value_1 and (value_1, value_2) not in matches:
                        matches.setdefault((value_1, value_2), []).append(jaccard(docs[value_1], docs[value_2])) #docs[value_1] - shingle numbers
    print("Creating matches (jaccard) took --- %s seconds ---" % (time.time() - start_time))
    return matches