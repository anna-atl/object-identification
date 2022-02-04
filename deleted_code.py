for shingle_in_doc in shingle_counts:
    tf_shingle_in_doc = shingle_counts[shingle_in_doc] / len(shingles_in_doc)
    shingle_weight_in_doc = tf_shingle_in_doc * idf_shingles[shingle_in_doc]
    shingles_weights_in_docs_dict[doc_index][shingle_in_doc] = int(round(shingle_weight_in_doc * 100,
                                                                         0))  # each doc has dict. key - shingle index, value - shingle's weight in doc
    all_shingles_weights.setdefault(shingle_in_doc, []).append((doc_index, shingles_weights_in_docs_dict[doc_index][
        shingle_in_doc]))  # key - shingle, value - all weights of the shingle
    all_shingles_weights_only_weight.setdefault(shingle_in_doc, []).append(
        shingles_weights_in_docs_dict[doc_index][shingle_in_doc])
    if experiment_mode == 'test':
        all_shingles_weights_only_weight.setdefault(all_shingles[shingle_in_doc], []).append(
            shingles_weights_in_docs_dict[doc_index][shingle_in_doc])



number_of_shingles = len(all_shingles_weights.keys())






def create_weights(docs_shingled, all_shingles_docs, shingle_weight, experiment_mode, all_shingles_weights):
    idf_shingles = {}

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






docs_shingled[doc_index] = shingles
doc_shingled.append(shingles_dict[shingle])  # fix the reversed

for shingle in shingles:
    shingles_positions_in_doc.setdefault(shingle_in_doc, []).append(shingle_index_in_doc)
    shingles_positions_in_doc_from_end.setdefault(shingle_in_doc, []).append(
        len(shingles_in_doc) - shingle_index_in_doc)

total_number_of_shingles = len(
    [len(docs_with_this_shingle) for shingle, docs_with_this_shingle in all_shingles.items()])
for shingle, docs_with_this_shingle in all_shingles.items():
    shingles_frequency[shingle] = len(docs_with_this_shingle)
    idf_shingles[shingle] = total_number_of_shingles / shingles_frequency[shingle]





        # and create weights of shingles in docs - list of dictionaries, key - shingle(index), value - weight (sum the weights if its several times)
        for shingle_position_from_end, shingle_index in enumerate(reversed(doc_shingled)):
                if shingle_in_doc_weight = (shingle_position_from_end + 1) / len(doc_shingled) * shingles_weights_dict[
                            shingle_index]  # check if list is already created
                        shingles_weights_in_doc.setdefault(shingle_index, []).append(shingle_in_doc_weight)
        else:
            shingles_weights_in_doc[shingle_index] = [1]


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

if shingle_weight != "none":
    shingles_positions_in_doc = list(range(len(shingles_in_doc)))
    shingles_positions_in_doc_from_end = list(range(len(shingles_in_doc)))

    for shingle_index_in_doc, shingle_in_doc in enumerate(shingles_in_doc):
        all_shingles.setdefault(shingle_in_doc, []).append(doc_index)
        shingles_positions_in_doc.append(shingle_index_in_doc)
        shingles_positions_in_doc_from_end[shingle_index_in_doc] = len(shingles_in_doc) - shingle_index_in_doc
        all_shingles_positions_in_docs.setdefault(shingle_in_doc, []).append(shingle_index_in_doc)
        all_shingles_positions_in_docs_from_end.setdefault(shingle_in_doc, []).append(
            len(shingles_in_doc) - shingle_index_in_doc)

shingle_positions_in_docs[doc_index].setdefault(shingle_in_doc, []).append(shingle_index_in_doc)


def create_signatures_array(docs_shingled, buckets_type, signature_size, all_shingles_weights,
                            shingles_weights_in_docs):
    signatures = {}

    for permutation_number in range(signature_size):
        if buckets_type == 'minhash':
            random.shuffle(shingles_shuffled)
            for doc_index, doc_shingled in enumerate(docs_shingled):
                doc_a = [shingles_shuffled[i] for i in
                         doc_shingled]  # --check this-- recreating shingles list of a doc with randomization
                try:
                    signature[doc_index] = min(
                        doc_a)  # saving the smallest number for this randomization for this signature
                except:
                    print('didnt work for docs_shingled {}'.format(docs_shingled[doc_index]))

        signatures.setdefault(permutation_number, []).append(docs_signatures)

    signatures = np.zeros((signature_size, len(docs_shingled)),
                          dtype=tuple)  # create a df with # rows = signature_size and #columns = docs
    shingles_shuffled = [i for i in
                         range(len(all_shingles_weights))]  # create list of shingles indexes for further randomizing

    shingles_randomized = [[] for i in range(len(all_shingles_weights))]
    hash_weights_random = [i for i in range(len(all_shingles_weights))]

    for signature in signatures:  # iterating through rows of the signatures array
        if buckets_type == 'minhash':
            random.shuffle(shingles_shuffled)
            for doc_index, doc_shingled in enumerate(docs_shingled):
                doc_a = [shingles_shuffled[i] for i in
                         doc_shingled]  # --check this-- recreating shingles list of a doc with randomization
                try:
                    signature[doc_index] = min(
                        doc_a)  # saving the smallest number for this randomization for this signature
                except:
                    print('didnt work for docs_shingled {}'.format(docs_shingled[doc_index]))
        elif buckets_type == 'weighted minhash 1':
            # print(max(shingles_weights_dict))
            # print(min(shingles_weights_dict))
            for hash_index, hash_randomized in enumerate(shingles_randomized):
                randomhash = random.sample(range(0, 1000), all_shingles_weights[
                    hash_index] + 1)  # this is [vk(x), vk(x)...], k the same, x changes
                shingles_randomized[hash_index] = randomhash
            for doc_index, doc_hashed in enumerate(docs_shingled):
                minvalue = 1000000
                shingles_weights_in_doc = shingles_weights_in_docs[doc_index]
                for hash_position, hash_index in enumerate(doc_hashed):
                    # print(hash_index)
                    doc_a = shingles_randomized[hash_index]
                    # print(doc_a)
                    hash_in_doc_weight = shingles_weights_in_doc[hash_index]
                    hash_in_doc_weight = np.round(hash_in_doc_weight[0], 0)  # [0] hard coded, should be fixed
                    hash_in_doc_weight = hash_in_doc_weight.astype(int)
                    doc_a = doc_a[:hash_in_doc_weight + 1]
                    a = min(doc_a)
                    # print(a)
                    if a < minvalue:
                        minvalue = a
                        minindex = hash_index
                signature[doc_index] = (minindex, minvalue)
        elif buckets_type == 'weighted minhash 2':
            for hash_index, hash_weight in enumerate(all_shingles_weights):
                r1 = random.gammavariate(2, 1)
                r2 = random.gammavariate(2, 1)
                b1 = random.uniform(0, 1)
                b2 = random.uniform(0, 1)
                c = random.gammavariate(2, 1)
                hash_weights_random[hash_index] = hashes_parameters(r1, r2, b1, b2, c)  # class here is ok
            for doc_index, doc_hashed in enumerate(docs_shingled):  # for iterating over indexes in list as well
                minvalue = 1000000
                for hash_position, hash_index in enumerate(reversed(doc_hashed)):
                    hash_in_doc_weight = shingles_weights_in_docs[hash_index]
                    lny2 = hash_weights_random[hash_index].r2 * (math.floor(
                        math.log(hash_in_doc_weight) / hash_weights_random[hash_index].r2 + hash_weights_random[
                            hash_index].b2) - hash_weights_random[hash_index].b2)
                    z2 = math.exp(lny2) * math.exp(hash_weights_random[hash_index].r2)
                    a = hash_weights_random[hash_index].c / z2
                    if a < minvalue:
                        minvalue = a  # a - min random number for this shingle
                        minindex = hash_index
                        minposition = hash_position
                k = minindex
                # for the I2CWS:
                hash_in_doc_weight = (minposition + 1) / len(doc_hashed) * shingles_weights_dict[
                    k]  # this won't be needed
                hash_in_doc_weight = round(hash_in_doc_weight, 0)  # no rounding
                t1 = math.floor(math.log(hash_in_doc_weight) / hash_weights_random[k].r1 + hash_weights_random[k].b1)
                signature[doc_index] = (k, t1)

    return signatures



def create_shingled_docs(docs, shingle_type, shingle_size, shingle_weight, experiment_mode):
    '''
    This function creates a list and dict of shingles
    :param texts:one string column of a df (comp names), which is going to be divided in shingles
    :param k: shingle size
    :return: list of all shingles with order
    dict of shingles, shingle as a key, index of the shingle in the list as a value
    '''
    k = shingle_size
    all_shingles = {}
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

        if shingle_weight != "none":
            shingles_positions_in_docs[doc_index] = list(range(len(shingles_in_doc)))
            shingles_positions_in_docs_from_end[doc_index] = list(range(len(shingles_in_doc)))

            for shingle_index_in_doc, shingle_in_doc in enumerate(shingles_in_doc):
                all_shingles.setdefault(shingle_in_doc, []).append(doc_index)
                shingles_positions_in_docs[doc_index][shingle_index_in_doc] = shingle_index_in_doc
                shingles_positions_in_docs_from_end[doc_index][shingle_index_in_doc] = len(shingles_in_doc) - shingle_index_in_doc
                #shingles_positions_in_doc.append(shingle_index_in_doc)
                #shingles_positions_in_doc_from_end.append(len(shingles_in_doc) - shingle_index_in_doc)
                all_shingles_positions_in_docs.setdefault(shingle_in_doc, []).append(shingle_index_in_doc)
                all_shingles_positions_in_docs_from_end.setdefault(shingle_in_doc, []).append(len(shingles_in_doc) - shingle_index_in_doc)

    if shingle_weight != "none":
        total_number_of_docs = len(docs)
        for shingle, docs_with_this_shingle in all_shingles.items():
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
            shingles_weights_in_docs[doc_index] = list(range(len(shingles_in_doc)))

            for shingle_index_in_doc, shingle_in_doc in enumerate(shingles_in_doc):
                shingles_frequency_in_docs[doc_index][shingle_index_in_doc] = all_shingles[shingle_in_doc].count(doc_index)

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


    if shingle_weight == 'tf-idf-cp-0':
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
        total_number_of_docs = len(docs_shingled)
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

                shingles_weights_in_docs_dict[doc_index].setdefault(shingle_in_doc, []).append(shingles_weights_in_docs[doc_index][shingle_index_in_doc])

                all_shingles_weights.setdefault(shingle_in_doc, []).append((doc_index, shingles_weights_in_docs[doc_index][shingle_index_in_doc]))

    if experiment_mode == 'test':
        df_shingles_weights = pd.DataFrame.from_dict(all_shingles_weights, orient='index') #to see all shingles weights in docs overall

    return shingles_weights_in_docs, shingles_weights_in_docs_dict, all_shingles_weights




    for matching_attribute in matching_attributes:
        for shingle_type in shingle_types:
            for buckets_type in buckets_types:
                for signature_size in signature_sizes:
                    for bands_number in bands_numbers:
                        for attribute_threshold in attribute_thresholds:

                            attributes_to_bucket = {k: v for k, v in attribute_params.items() if
                                                    v.buckets_type != 'no buckets'}
                            df_to_bucket = df.dropna(
                                subset=[v.matching_attribute for k, v in attributes_to_bucket.items()])
                            try:
                                df_to_bucket = df_to_bucket.sample(n=dataset_size)
                            except:
                                print('Warning: dataset size {} is larger than the imported {} dataset size'.format(
                                    dataset_size, len(df_to_bucket.index)))

                            docs_mapping_new_old = {}
                            docs_mapping_old_new = {}
                            docs_to_match = {}

                            start_time = time.time()
                            print('Started to create documents mapping')
                            for attribute_name, attribute_pars in attribute_params.items():
                                docs_mapping_new_old[attribute_name], docs_mapping_old_new[attribute_name], \
                                docs_to_match[attribute_name] = create_mapping(df_to_bucket,
                                                                               attribute_pars.matching_attribute)
                            print("Creating mapped documents took --- %s seconds ---" % (time.time() - start_time))

                            return df_to_bucket, docs_mapping_new_old, docs_mapping_old_new, docs_to_match

