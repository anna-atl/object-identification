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
