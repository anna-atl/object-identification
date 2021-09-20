import pandas as pd
import time
import numpy as np
import datetime
import string

import df_imports
import df_mapped
import df_labeled
import shingling
import creating_buckets
import comparison
import results_evaluation
import exporting_output


import json

pd.set_option('display.max_columns', None)


class attribute_matching_params:
    def __init__(self, matching_attribute, shingle_type='none', shingle_size=0, shingle_weight='none', buckets_type='none', signature_size=50, bands_number=5, comparison_method='jaccard', attribute_threshold=0):
        '''
        important to have the attribute_threshold in the end bc it's not used for the finding_best_methods_for_atts
        '''
        self.matching_attribute = matching_attribute
        #shingling
        self.shingle_type = shingle_type# token, shingle, shingle words
        self.shingle_size = shingle_size# 1,2
        self.shingle_weight = shingle_weight# 'normal', 'frequency', 'weighted'
        #creating_signatures
        self.buckets_type = buckets_type #minhash, weighted minhash 1, weighted minhash 2, one bucket, no buckets
        self.signature_size = signature_size
        self.bands_number = bands_number
        #comparison
        self.comparison_method = comparison_method #jaccard, weighted jaccard, fuzzy
        self.attribute_threshold = attribute_threshold

    def __str__(self):
        return "matching_attribute: %s,  attribute_threshold: %s, shingle_type: %s, shingle_size: %s, shingle_weight: %s, bands_number: %s, signature_size: %s" % (self.matching_attribute, self.attribute_threshold, self.shingle_type, self.shingle_size, self.shingle_weight, self.bands_number, self.signature_size)

class scenario_matching_params:
    def __init__(self, scenario, number_of_tries=1, dataset_size_to_import=0, dataset_size=0, sum_score='sum', attributes_matching):
        self.scenario = scenario
        self.number_of_tries = number_of_tries
        self.dataset_size_to_import = dataset_size_to_import
        self.dataset_size = dataset_size
        self.sum_score = sum_score  # sum_scores, multiplication
        self.attributes_matching = attributes_matching  # sum_scores, multiplication

if __name__ == "__main__":
    with open('/Users/Annie/Dropbox/Botva/TUM/Master_Thesis/object-identification/matching_attributes.json', 'r') as json_file:
        data = json.loads(json_file.read())
        #json.load(json_file)

    #checks needed: dataset_size_to_import > dataset_size
    #there should be at least one attribute without 'no buckets' bucket type

    atts = attribute_matching_params(data["matching_attribute"], data["shingle_type"],
                                     data["shingle_size"],
                                     data["shingle_weight"], data["buckets_type"],
                                     data["signature_size"], data["bands_number"],
                                     data["comparison_method"], data["attribute_threshold"],
    )

    mats = scenario_matching_params(data["scenario"],
                                     data["number_of_tries"],
                                     data["dataset_size_to_import"],
                                     data["dataset_size"],
                                     data["sum_score"],
                                    data["attribute_params"]
    )

    start_time = time.time()
    print('Started downloading datasets')
    df_with_attributes = df_imports.main(mats.dataset_size_to_import)
    print("Importing datasets took --- %s seconds ---" % (time.time() - start_time))

    number_of_tries = mats.number_of_tries

    for try_number in number_of_tries:
        df_to_match, docs_mapping, docs = df_mapped.main(df_with_attributes, atts.matching_attribute, mats.dataset_size)

        df_labeled_data = df_labeled.main(df_to_match)

        for attribute in mats.attribute_params:
            #creating shingles and weights
            docs_shingled, shingles_weights_in_docs, shingles_weights_list = shingling.main(docs, atts.shingle_type, atts.shingle_size, atts.shingle_weight)

        if atts.buckets_type != 'no buckets' or atts.buckets_type != 'one bucket':
            #minhash
            buckets_of_bands = creating_buckets.main(docs_shingled, shingles_weights_list, shingles_weights_in_docs, atts.buckets_type, atts.signature_size, atts.bands_number)
            #comparing candidate pairs
        elif atts.buckets_type == 'one bucket':
            buckets_of_bands = [{} for i in range(bands_number)]
            #buckets_of_band.setdefault(tuple(signatures[band*r:band*r+r, doc_index]), []).append(doc_index)

        for attribute in attributes:
            df_matches = comparison.main(buckets_of_bands, docs_shingled, atts.comparison_method, shingles_weights_in_docs, atts.sum_score, atts.matching_attribute)


        print("Started adding matches attributes...")
        matches_with_attributes = df_imports.add_attributes_to_matches(df_matches, df_with_attributes, docs_mapping)
        print("...Added matches attributes")
        results = results_evaluation.main(df_matches, df_with_attributes, df_labeled_data, atts.matching_attribute)

        print('------------------------------------------------')
        print("Matching algorithm took for the {} and {} size --- {} seconds ---".format(atts.matching_attribute, len(docs), time.time() - start_time))


    for attribute in matched_pairs:
        try:
            df_all_matches['match_score_{}'.format(attribute.matching_attribute)] = df_all_matches['match_score_{}'.format(attribute.matching_attribute)].fillna(0) * att1_weight
            df_all_matches['match_score'] = df_all_matches['match_score'] + df_all_matches[
                'match_score_{}'.format(attribute.matching_attribute)] * att2_weight
        except:
            print('No matches for the {} attribute'.format(attribute.matching_attribute))

    df_all_matches = df_all_matches.sort_values(by='match_score', ascending=False)

    df_all_matches['match_score'] = df_all_matches['match_score_{}'.format(matching_attribute)]
    exporting_output.main()