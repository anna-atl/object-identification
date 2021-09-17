import pandas as pd
import time
import numpy as np
import datetime
import string

import df_imports
import shingling
import creating_buckets
import comparison
import results_evaluation
import exporting_output

pd.set_option('display.max_columns', None)

class attribute_matching_params:
    def __init__(self, matching_attribute, hash_type='none', shingle_size=0, hash_weight='none', buckets_type='none', signature_size=50, bands_number=5, comparison_method='jaccard', sum_score='sum', attribute_threshold=0):
        '''
        important to have the attribute_threshold in the end bc it's not used for the finding_best_methods_for_atts
        '''
        self.matching_attribute = matching_attribute
        #shingling
        self.hash_type = hash_type# token, shingle, shingle words
        self.shingle_size = shingle_size# 1,2
        self.hash_weight = hash_weight# 'normal', 'frequency', 'weighted'
        #creating_signatures
        self.buckets_type = buckets_type #minhash, weighted minhash 1, weighted minhash 2,
        self.signature_size = signature_size
        self.bands_number = bands_number
        #comparison
        self.comparison_method = comparison_method #jaccard, weighted jaccard, fuzzy
        self.sum_score = sum_score #sum_scores, multiplication
        self.attribute_threshold = attribute_threshold

    def __str__(self):
        return "matching_attribute: %s, matching_method: %s,  attribute_threshold: %s, hash_type: %s, shingle_size: %s, hash_weight: %s, bands_number: %s, signature_size: %s" % (self.matching_attribute, self.matching_method, self.attribute_threshold, self.hash_type, self.shingle_size, self.hash_weight, self.bands_number, self.signature_size)

if __name__ == "__main__":
    number_of_tries = 1 #how many random datasets should be created
    dataset_size_to_import = 500
    dataset_size = 1000000

    matching_attribute = 'name_clean'
    hash_type = 'shingle'
    shingle_size = 3
    hash_weight = 'weighted'
    buckets_type = 'weighted minhash 1'
    signature_size = 50
    bands_number = 5
    comparison_method = 'weighted jaccard'

    sum_score = 'sum' #outside of json
    attribute_threshold = 0

    atts = attribute_matching_params(matching_attribute,
                                          hash_type, shingle_size, hash_weight,
                                          buckets_type, signature_size, bands_number,
                                          comparison_method, sum_score, attribute_threshold)

    start_time = time.time()
    print('Started downloading datasets')
    df_with_attributes, docs_mapping, docs, df_labeled_data = df_imports.main(dataset_size_to_import, atts.matching_attribute, dataset_size)
    print("Importing datasets took --- %s seconds ---" % (time.time() - start_time))

    #creating shingles and weights
    docs_shingled, shingles_weights_in_docs, hash_weights_list = shingling.main(docs, atts.hash_type, atts.shingle_size, atts.hash_weight)
    #minhash
    buckets_of_bands = creating_buckets.main(docs_shingled, hash_weights_list, shingles_weights_in_docs, atts.buckets_type, atts.signature_size, atts.bands_number)
    #comparing candidate pairs
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