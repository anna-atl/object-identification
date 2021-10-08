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
    def __init__(self, scenario, number_of_tries=1, dataset_size_to_import=0, dataset_size=0, sum_score='sum', attribute_params={}):
        self.scenario = scenario
        self.number_of_tries = number_of_tries
        self.dataset_size_to_import = dataset_size_to_import
        self.dataset_size = dataset_size
        self.sum_score = sum_score  # sum_scores, multiplication
        self.attribute_params = {}
        for attribute, params in attribute_params.items():
            self.attribute_params[attribute] = attribute_matching_params(params["matching_attribute"],
                                                                            params["shingle_type"],
                                                                            params["shingle_size"],
                                                                            params["shingle_weight"],
                                                                            params["buckets_type"],
                                                                            params["signature_size"],
                                                                            params["bands_number"],
                                                                            params["comparison_method"],
                                                                            params["attribute_threshold"])

if __name__ == "__main__":
    with open('/Users/Annie/Dropbox/Botva/TUM/Master_Thesis/object-identification/scenarios/scenario_1', 'r') as json_file:
        data = json.loads(json_file.read())

    mats = scenario_matching_params(data["scenario"], data["number_of_tries"], data["dataset_size_to_import"], data["dataset_size"],
                                    data["sum_score"], data["attribute_params"]
                                    )

    #checks needed: dataset_size_to_import > dataset_size
    #there should be at least one attribute without 'no buckets' bucket type

    start_time = time.time()
    print('Started downloading datasets')
    df_with_attributes = df_imports.main(mats.dataset_size_to_import)
    print("Importing datasets took --- %s seconds ---" % (time.time() - start_time))
    a = mats.attribute_params

    for try_number in range(mats.number_of_tries):

        #created a list of attributes which are going to be minhashed (create buckets), so they should be not null
        attributes_to_bucket = [v.matching_attribute for k, v in mats.attribute_params.items() if v.buckets_type != 'no buckets']
        df_to_match, docs_mapping, docs = df_mapped.main(df_with_attributes, attributes_to_bucket, mats.dataset_size)
        df_labeled_data = df_labeled.main(df_to_match)

        buckets = []
        docs_shingled_all = {}
        shingles_weights_in_docs = {}

        for att, attribute in mats.attribute_params.items(): #add value
            #creating shingles and weights
            docs_shingled, shingles_weights_in_docs, shingles_weights_list = shingling.main(docs, att.shingle_type, att.shingle_size, att.shingle_weight)

            if att.buckets_type != 'no buckets' or att.buckets_type != 'one bucket':
                #minhash
                buckets_of_bands = creating_buckets.main(docs_shingled, shingles_weights_list, shingles_weights_in_docs, att.buckets_type, att.signature_size, att.bands_number)
                #comparing candidate pairs
            elif att.buckets_type == 'one bucket':
                all_docs = [i for i in range(len(docs_shingled))]
                buckets_of_bands = [{(0, 0): [i for i in range(len(docs_shingled))]}] #put all
            buckets.append(buckets_of_bands)

        for att in mats.attribute_params:
            df_att_matches = comparison.main(buckets, docs_shingled, att.comparison_method, shingles_weights_in_docs, mats.sum_score, att.matching_attribute)

        print("Started adding matches attributes...")

        def add_attributes_to_matches(df_matches, df_with_attributes, docs_mapping):
            print("Started joining the result with the mapping table...")
            df_matches_full = pd.merge(df_matches, docs_mapping, how='left', left_on=['doc_1'], right_on=['new_index'])
            b = df_matches_full[(df_matches_full['doc_1'] == 20236)]
            print(b)

            df_matches_full = df_matches_full.drop(['new_index', 'old_index', 'doc_1'], axis=1)
            df_matches_full = pd.merge(df_matches_full, docs_mapping, how='left', left_on=['doc_2'],
                                       right_on=['new_index'])
            df_matches_full = df_matches_full.drop(['new_index', 'old_index', 'doc_2'], axis=1)
            df_matches_full = df_matches_full.rename(columns={'id_x': 'doc_1', 'id_y': 'doc_2'}, inplace=False)
            b = df_matches_full[(df_matches_full['doc_1'] == 2490742) | (df_matches_full['doc_2'] == 3121092)]
            print(b)

            b = df_matches_full.loc[df_matches_full['doc_1'] < df_matches_full['doc_2']]
            c = df_matches_full.loc[df_matches_full['doc_1'] > df_matches_full['doc_2']]
            c = c.rename(columns={'doc_1': 'doc_2', 'doc_2': 'doc_1'}, inplace=False)
            c = c[['match_score_{}'.format(attribute.matching_attribute), 'doc_1', 'doc_2']]
            df_matches_full = b.append(c)

            df_matches_full = pd.merge(df_matches, df, how='left', left_on=['doc_1'], right_on=['id'])
            df_matches_full = df_matches_full.drop(['id'], axis=1)
            df_matches_full = pd.merge(df_matches_full, df, how='left', left_on=['doc_2'], right_on=['id'])
            df_matches_full = df_matches_full.drop(['id'], axis=1)
            return df_matches_full


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