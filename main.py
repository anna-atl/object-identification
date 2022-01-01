import pandas as pd
import time

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
    def __init__(self, matching_attribute, shingle_type='none', shingle_size=0, shingle_weight='none', buckets_type='none', signature_size=50, bands_number=5, comparison_method='jaccard', attribute_weight=1, attribute_threshold=0):
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
        self.attribute_weight = attribute_weight
        self.attribute_threshold = attribute_threshold

    #def __str__(self): return "matching_attribute: %s,  attribute_threshold: %s, shingle_type: %s, shingle_size: %s, shingle_weight: %s, bands_number: %s, signature_size: %s" % (self.matching_attribute, self.attribute_threshold, self.shingle_type, self.shingle_size, self.shingle_weight, self.bands_number, self.signature_size)

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
                                                                            params["attribute_weight"],
                                                                            params["attribute_threshold"])

def add_attributes_to_matches(df_matches, df_with_attributes):
    print("Started joining the result with the mapping table...")

    df_matches_full = pd.merge(df_matches, df_with_attributes, how='left', left_on=['doc_1'], right_index=True)
    #df_matches_full = df_matches_full.drop(['id'], axis=1)
    df_matches_full = pd.merge(df_matches_full, df_with_attributes, how='left', left_on=['doc_2'], right_index=True)
    #df_matches_full = df_matches_full.drop(['id'], axis=1)

    return df_matches_full

if __name__ == "__main__":
    with open('scenarios/scenario_1', 'r') as json_file:
        data = json.loads(json_file.read())

    mats = scenario_matching_params(data["scenario"], data["number_of_tries"], data["dataset_size_to_import"], data["dataset_size"],
                                    data["sum_score"], data["attribute_params"]
                                    )

    #checks needed: dataset_size_to_import > dataset_size
    #there should be at least one attribute without 'no buckets' bucket type
    #if not no buckets buckets type, then to check if signature size !=0 and number of bands is correct

    start_time = time.time()
    print('Started downloading datasets')
    df_with_attributes = df_imports.main(mats.dataset_size_to_import)
    print("Importing datasets took --- %s seconds ---" % (time.time() - start_time))
    print('------------------------------------------------')

    for try_number in range(mats.number_of_tries):

        #created a list of attributes which are going to be minhashed (create buckets), so they should be not null
        df_to_bucket, docs_mapping_new_old, docs_mapping_old_new, docs_to_match = df_mapped.main(df_with_attributes, mats.attribute_params, mats.dataset_size)
        df_labeled_data, labeled_positive, labeled_negative, labeled_matches_count = df_labeled.main(df_to_bucket)

        buckets = []
        docs_shingled = {}
        shingles_weights_dict = {}
        shingles_weights_in_docs = {}

        for attribute_name, attribute_pars in mats.attribute_params.items():
            print('Started working on {} '.format(attribute_name))
            start_time = time.time()
            print("Started creating shingles...")
            docs_shingled[attribute_name], shingles_weights_dict[attribute_name]\
                , shingles_weights_in_docs[attribute_name] = shingling.main(docs_to_match[attribute_name]
                , attribute_pars.shingle_type, attribute_pars.shingle_size, attribute_pars.shingle_weight)
            print("Converting docs to shingles took --- %s seconds ---" % (time.time() - start_time))

            if attribute_pars.buckets_type != 'no buckets' and attribute_pars.buckets_type != 'one bucket':
                buckets_of_bands = creating_buckets.main(docs_shingled[attribute_name]
                            , shingles_weights_dict[attribute_name], shingles_weights_in_docs[attribute_name], attribute_pars.buckets_type, attribute_pars.signature_size, attribute_pars.bands_number, docs_mapping_new_old[attribute_name])
            elif attribute_pars.buckets_type == 'one bucket':
                #FIX IT
                buckets_of_bands = [{(0, 0): [i for i in range(len(docs_shingled[attribute_name]))]}]
            else:
                buckets_of_bands = [{}]
            buckets.extend(buckets_of_bands)

        matched_pairs = {}
        for buckets_of_band in buckets:
            for bucket, docs_in_bucket in buckets_of_band.items():  # values_list - doc indexes in one buckets
                for doc_index_1 in docs_in_bucket:  # iterating through doc_indexes
                    for doc_index_2 in docs_in_bucket:
                        if doc_index_2 > doc_index_1:
                            matched_pairs[doc_index_1, doc_index_2] = 1

        df_matches = pd.DataFrame.from_dict(matched_pairs, orient='index')
        df_matches['matches_tuple'] = df_matches.index
        a = df_matches['matches_tuple'].tolist()
        df_matches[['doc_1', 'doc_2']] = pd.DataFrame(df_matches['matches_tuple'].tolist(), index=df_matches.index)
        df_matches = df_matches.drop(['matches_tuple', 0], axis=1)
        df_matches = df_matches.reset_index(drop=True)

        for attribute_name, attribute_pars in mats.attribute_params.items():
            print('Started working on {} '.format(attribute_name))
            df_att_matches = comparison.main(buckets, docs_shingled[attribute_name], attribute_pars.comparison_method, shingles_weights_in_docs[attribute_name], attribute_pars.matching_attribute, docs_mapping_old_new[attribute_name])

            df_matches = pd.merge(df_matches, df_att_matches, how='left', left_on=['doc_1', 'doc_2'],
                                  right_on=['doc_1', 'doc_2'])

        print("Started creating a common matching score...")
        df_matches['match_score'] = df_matches.iloc[:, 2:].sum(axis=1)
        df_matches = df_matches.sort_values(by='match_score', ascending=False)
        print("Finished creating a common matching score...")
        print("------------------------------------------")

        print("Started adding matches attributes...")
        df_matches_full = add_attributes_to_matches(df_matches, df_with_attributes)

        df_matches_estimation, false_positive, false_negative, true_positive, true_negative = results_evaluation.main(df_matches_full, df_labeled_data, labeled_positive, labeled_negative)
        df_matches_estimation.to_csv("df_results_{}_{}.csv".format(matching_attribute, str(datetime.datetime.now())))

        print('------------------------------------------------')
        print("Matching algorithm took for {} size --- {} seconds ---".format(len(df_to_bucket.index), time.time() - start_time))

        #exporting_output.main()

        print('end')
