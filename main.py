import pandas as pd
import time
import datetime
import df_imports
import df_mapped
import df_labeled
import shingling
import creating_buckets
import comparison
import results_evaluation
import exporting_experiment_results

import json

pd.set_option('display.max_columns', None)

class attribute_matching_params:
    def __init__(self, matching_attributes, shingle_types=['none'], shingle_sizes=[1], shingle_weights=['none'],
                 buckets_types=['none'], signature_sizes=[0], bands_numbers=[0], comparison_methods=['none'], attribute_weights=[1], attribute_thresholds=[0]):
        '''
        important to have the attribute_threshold in the end bc it's not used for the finding_best_methods_for_atts
        '''
        self.matching_attributes = matching_attributes
        #shingling
        self.shingle_types = shingle_types# token, shingle, shingle words
        self.shingle_sizes = shingle_sizes# 1,2
        self.shingle_weights = shingle_weights# 'normal', 'frequency', 'weighted'
        #creating_signatures
        self.buckets_types = buckets_types #minhash, weighted minhash 1, weighted minhash 2, one bucket, no buckets
        self.signature_sizes = signature_sizes
        self.bands_numbers = bands_numbers
        #comparison
        self.comparison_methods = comparison_methods #jaccard, weighted jaccard, fuzzy
        self.attribute_weights = attribute_weights
        self.attribute_thresholds = attribute_thresholds

    #def __str__(self): return "matching_attribute: %s,  attribute_threshold: %s, shingle_type: %s, shingle_size: %s, shingle_weight: %s, bands_number: %s, signature_size: %s" % (self.matching_attribute, self.attribute_threshold, self.shingle_type, self.shingle_size, self.shingle_weight, self.bands_number, self.signature_size)

class scenario_matching_params:
    def __init__(self, scenario_name, experiment_mode="test", number_of_tries=1, dataset_size_to_import=0, dataset_size=0, sum_score='none', attribute_params={}):
        self.scenario_name = scenario_name
        self.experiment_mode = experiment_mode
        self.number_of_tries = number_of_tries
        self.dataset_size_to_import = dataset_size_to_import
        self.dataset_size = dataset_size
        self.sum_score = sum_score  # sum_scores, multiplication
        self.attribute_params = {}
        for attribute, params in attribute_params.items():
            self.attribute_params[attribute] = attribute_matching_params(params["matching_attributes"],
                                                                            params["shingle_types"],
                                                                            params["shingle_sizes"],
                                                                            params["shingle_weights"],
                                                                            params["buckets_types"],
                                                                            params["signature_sizes"],
                                                                            params["bands_numbers"],
                                                                            params["comparison_methods"],
                                                                            params["attribute_weights"],
                                                                            params["attribute_thresholds"])

def add_attributes_to_matches(df_matches, df_with_attributes):
    print("Started joining the result with the mapping table...")

    df_matches_full = pd.merge(df_matches, df_with_attributes, how='left', left_on=['doc_1'], right_index=True)
    #df_matches_full = df_matches_full.drop(['id'], axis=1)
    df_matches_full = pd.merge(df_matches_full, df_with_attributes, how='left', left_on=['doc_2'], right_index=True)
    #df_matches_full = df_matches_full.drop(['id'], axis=1)

    return df_matches_full

if __name__ == "__main__":
    with open('scenarios/scenario_minhash_name_2', 'r') as json_file:
        data = json.loads(json_file.read())

    mats = scenario_matching_params(data["scenario_name"], data["experiment_mode"], data["number_of_experiments"], data["dataset_size_to_import"], data["dataset_size"],
                                    data["sum_score"], data["attribute_params"]
                                    )

    print('Started working on the {}'.format(mats.scenario_name))
    df_experiment_results = pd.DataFrame()

    # checks needed for the dataset:
    # dataset_size_to_import > dataset_size
    # at least one att should be bucketed
    # if bucket type == no buckets/one bucket, then signature size =0 and number of bands = 0
    # if shingle_weight!= none, then buckets_types = cws or i2cws

    start_time = time.time()
    print('Started downloading datasets')
    df_with_attributes = df_imports.main(mats.dataset_size_to_import)
    print("Importing datasets took --- %s seconds ---" % (time.time() - start_time))
    print('------------------------------------------------')

    for experiment_number in range(mats.number_of_tries):
        # created a list of attributes which are going to be minhashed (create buckets), so they should be not null
        df_to_bucket, docs_mapping_new_old, docs_mapping_old_new, docs_to_match = df_mapped.main(
            df_with_attributes, mats.dataset_size, mats.experiment_mode, mats.attribute_params)
        df_labeled_data, labeled_positive, labeled_negative, labeled_matches_count = df_labeled.main(
            df_to_bucket)
        print('')

        for attribute_name, attribute_pars in mats.attribute_params.items():

            buckets = {}
            docs_shingled = {}
            all_shingles_weights = {}
            all_shingles_weights_only_weight = {}
            shingles_weights_in_docs_dict = {}

            for matching_attribute in attribute_pars.matching_attributes:
                docs_shingled[attribute_name] = {}
                all_shingles_weights[attribute_name] = {}
                all_shingles_weights_only_weight[attribute_name] = {}
                shingles_weights_in_docs_dict[attribute_name] = {}
                buckets[attribute_name] = {}
                for shingle_type in attribute_pars.shingle_types:
                    docs_shingled[attribute_name][shingle_type] = {}
                    all_shingles_weights[attribute_name][shingle_type] = {}
                    all_shingles_weights_only_weight[attribute_name][shingle_type] = {}
                    shingles_weights_in_docs_dict[attribute_name][shingle_type] = {}
                    buckets[attribute_name][shingle_type] = {}
                    for shingle_size in attribute_pars.shingle_sizes:

                        docs_shingled[attribute_name][shingle_type][shingle_size] = {}
                        all_shingles_weights[attribute_name][shingle_type][shingle_size] = {}
                        all_shingles_weights_only_weight[attribute_name][shingle_type][shingle_size] = {}
                        shingles_weights_in_docs_dict[attribute_name][shingle_type][shingle_size] = {}
                        buckets[attribute_name][shingle_type][shingle_size] = {}
                        for shingle_weight in attribute_pars.shingle_weights:
                            processing_time = time.time()
                            print('--Started preprocessing {} '.format(attribute_name))
                            docs_shingled[attribute_name][shingle_type][shingle_size][shingle_weight], all_shingles_weights[attribute_name][shingle_type][shingle_size][shingle_weight]\
                                , all_shingles_weights_only_weight[attribute_name][shingle_type][shingle_size][shingle_weight], shingles_weights_in_docs_dict[attribute_name][shingle_type][shingle_size][shingle_weight] = shingling.main(docs_to_match[attribute_name], shingle_type, shingle_size, shingle_weight, mats.experiment_mode)

                            buckets[attribute_name][shingle_type][shingle_size][shingle_weight] = {}
                            for buckets_type in attribute_pars.buckets_types:
                                buckets[attribute_name][shingle_type][shingle_size][shingle_weight][buckets_type] = {}
                                for signature_size in attribute_pars.signature_sizes:

                                    buckets[attribute_name][shingle_type][shingle_size][shingle_weight][
                                        buckets_type][signature_size] = {}
                                    for bands_number in attribute_pars.bands_numbers:
                                        print('')
                                        print(
                                            'Started to work on {} {} {} {} {} {} {}'.format(matching_attribute, shingle_type,
                                                                                       shingle_size, shingle_weight,
                                                                                       buckets_type, signature_size, bands_number))
                                        buckets[attribute_name][shingle_type][shingle_size][shingle_weight][
                                            buckets_type][signature_size][bands_number] = []
                                        if buckets_type != 'no buckets' and buckets_type != 'one bucket':
                                            print('--Started bucketing {} '.format(attribute_name))
                                            buckets_of_bands = creating_buckets.main(docs_shingled[attribute_name][shingle_type][shingle_size][shingle_weight]
                                                                                     , all_shingles_weights[attribute_name][shingle_type][shingle_size][shingle_weight], all_shingles_weights_only_weight[attribute_name][shingle_type][shingle_size][shingle_weight], shingles_weights_in_docs_dict[attribute_name][shingle_type][shingle_size][shingle_weight], buckets_type, signature_size, bands_number, docs_mapping_new_old[attribute_name])
                                        elif buckets_type == 'one bucket':
                                            #FIX IT
                                            buckets_of_bands = [{(0, 0): [i for i in range(len(docs_shingled[attribute_name]))]}]
                                        else:
                                            print('--No bucketing for {} '.format(attribute_name))
                                            buckets_of_bands = [{}]

                                        buckets[attribute_name][shingle_type][shingle_size][shingle_weight][buckets_type][signature_size][bands_number].extend(buckets_of_bands)
                                        print('')

                                        matched_pairs = {}
                                        for buckets_of_band in buckets[attribute_name][shingle_type][shingle_size][shingle_weight][buckets_type][signature_size][bands_number]:
                                            for bucket, docs_in_bucket in buckets_of_band.items():  # values_list - doc indexes in one buckets
                                                for doc_index_1 in docs_in_bucket:  # iterating through doc_indexes
                                                    for doc_index_2 in docs_in_bucket:
                                                        if doc_index_2 > doc_index_1:
                                                            matched_pairs[doc_index_1, doc_index_2] = 1

                                        if len(matched_pairs) != 0:
                                            df_matches = pd.DataFrame.from_dict(matched_pairs, orient='index')
                                            df_matches['matches_tuple'] = df_matches.index
                                            df_matches[['doc_1', 'doc_2']] = pd.DataFrame(list(df_matches['matches_tuple']), index=df_matches.index)
                                            df_matches = df_matches.drop(['matches_tuple', 0], axis=1)
                                            df_matches = df_matches.reset_index(drop=True)

                                            for comparison_method in attribute_pars.comparison_methods:
                                                df_att_matches = comparison.main(buckets[attribute_name][shingle_type][shingle_size][shingle_weight][buckets_type][signature_size][bands_number], docs_shingled[attribute_name][shingle_type][shingle_size][shingle_weight], comparison_method,
                                                                         shingles_weights_in_docs_dict[attribute_name][shingle_type][shingle_size][shingle_weight], matching_attribute, docs_mapping_old_new[attribute_name])

                                                df_matches = pd.merge(df_matches, df_att_matches, how='left', left_on=['doc_1', 'doc_2'],
                                                                      right_on=['doc_1', 'doc_2'])

                                                print("--Started postprocessing the results...")
                                                for attribute_threshold in attribute_pars.attribute_thresholds:
                                                    for attribute_weight in attribute_pars.attribute_weights:
                                                        print("----Started creating a weighted matching score...")
                                                        df_matches['match_score_{}'.format(matching_attribute)] = df_matches[
                                                                                                                      'match_score_{}'.format(
                                                                                                                          matching_attribute)] * attribute_weight
                                                        df_matches = df_matches[df_matches['match_score_{}'.format(matching_attribute)] >= attribute_threshold]

                                                        if mats.sum_score == 'sum':
                                                            print("----Started creating a common matching score...")
                                                            df_matches['match_score'] = df_matches.iloc[:, 2:].sum(axis=1)
                                                        elif mats.sum_score == 'none': #when only one attribute is used -> we don't need to sum up
                                                            df_matches['match_score'] = df_matches['match_score_{}'.format(matching_attribute)]
                                                        df_matches = df_matches.sort_values(by='match_score', ascending=False)

                                                        print("----Started adding matches attributes...")
                                                        df_matches_full = add_attributes_to_matches(df_matches, df_with_attributes)
                                                        print("----//Finished adding matches attributes...")

                                                        final_time = time.time() - processing_time
                                                        print("The whole algorithm took for {} size --- {} seconds ---".format(len(df_to_bucket.index), final_time))
                                                        print("----Started preparing results outputs and evaluation")
                                                        df_matches_with_estimation, labeled_number_of_matches, false_positive, false_negative, true_positive, true_negative = results_evaluation.main(df_matches_full, df_labeled_data, labeled_positive, labeled_negative)
                                                        df_matches_with_estimation = df_matches_with_estimation.sort_values(by='match_score', ascending=False)

                                                        if mats.experiment_mode == 'test':
                                                            df_matches_with_estimation.to_csv("df_results_{}_{}_{}.csv".format(mats.experiment_mode, mats.scenario_name, str(datetime.datetime.now())))

                                                        experiment_results = exporting_experiment_results.main(len(df_matches_with_estimation), mats.scenario_name, experiment_number, mats.dataset_size, final_time, labeled_number_of_matches, false_positive, false_negative, true_positive, true_negative, attribute_name, shingle_type, shingle_size, shingle_weight, buckets_type, signature_size, bands_number, attribute_threshold, attribute_weight)
                                                        df_experiment_results = df_experiment_results.append(experiment_results, ignore_index=True)

                                            del df_matches_full
                                            del df_matches
                                            del df_att_matches

                                        else:
                                            print('no matches found')
                                            experiment_results = exporting_experiment_results.main(0, mats.scenario_name,
                                                                                                   experiment_number, mats.dataset_size, 0,
                                                                                                   0, 0,
                                                                                                   0, 0, 0,
                                                                                                   attribute_name, shingle_type, shingle_size,
                                                                                                   shingle_weight, buckets_type, signature_size,
                                                                                                   bands_number, "no matches", "no matches")
                                            df_experiment_results = df_experiment_results.append(experiment_results, ignore_index=True)
    df_experiment_results = df_experiment_results.sort_values(by=['bands_number', 'signature_size','shingle_type', 'shingle_size', 'attribute_threshold'], ascending=True)
    df_experiment_results.loc[(df_experiment_results['precision'] >= 0.7) |(df_experiment_results['recall'] >= 0.7)|(df_experiment_results['f1score'] >= 0.7), 'result_07'] = 1

    df_experiment_results.to_csv("results_{}_{}.csv".format(mats.scenario_name, str(datetime.datetime.now())))

    del df_with_attributes
    print('end')
