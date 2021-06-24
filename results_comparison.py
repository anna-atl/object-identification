import pandas as pd
import main_joint
import time
import datetime

class normal_matching_params:
    def __init__(self, matching_attribute, attribute_weight, matching_method):
        self.matching_attribute = matching_attribute
        self.matching_method = matching_method#jaccard etc?

class minhash_matching_params:
    def __init__(self, matching_attribute, hash_type, hash_weight, attribute_weight=1, shingle_size=0, bands_number=5, signature_size=50):
        self.matching_attribute = matching_attribute
        self.attribute_weight = attribute_weight
        self.hash_type = hash_type# token, shingle
        self.shingle_size = shingle_size# 1,2
        self.hash_weight = hash_weight# 'normal', 'frequency', 'weighted'
        self.bands_number = bands_number
        self.signature_size = signature_size

def import_labeled_data():
    labeled_data = "~/Dropbox/Botva/TUM/Master_Thesis/object-identification/labeled_data_2.csv"
    df_labeled_data = pd.read_csv(labeled_data, sep=';', error_bad_lines=False)
    df_labeled_data = df_labeled_data.dropna(subset=['is_duplicate'])
    df_labeled_data = df_labeled_data[['id_x', 'id_y', 'is_duplicate']]
    return df_labeled_data

if __name__ == "__main__":
    #test_mode
    #dataset_sizes = [100]
    #dataset_sizes = [100, 1000, 10000]
    dataset_sizes = [100, 1000, 10000]
    matching_attributes = ['name_clean']
    #matching_attributes = ['name_clean', 'url_clean']
    thresholds = [0.5]
    #thresholds = [0.5, 0.7, 0.9]
    #matching_methods = ['minhash', 'fuzzy', 'exact']
    matching_methods = ['minhash']
    #hash_types = [['token'], ['shingle', 3]]
    #hash_types = ['token', 'shingle']
    hash_types = ['shingle']
    hash_weights = ['normal']
    #hash_weights = ['normal', 'frequency', 'weighted']
    #shingle_sizes = [2, 3, 4]
    shingle_sizes = [2, 3]

    column_names = ['dataset_size', 'matching_attribute', 'threshold', 'matching_method', 'hash_type', 'shingles_size',
                    'hash_weight', 'total_time', 'number_of_matches', 'minhash_time', 'sign_creation_time', 'false_pos_rate']
    df_results = pd.DataFrame(columns=column_names)

    #ps1 = [minhash_matching_params('name_clean', 1, ['shingles', 3], 'normal')]

    matches_dfs = []

    df_labeled_data = import_labeled_data()

    for dataset_size in dataset_sizes:
        for matching_attribute in matching_attributes:
            for threshold in thresholds:
                for matching_method in matching_methods:
                    if matching_method == 'minhash':
                        for hash_type in hash_types:
                            for hash_weight in hash_weights:
                                if hash_type == 'shingle':
                                    for k in shingle_sizes:
                                        matching_params = [
                                            minhash_matching_params(matching_attribute, hash_type, hash_weight, shingle_size = k)]
                                        df_all_matches, all_time = main_joint.main(dataset_size, threshold,
                                                                                   matching_method, matching_params)
                                        df_matches_estimation = pd.merge(df_all_matches, df_labeled_data, how='left',
                                                                   left_on=['doc_1', 'doc_2'], right_on=['id_x', 'id_y'])
                                        df_matches_estimation = df_matches_estimation.drop(['id_x_x', 'id_x_y', 'id_y_x', 'id_y_y'], axis=1)

                                        #matches_dfs.append(df_all_matches)
                                        experiment = {'dataset_size': dataset_size,
                                                      'matching_attribute': matching_params[0].matching_attribute,# [0]should be fixed
                                                      'threshold': threshold,
                                                      'matching_method': matching_method,
                                                      'hash_type': matching_params[0].hash_type,
                                                      'shingles_size': matching_params[0].shingle_size,
                                                      'hash_weight': matching_params[0].hash_weight,
                                                      'total_time': all_time,
                                                      'number_of_matches': len(df_all_matches)
                                                      # 'minhash_time', 'sign_creation_time', 'false_pos_rate'
                                                      }
                                        df_results = df_results.append(experiment, ignore_index=True)
                                elif hash_type == 'token':
                                    matching_params = [minhash_matching_params(matching_attribute, hash_type, hash_weight)]
                                    df_all_matches, all_time = main_joint.main(dataset_size, threshold, matching_method, matching_params)
                                    #matches_dfs.append(df_all_matches)
                                    experiment = {'dataset_size': dataset_size,
                                                  'matching_attribute': matching_params[0].matching_attribute,#[0]should be fixed
                                                  'threshold': threshold,
                                                  'matching_method': matching_method,
                                                  'hash_type': matching_params[0].hash_type,
                                                  'shingles_size': matching_params[0].shingle_size,
                                                  'hash_weight': matching_params[0].hash_weight,
                                                  'total_time': all_time,
                                                  'number_of_matches': len(df_all_matches)
                                                  # 'minhash_time', 'sign_creation_time', 'false_pos_rate'
                                                  }
                                    df_results = df_results.append(experiment, ignore_index=True)
                            else: continue
    df_results.to_csv("df_results_{}.csv".format(str(datetime.datetime.now())))


    '''
    df_all_matches, all_time = main_joint.main(dataset_size, matching_attribute,
                                               matching_method, hash_type, shingle_size, hash_weight
                #[minhash_matching_params(matching_attribute, attribute_weight, hash_type, weights_method)], 0) #hardcoded threshold
                            if hash_type[0] == 'tokens':
                    df_all_matches = main_joint.main(dataset_size, [minhash_matching_params(matching_attribute, attribute_weight, hash_type, weights_method)], 0) #hardcoded threshold
                elif hash_type[0] == 'shingles':
                    df_all_matches = main_joint.main(dataset_size, [minhash_matching_params('name_clean', 1, ['shingles', 3], 'normal')], 0) #hardcoded threshold
           for hash_feature in hash_type:
    '''
    #df_all_matches = main_joint.main(dataset_size, ps1, threshold)




print('end')
