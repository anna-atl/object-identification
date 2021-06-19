import pandas as pd
import main_joint

class normal_matching_params:
    def __init__(self, matching_attribute, attribute_weight, matching_method):
        self.matching_attribute = matching_attribute
        self.matching_method = matching_method#jaccard etc?

class minhash_matching_params:
    def __init__(self, matching_attribute, attribute_weight, hash_type, weights_method, bands_number=5, signature_size=50):
        self.matching_attribute = matching_attribute
        self.attribute_weight = attribute_weight
        self.hash_type = hash_type#[tokens], [shingles, shingle_size]
        self.weights_method = weights_method# 'normal', 'frequency', 'weighted'
        self.bands_number = bands_number
        self.signature_size = signature_size

def import_labeled_data():
    labeled_data = "~/Dropbox/Botva/TUM/Master_Thesis/object-identification/labeled_data_2.csv"
    df_labeled_data = pd.read_csv(labeled_data, error_bad_lines=False)
    df_labeled_data = df_labeled_data.dropna(subset=['is_duplicate'])
    df_labeled_data = df_labeled_data[['match_score_name_clean', 'id_x', 'id_y', 'is_duplicate']]
    return df_labeled_data

if __name__ == "__main__":
    dataset_sizes = [100, 1000, 10000, 100000, 1000000]
    thresholds = [0.5, 0.6, 0.7, 0.8, 0.9]
    shingle_sizes = [2, 3, 4]
    hash_types = [['tokens'], ['shingles', shingle_sizes]]
    weights_methods = ['normal', 'frequency', 'weighted']

    ps1 = [minhash_matching_params('name_clean', 1, ['shingles', 3], 'normal')]

    df_all_matches = main_joint.main(dataset_size, ps1, threshold)

    df_labeled_data = import_labeled_data()



