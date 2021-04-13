import pandas as pd
import time
import numpy as np
import string
import collections
#import binascii
import datetime
import df_imports

import random
pd.set_option('display.max_columns', None)


class attribute_match_params:
#    split_method = 1 #1 - tokens, 2 - shingles, 3 - shingles from tokens
#    weights_method = 1 # 1 - no weights, 2 - frequency weights, 3 - sophisticated weights

    def __init__(self, matching_attributes, split_method, weights_method, bands_number = 5, signature_size = 50, shingle_size = 3):
        self.matching_attributes = matching_attributes
        self.split_method = split_method
        self.weights_method = weights_method
        self.shingle_size = shingle_size
        self.signature_size = signature_size
"""
class scenario_params:
    df_size = 1000
    time_spent = 1
    df_matches_output =

    def __init__(self, df_size):
        pass
"""
def main(df, n):
    texts = df['name_clean'] #which column to use for minhash
    tokens_weights_dict, tokens_set, tokens_dict = create_tokens_dict(texts)

    docs, token_weights = create_doc_tokens(texts, tokens_weights_dict, tokens_dict)

    signature_size = 50
    signatures = create_signatures_array(docs, signature_size, tokens_set)


    buckets_bands = create_buckets(signatures, bands_number)

    matches = create_matches(buckets_bands, docs, token_weights)

    df_matches_full = create_df_with_attributes(matches, df)
    del df


if __name__ == "__main__":
    datasets_size = [1000000]

    s1 = attribute_match_params('name', 1, 1)
#    print(s1.matching_attributes, s1.split_method, s1.weights_method, s1.shingle_size, s1.signature_size)

    start_time = time.time()
    df = df_imports.df_import()
    print("Importing datasets took --- %s seconds ---" % (time.time() - start_time))

    for n in datasets_size:
        start_time = time.time()
        print('Started working with the dataset (%s size):' % n)
        a, df_matches_full = main(df.head(n), n)
        main(parameters)

        df_matches_full.to_csv("df_matches_full_{}_{}.csv".format(n, datetime.date))
        time_spent.append(a)
        df_matches_outputs.append(df_matches_full)
        print("Whole algorithm took --- %s seconds ---" % (time.time() - start_time))


    for a, n in zip(time_spent, datasets_size):
        print('for {} dataset size it took {} seconds'.format(n, a))
        print('end')
