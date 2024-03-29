import pandas as pd
import numpy as np
import time

def create_mapping(df, matching_attribute):
    docs_to_match = df[['id', matching_attribute]]
    docs_to_match = docs_to_match.dropna(subset=[matching_attribute])
    docs_mapping = docs_to_match
    docs_to_match = docs_to_match[matching_attribute]

    print('The attribute {} has {} records'.format(matching_attribute, len(docs_to_match)))

    docs_mapping = docs_mapping.dropna(subset=[matching_attribute])
    docs_mapping['old_index'] = docs_mapping.index
    docs_mapping = docs_mapping.reset_index(drop=True)
    docs_mapping['new_index'] = docs_mapping.index
    docs_mapping = docs_mapping.drop([matching_attribute, 'id'], axis=1)
    docs_mapping_dict_new_old = docs_mapping.set_index('new_index').T.to_dict('list')
    docs_mapping_dict_new_old = {k: v[0] for k, v in docs_mapping_dict_new_old.items()}
    docs_mapping_dict_old_new = docs_mapping.set_index('old_index').T.to_dict('list')
    docs_mapping_dict_old_new = {k: v[0] for k, v in docs_mapping_dict_old_new.items()}
    return docs_mapping_dict_new_old, docs_mapping_dict_old_new, docs_to_match

def main(df, dataset_size, experiment_mode, attribute_params):
    for attribute_name, attribute_pars in attribute_params.items():
        if (experiment_mode != 'individual combinations') and (experiment_mode != 'test'):
            attributes_to_bucket = {k: v for k, v in attribute_params.items() if v.buckets_type != 'no buckets'}
            df_to_bucket = df.dropna(subset=[v.matching_attribute for k, v in attributes_to_bucket.items()])
            try:
                df_to_bucket = df_to_bucket.sample(n=dataset_size)
            except:
                print('Warning: dataset size {} is larger than the imported {} dataset size'.format(dataset_size, len(df_to_bucket.index)))
        elif (experiment_mode == 'individual combinations') or (experiment_mode == 'test'):
            for buckets_type in attribute_pars.buckets_types:
                for matching_attribute in attribute_pars.matching_attributes:
                    if buckets_type != 'no buckets':
                        df_to_bucket = df.dropna(subset=[matching_attribute])
                    try:
                        df_to_bucket = df_to_bucket.sample(n=dataset_size)
                    except:
                        print('Warning: dataset size {} is larger than the imported {} dataset size'.format(dataset_size, len(df_to_bucket.index)))

    docs_mapping_new_old = {}
    docs_mapping_old_new = {}
    docs_to_match = {}

    start_time = time.time()

    for attribute_name, attribute_pars in attribute_params.items():
        for matching_attribute in attribute_pars.matching_attributes:
            print('Started to create documents mapping')
            docs_mapping_new_old[attribute_name], docs_mapping_old_new[attribute_name], docs_to_match[attribute_name] = create_mapping(df_to_bucket, matching_attribute)
            print("Creating mapped documents took --- %s seconds ---" % (time.time() - start_time))

    return df_to_bucket, docs_mapping_new_old, docs_mapping_old_new, docs_to_match
