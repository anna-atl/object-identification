import pandas as pd
import numpy as np

def mapping_creation(df, matching_attribute):
    docs = df[['id', matching_attribute]]
    docs = docs.dropna(subset=[matching_attribute])
    docs_mapping = docs
    docs = docs[matching_attribute]
    print(docs)

    print('------------------------------------------------')
    print('The attribute {} has {} records'.format(matching_attribute, len(docs)))

    docs_mapping_dict = {}
    docs_mapping = docs_mapping.dropna(subset=[matching_attribute])
    docs_mapping['old_index'] = docs_mapping.index
    docs_mapping = docs_mapping.reset_index(drop=True)
    docs_mapping['new_index'] = docs_mapping.index
    docs_mapping = docs_mapping.drop([matching_attribute, 'id'], axis=1)
    docs_mapping_dict = docs_mapping.set_index('new_index').T.to_dict('list')
    docs_mapping_dict = {k: v[0] for k, v in docs_mapping_dict.items()}
    return docs_mapping_dict, docs

def main(df, attribute_params, dataset_size):
    attributes_to_bucket = {k: v for k, v in attribute_params.items() if v.buckets_type != 'no buckets'}
    df_to_bucket = df.dropna(subset=[v.matching_attribute for k, v in attributes_to_bucket.items()])
    try:
        df_to_bucket = df_to_bucket.sample(n=dataset_size)
    except:
        print('dataset size is larger than...')

    docs_mapping = {}
    docs = {}

    for attribute_name, attribute_pars in attribute_params.items():
        docs_mapping[attribute_name], docs[attribute_name] = mapping_creation(df_to_bucket, attribute_pars.matching_attribute)

    return df_to_bucket, docs_mapping, docs
