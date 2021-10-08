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

    docs_mapping = docs_mapping.dropna(subset=[matching_attribute])
    docs_mapping['old_index'] = docs_mapping.index
    docs_mapping = docs_mapping.reset_index(drop=True)
    docs_mapping['new_index'] = docs_mapping.index
    docs_mapping = docs_mapping.drop([matching_attribute], axis=1)
    return docs_mapping, docs

def main(df, attribute_params, dataset_size):
    attributes_to_bucket = {k: v for k, v in attribute_params.items() if v.buckets_type != 'no buckets'}
    df = df.dropna(subset=[v.matching_attribute for k, v in attributes_to_bucket.items()])
    try:
        df = df.sample(n=dataset_size)
    except:
        print('dataset size is larger than...')

    docs_mapping = {}
    docs = {}

    for attribute_name, attribute_pars in attributes_to_bucket.items():
        docs_mapping[attribute_name], docs[attribute_name] = mapping_creation(df, attribute_pars.matching_attribute)

    return df, docs_mapping, docs
