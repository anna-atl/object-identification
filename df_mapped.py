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

def main(df, matching_attribute, dataset_size):
    df = df.dropna(subset=['url_clean', 'name_clean'])
    try:
        df = df.sample(n=dataset_size)
    except:
        print('dataset size is larger than...')
    docs_mapping, docs = mapping_creation(df, matching_attribute)

    return df, docs_mapping, docs
