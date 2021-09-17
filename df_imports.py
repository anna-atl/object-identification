#add other attributes here
import pandas as pd
import numpy as np
import sqlite3
from sqlite3 import Error

def df_prepare(df):
    '''
    This def cleans names from non text-number characters
    :param df: df with column name from raw datasets
    :return: df with cleaned names
    '''
    str_columns = ['name', 'country', 'state', 'city', 'zip', 'street', 'url', 'industry']
    df[str_columns] = df[str_columns].apply(lambda x: x.astype(str).str.upper())
    #should fix it - to apply above function only to _clean columns
    df['name_clean'] = df['name'].apply(lambda x: x.replace(';', ''))
    #df['name_clean'] = df['name_clean'].apply(lambda x: x.upper())
    df = df.drop(df[df['name_clean'] == '#NAME?'].index, inplace=False)
    df['name_clean'] = df['name_clean'].apply(lambda x: x.replace('.', ''))
    df['name_clean'] = df['name_clean'].str.replace('[^0-9a-zA-Z]+', ' ')
    df['name_clean'] = df['name_clean'].str.replace(' +', ' ')
    df['name_clean'] = df['name_clean'].replace(r'^\s*$', np.NaN, regex=True) #i dont know why the one beofre doesnt work
    df['name_clean'] = df['name_clean'].str.strip()
    df['name_clean'] = df['name_clean'].replace('NONE', np.nan)

    df['country_clean'] = df['country'].apply(lambda x: x.replace('.', ''))
    #df['country_clean'] = df['country_clean'].apply(lambda x: x.astype(str).str.upper())
    df['country_clean'] = df['country_clean'].str.replace('[^0-9a-zA-Z]+', ' ')
    df['country_clean'] = df['country_clean'].str.replace(' +', ' ')
    df['country_clean'] = df['country_clean'].str.strip()
    df['country_clean'] = df['country_clean'].replace('NONE', np.nan)

    df['state_clean'] = df['state'].apply(lambda x: x.replace('.', ''))
    #df['state_clean'] = df['state_clean'].apply(lambda x: x.astype(str).str.upper())
    df['state_clean'] = df['state_clean'].str.replace('[^0-9a-zA-Z]+', ' ')
    df['state_clean'] = df['state_clean'].str.replace(' +', ' ')
    df['state_clean'] = df['state_clean'].str.strip()
    df['state_clean'] = df['state_clean'].replace('NONE', np.nan)

    df['city_clean'] = df['city'].apply(lambda x: x.replace('.', ''))
    #df['city_clean'] = df['city_clean'].apply(lambda x: x.astype(str).str.upper())
    df['city_clean'] = df['city_clean'].str.replace('[^0-9a-zA-Z]+', ' ')
    df['city_clean'] = df['city_clean'].str.replace(' +', ' ')
    df['city_clean'] = df['city_clean'].str.strip()
    df['city_clean'] = df['city_clean'].replace('NONE', np.nan)

    df['zip_clean'] = df['zip'].apply(lambda x: x.replace('.', ''))
    #df['zip_clean'] = df['zip_clean'].apply(lambda x: x.astype(str).str.upper())
    df['zip_clean'] = df['zip_clean'].str.replace('[^0-9a-zA-Z]+', ' ')
    df['zip_clean'] = df['zip_clean'].str.replace(' +', ' ')
    df['zip_clean'] = df['zip_clean'].str.strip()
    df['zip_clean'] = df['zip_clean'].replace('NONE', np.nan)

    df['street_clean'] = df['street'].apply(lambda x: x.replace('.', ''))
    #df['street_clean'] = df['street_clean'].apply(lambda x: x.astype(str).str.upper())
    df['street_clean'] = df['street_clean'].str.replace('[^0-9a-zA-Z]+', ' ')
    df['street_clean'] = df['street_clean'].str.replace(' +', ' ')
    df['street_clean'] = df['street_clean'].str.strip()
    df['street_clean'] = df['street_clean'].replace('NONE', np.nan)

    df['url_clean'] = df['url'].str.replace('[^0-9a-zA-Z]+', ' ')
    #df['url_clean'] = df['url_clean'].apply(lambda x: x.astype(str).str.upper())
    df['url_clean'] = df['url_clean'].str.replace(' +', ' ')
    df['url_clean'] = df['url_clean'].str.replace('WWW ', '')
    df['url_clean'] = df['url_clean'].str.replace('HTTP ', '')
    df['url_clean'] = df['url_clean'].str.replace('HTTPS ', '')
    df['url_clean'] = df['url_clean'].str.strip()
    df['url_clean'] = df['url_clean'].replace('NONE', np.nan)

    df['industry_clean'] = df['industry'].apply(lambda x: x.replace('.', ''))
    #df['industry_clean'] = df['industry_clean'].apply(lambda x: x.astype(str).str.upper())
    df['industry_clean'] = df['industry_clean'].str.replace('[^0-9a-zA-Z]+', ' ')
    df['industry_clean'] = df['industry_clean'].str.replace(' +', ' ')
    df['industry_clean'] = df['industry_clean'].str.strip()
    df['industry_clean'] = df['industry_clean'].replace('NONE', np.nan)

    df = df.replace(r'^\s*$', np.NaN, regex=True)
    #df = df.dropna(subset=['url_clean'])
    df = df.dropna(how='all')
    df = df.reset_index(drop=True)

    return df

def df_import(dataset_size):
    database = "/Users/Annie/Dropbox/Botva/TUM/Master_Thesis/datasets/companies.db"
    # create a database connection
    conn = sqlite3.connect(database)
    df = pd.read_sql_query("SELECT * FROM companies WHERE url IS NOT NULL ORDER BY name LIMIT (?)", conn, params=(dataset_size,))
    #df = pd.read_sql_query("SELECT * FROM companies WHERE url IS NOT NULL LIMIT (?)", conn, params=(dataset_size,))
    #df = pd.read_sql_query("SELECT * FROM companies WHERE datasource <> 'peopledatalab' and url IS NOT NULL ORDER BY name LIMIT (?)", conn, params=(dataset_size,))

    return df

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

def import_labeled_data():
    labeled_data = "~/Dropbox/Botva/TUM/Master_Thesis/object-identification/labeled_data_final_2.csv"
    df_labeled_data = pd.read_csv(labeled_data, sep=';', error_bad_lines=False)
    df_labeled_data = df_labeled_data.dropna(subset=['is_duplicate'])
    df_labeled_data = df_labeled_data[['id_x', 'id_y', 'is_duplicate']] ##this is correct! (id_x, not doc_1 indexes)

    b = df_labeled_data.loc[df_labeled_data['id_x'] < df_labeled_data['id_y']] #should be fixed later
    c = df_labeled_data.loc[df_labeled_data['id_x'] > df_labeled_data['id_y']]
    c = c.rename(columns={'id_x': 'id_y', 'id_y': 'id_x'}, inplace=False)
    c = c[['id_x', 'id_y', 'is_duplicate']]
    df_labeled_data = b.append(c)

    return df_labeled_data

def find_labeled_data_in_df(df_labeled_data, df):
    df_labeled_data_in_df = pd.merge(df_labeled_data, df,  how='left', left_on=['id_x'], right_on=['id'])
    df_labeled_data_in_df = df_labeled_data_in_df.dropna(subset=['id'])
    df_labeled_data_in_df = df_labeled_data_in_df[['id_x', 'id_y', 'is_duplicate']]
    df_labeled_data_in_df = pd.merge(df_labeled_data_in_df, df,  how='left', left_on=['id_y'], right_on=['id'])
    df_labeled_data_in_df = df_labeled_data_in_df.dropna(subset=['id'])
    df_labeled_data_in_df = df_labeled_data_in_df[['id_x', 'id_y', 'is_duplicate']]

    #labeled_positive = df_labeled_data_in_df['is_duplicate'].loc[(df_labeled_data_in_df['is_duplicate'] == 1) | (df_labeled_data_in_df['is_duplicate'] == 2)].count()
    labeled_positive = df_labeled_data_in_df['is_duplicate'].loc[df_labeled_data_in_df['is_duplicate'] == 2].count()
    labeled_negative = df_labeled_data_in_df['is_duplicate'].loc[df_labeled_data_in_df['is_duplicate'] == 0].count()
    #labeled_negative = df_labeled_data_in_df['is_duplicate'].loc[(df_labeled_data_in_df['is_duplicate'] == 0) | (df_labeled_data_in_df['is_duplicate'] == 1)].count()
    labeled_matches_count = df_labeled_data_in_df['is_duplicate'].count()
    return df_labeled_data_in_df, labeled_positive, labeled_negative, labeled_matches_count

def main(dataset_size_to_import, matching_attribute, dataset_size):
    df = df_import(dataset_size_to_import)
    df = df_prepare(df)
    df = df.dropna(subset=['url_clean', 'name_clean'])
    try:
        df = df.sample(n=dataset_size)
    except:
        print('dataset size is larger than...')
    docs_mapping, docs = mapping_creation(df, matching_attribute)

    df_labeled_data = import_labeled_data()

    df_labeled_data_in_df, labeled_positive, labeled_negative, labeled_matches_count = find_labeled_data_in_df(df_labeled_data, df)

    return df, docs_mapping, docs, df_labeled_data

def add_attributes_to_matches(df_matches, df_with_attributes, docs_mapping):
    print("Started joining the result with the mapping table...")
    df_matches_full = pd.merge(df_matches, docs_mapping, how='left', left_on=['doc_1'], right_on=['new_index'])
    b = df_matches_full[(df_matches_full['doc_1'] == 20236)]
    print(b)

    df_matches_full = df_matches_full.drop(['new_index', 'old_index', 'doc_1'], axis=1)
    df_matches_full = pd.merge(df_matches_full, docs_mapping, how='left', left_on=['doc_2'], right_on=['new_index'])
    df_matches_full = df_matches_full.drop(['new_index', 'old_index', 'doc_2'], axis=1)
    df_matches_full = df_matches_full.rename(columns={'id_x': 'doc_1', 'id_y': 'doc_2'}, inplace=False)
    b = df_matches_full[(df_matches_full['doc_1'] == 2490742) | (df_matches_full['doc_2'] == 3121092)]
    print(b)

    b = df_matches_full.loc[df_matches_full['doc_1'] < df_matches_full['doc_2']]
    c = df_matches_full.loc[df_matches_full['doc_1'] > df_matches_full['doc_2']]
    c = c.rename(columns={'doc_1': 'doc_2', 'doc_2': 'doc_1'}, inplace=False)
    c = c[['match_score_{}'.format(attribute.matching_attribute), 'doc_1', 'doc_2']]
    df_matches_full = b.append(c)

    df_matches_full = pd.merge(df_matches, df,  how='left', left_on=['doc_1'], right_on=['id'])
    df_matches_full = df_matches_full.drop(['id'], axis=1)
    df_matches_full = pd.merge(df_matches_full, df,  how='left', left_on=['doc_2'], right_on=['id'])
    df_matches_full = df_matches_full.drop(['id'], axis=1)
    return df_matches_full