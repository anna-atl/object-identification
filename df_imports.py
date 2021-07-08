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
    #df = df.apply(lambda x: x.astype(str).str.upper())
    str_columns = ['name', 'country', 'state', 'city', 'zip', 'street', 'url', 'industry']
    df[str_columns] = df[str_columns].apply(lambda x: x.astype(str).str.upper())
    #should fix it - to apply above function only to _clean columns
    df['name_clean'] = df['name'].apply(lambda x: x.replace(';', ''))
    #df['name_clean'] = df['name_clean'].apply(lambda x: x.upper())
    df = df.drop(df[df['name_clean'] == '#NAME?'].index, inplace=False)
    df['name_clean'] = df['name_clean'].apply(lambda x: x.replace('.', ''))
    df['name_clean'] = df['name_clean'].str.replace('[^0-9a-zA-Z]+', ' ')
    df['name_clean'] = df['name_clean'].str.replace(' +', ' ')
    df['name_clean'] = df['name_clean'].str.strip()
    df['name_clean'] = df['name_clean'].replace(r'^\s*$', np.NaN, regex=True) #i dont know why the one beofre doesnt work
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

    #df = df.dropna(subset=['url_clean'])
    df = df.dropna(how='all')
#    df = df.dropna()
    return df

def df_import(dataset_size):
    database = "/Users/Annie/Dropbox/Botva/TUM/Master_Thesis/datasets/companies.db"

    # create a database connection
    conn = sqlite3.connect(database)
    #labeled data mode
    #df = pd.read_sql_query("SELECT * FROM companies WHERE url IS NOT NULL ORDER BY name LIMIT (?)", conn, params=(dataset_size,))
    #df = pd.read_sql_query("SELECT * FROM companies WHERE url IS NOT NULL LIMIT (?)", conn, params=(dataset_size,))
    df = pd.read_sql_query("SELECT * FROM companies WHERE url IS NOT NULL LIMIT (?)", conn, params=(dataset_size,))
    #df = pd.read_sql_query("SELECT * FROM companies WHERE datasource <> 'peopledatalab' LIMIT (?)", conn, params=(dataset_size,))

    df = df_prepare(df)
    df = df.reset_index(drop=True)
#    print(df)
    print(df.dtypes)
    return df

