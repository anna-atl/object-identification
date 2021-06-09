#add other attributes here
import pandas as pd
import numpy as np

def df_prepare(df):
    '''
    This def cleans names from non text-number characters
    :param df: df with column name from raw datasets
    :return: df with cleaned names
    '''
    df = df.apply(lambda x: x.astype(str).str.upper())
    #should fix it - to apply above function only to _clean columns
    df['name_clean'] = df['name'].apply(lambda x: x.replace(';', ''))
    df = df.drop(df[df['name_clean'] == '#NAME?'].index, inplace=False)
    df['name_clean'] = df['name_clean'].apply(lambda x: x.replace('.', ''))
    df['name_clean'] = df['name_clean'].str.replace('[^0-9a-zA-Z]+', ' ')
    df['name_clean'] = df['name_clean'].str.replace(' +', ' ')
    df['name_clean'] = df['name_clean'].str.strip()
    df['name_clean'] = df['name_clean'].replace(r'^\s*$', np.NaN, regex=True) #i dont know why the one beofre doesnt work
    df['name_clean'] = df['name_clean'].replace('NAN', np.nan)

    df['country_clean'] = df['country'].apply(lambda x: x.replace('.', ''))
    df['country_clean'] = df['country_clean'].str.replace('[^0-9a-zA-Z]+', ' ')
    df['country_clean'] = df['country_clean'].str.replace(' +', ' ')
    df['country_clean'] = df['country_clean'].str.strip()
    df['country_clean'] = df['country_clean'].replace('NAN', np.nan)

    df['city_clean'] = df['city'].apply(lambda x: x.replace('.', ''))
    df['city_clean'] = df['city_clean'].str.replace('[^0-9a-zA-Z]+', ' ')
    df['city_clean'] = df['city_clean'].str.replace(' +', ' ')
    df['city_clean'] = df['city_clean'].str.strip()
    df['city_clean'] = df['city_clean'].replace('NAN', np.nan)

    df['street_clean'] = df['street'].apply(lambda x: x.replace('.', ''))
    df['street_clean'] = df['street_clean'].str.replace('[^0-9a-zA-Z]+', ' ')
    df['street_clean'] = df['street_clean'].str.replace(' +', ' ')
    df['street_clean'] = df['street_clean'].str.strip()
    df['street_clean'] = df['street_clean'].replace('NAN', np.nan)

    df['url_clean'] = df['url'].str.replace('[^0-9a-zA-Z]+', ' ')
    df['url_clean'] = df['url_clean'].str.replace(' +', ' ')
    df['url_clean'] = df['url_clean'].str.replace('WWW ', '')
    df['url_clean'] = df['url_clean'].str.replace('HTTP ', '')
    df['url_clean'] = df['url_clean'].str.replace('HTTPS ', '')
    df['url_clean'] = df['url_clean'].str.strip()
    df['url_clean'] = df['url_clean'].replace('NAN', np.nan)
    df = df.sort_values(by=['url_clean'])

    df = df.dropna()
#    print(df)
    return df

def df_import():
    '''
    This function imports all dfs, keeps only comp names, created datasource column with datasource file info
    and merges all dfs together
    :return: merged df with columns name and datasource
    '''

    # import FR data (adjustments for delimeters and encoding - latin)
    wordbook_name_1 = "~/Dropbox/Botva/TUM/Master_Thesis/datasets/processed_files/france_rna_processed.csv"
    print('Started to import from', wordbook_name_1)
    #test mode:
    #df_1 = pd.read_csv(wordbook_name_1, encoding='latin-1', sep=';', error_bad_lines=False, nrows=10)
    df_1 = pd.read_csv(wordbook_name_1, encoding='latin-1', sep = ';', error_bad_lines=False) #error_bad_lines=False skips bad data
    df_1 = df_1[['name', 'country', 'city', 'zip', 'street', 'url']]
    df_1['datasource'] = 'rna'
    df = df_1
    del df_1

    wordbook_name_2 = "~/Dropbox/Botva/TUM/Master_Thesis/datasets/raw_files/companies_sorted.csv"
    print('Started to import from', wordbook_name_2)
    #test mode:
    #df_2 = pd.read_csv(wordbook_name_2, error_bad_lines=False, nrows=10)
    df_2 = pd.read_csv(wordbook_name_2, error_bad_lines=False)
    df_2 = df_2.rename(columns={'CompanyName': 'name', 'domain': 'url'})
    df_2['city'] = np.nan #should be changed to location
    df_2['zip'] = np.nan
    df_2['street'] = np.nan
    df_2 = df_2[['name', 'country', 'city', 'zip', 'street', 'url']]
    df_2['datasource'] = 'peopledatalab'
    df = df.append(df_2)
    del df_2

    wordbook_name_3 = "~/Dropbox/Botva/TUM/Master_Thesis/datasets/raw_files/list-of-companies-in-austria.csv"
    print('Started to import from', wordbook_name_3)
    #test mode:
    #df_3 = pd.read_csv(wordbook_name_3, error_bad_lines=False, nrows=10)
    df_3 = pd.read_csv(wordbook_name_3, error_bad_lines=False)
    df_3['country'] = 'austria'
    df_3['city'] = np.nan
    df_3['zip'] = np.nan
    df_3['street'] = np.nan
    df_3['url'] = np.nan
    df_3 = df_3[['name', 'country', 'city', 'zip', 'street', 'url']]
    df_3['datasource'] = 'powrbot_austria'
    df = df.append(df_3)
    del df_3

    ''' this dataset is temporarly not used because of it's size
    wordbook_name_4 = "~/Dropbox/Botva/TUM/Master_Thesis/datasets/raw_files/BasicCompanyDataAsOneFile-2021-02-01.csv"
    print('Started to import from', wordbook_name_4)
    #test mode:
    #df_4 = pd.read_csv(wordbook_name_4, error_bad_lines=False, nrows=10)
    df_4 = pd.read_csv(wordbook_name_4, error_bad_lines=False)
    df_4 = df_4.rename(columns={'CompanyName': 'name', 'RegAddress.AddressLine1': 'street', 'RegAddress.PostCode': 'zip', 'RegAddress.PostTown': 'city', 'CountryOfOrigin': 'country'})
    df_4['url'] = np.nan
    df_4 = df_4[['name', 'country', 'city', 'zip', 'street', 'url']]
    df_4['datasource'] = 'basiccomp'
    df = df.append(df_4)
    del df_4
    '''

    wordbook_name_5 = "~/Dropbox/Botva/TUM/Master_Thesis/datasets/raw_files/list-of-companies-in-belgium.csv"
    print('Started to import from', wordbook_name_5)
    #test mode:
    #df_5 = pd.read_csv(wordbook_name_5, error_bad_lines=False, nrows=10)
    df_5 = pd.read_csv(wordbook_name_5, error_bad_lines=False)
    df_5 = df_5.rename(columns={"name;;": "name"})
    df_5['country'] = 'belgium'
    df_5['city'] = np.nan
    df_5['zip'] = np.nan
    df_5['street'] = np.nan
    df_5['url'] = np.nan
    df_5 = df_5[['name', 'country', 'city', 'zip', 'street', 'url']]
    df_5['datasource'] = 'powrbot_belgium'
    df = df.append(df_5)
    del df_5

    wordbook_name_6 = "~/Dropbox/Botva/TUM/Master_Thesis/datasets/raw_files/list-of-companies-in-france.csv"
    print('Started to import from', wordbook_name_6)
    #test mode:
    #df_6 = pd.read_csv(wordbook_name_6, error_bad_lines=False, nrows=10)
    df_6 = pd.read_csv(wordbook_name_6, error_bad_lines=False)
    df_6 = df_6.rename(columns={"name;": "name"})
    df_6['country'] = 'france'
    df_6['city'] = np.nan
    df_6['zip'] = np.nan
    df_6['street'] = np.nan
    df_6['url'] = np.nan
    df_6 = df_6[['name', 'country', 'city', 'zip', 'street', 'url']]
    df_6['datasource'] = 'powrbot_france'
    df = df.append(df_6)
    del df_6

    wordbook_name_7 = "~/Dropbox/Botva/TUM/Master_Thesis/datasets/raw_files/list-of-companies-in-germany.csv"
    print('Started to import from', wordbook_name_7)
    #test mode:
    #df_7 = pd.read_csv(wordbook_name_7, error_bad_lines=False, nrows=10)
    df_7 = pd.read_csv(wordbook_name_7, error_bad_lines=False)
    df_7 = df_7.rename(columns={"name;": "name"})
    df_7['country'] = 'germany'
    df_7['city'] = np.nan
    df_7['zip'] = np.nan
    df_7['street'] = np.nan
    df_7['url'] = np.nan
    df_7 = df_7[['name', 'country', 'city', 'zip', 'street', 'url']]
    df_7['datasource'] = 'powrbot_germany'
    df = df.append(df_7)
    del df_7

    wordbook_name_8 = "~/Dropbox/Botva/TUM/Master_Thesis/datasets/raw_files/list-of-companies-in-united-kingdom.csv"
    print('Started to import from', wordbook_name_8)
    #test mode:
    #df_8 = pd.read_csv(wordbook_name_8, error_bad_lines=False, nrows=10)
    df_8 = pd.read_csv(wordbook_name_8, error_bad_lines=False)
    df_8 = df_8.rename(columns={"name;;": "name"})
    df_8['country'] = 'united kingdom'
    df_8['city'] = np.nan
    df_8['zip'] = np.nan
    df_8['street'] = np.nan
    df_8['url'] = np.nan
    df_8 = df_8[['name', 'country', 'city', 'zip', 'street', 'url']]
    df_8['datasource'] = 'powrbot_uk'
    df = df.append(df_8)
    del df_8

    print('Started cleaning up the input data')
    df = df_prepare(df)
    df = df.sort_values(by=['name'])
    df = df.reset_index(drop=True)
#    print(df)
#    print(df.dtypes)
    return df