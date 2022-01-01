import pandas as pd
import numpy as np
import time

def import_labeled_data():
    labeled_data = "labeled_data/labeled_data_final_2.csv"
    df_labeled_data = pd.read_csv(labeled_data, sep=';', error_bad_lines=False)
    df_labeled_data = df_labeled_data.dropna(subset=['is_duplicate'])
    df_labeled_data = df_labeled_data.loc[(df_labeled_data['is_duplicate'] == 2) | (df_labeled_data['is_duplicate'] == 0)]
    df_labeled_data = df_labeled_data[['id_x', 'id_y', 'is_duplicate', 'name_x', 'name_y']] ##this is correct! (id_x, not doc_1 indexes)

    b = df_labeled_data.loc[df_labeled_data['id_x'] < df_labeled_data['id_y']] #should be fixed later
    c = df_labeled_data.loc[df_labeled_data['id_x'] > df_labeled_data['id_y']]
    c = c.rename(columns={'id_x': 'id_y', 'id_y': 'id_x', 'name_x': 'name_y', 'name_y': 'name_x'}, inplace=False)
    c = c[['id_x', 'id_y', 'name_x', 'name_y', 'is_duplicate']]
    df_labeled_data = b.append(c)

    return df_labeled_data

def find_labeled_data_in_df(df_labeled_data, df):
    df_labeled_data_in_df = pd.merge(df_labeled_data, df,  how='left', left_on=['id_x'], right_on=['id'])
    df_labeled_data_in_df = df_labeled_data_in_df.dropna(subset=['id'])
    df_labeled_data_in_df = df_labeled_data_in_df[['id_x', 'id_y', 'name_x', 'name_y', 'is_duplicate']]
    df_labeled_data_in_df = pd.merge(df_labeled_data_in_df, df,  how='left', left_on=['id_y'], right_on=['id'])
    df_labeled_data_in_df = df_labeled_data_in_df.dropna(subset=['id'])
    df_labeled_data_in_df = df_labeled_data_in_df[['id_x', 'id_y', 'is_duplicate', 'name_x', 'name_y']]

    #labeled_positive = df_labeled_data_in_df['is_duplicate'].loc[(df_labeled_data_in_df['is_duplicate'] == 1) | (df_labeled_data_in_df['is_duplicate'] == 2)].count()
    labeled_positive = df_labeled_data_in_df['is_duplicate'].loc[df_labeled_data_in_df['is_duplicate'] == 2].count()
    labeled_negative = df_labeled_data_in_df['is_duplicate'].loc[df_labeled_data_in_df['is_duplicate'] == 0].count()
    #labeled_negative = df_labeled_data_in_df['is_duplicate'].loc[(df_labeled_data_in_df['is_duplicate'] == 0) | (df_labeled_data_in_df['is_duplicate'] == 1)].count()
    labeled_matches_count = df_labeled_data_in_df['is_duplicate'].count()
    return df_labeled_data_in_df, labeled_positive, labeled_negative, labeled_matches_count

def main(df):
    print('Started to download the labeled data')
    df_labeled_data = import_labeled_data()

    start_time = time.time()
    print('Identifying which labeled data is in imported df')
    df_labeled_data_in_df, labeled_positive, labeled_negative, labeled_matches_count = find_labeled_data_in_df(df_labeled_data, df)
    print("Merging labeled dataset with df took --- %s seconds ---" % (time.time() - start_time))
    print('------------------------------------------------')

    return df_labeled_data_in_df, labeled_positive, labeled_negative, labeled_matches_count
