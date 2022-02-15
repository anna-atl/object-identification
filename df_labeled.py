import pandas as pd
import numpy as np
import time

def import_labeled_data():
    labeled_data = "labeled_data/labeled_data_final_2.csv"
    df_labeled_data = pd.read_csv(labeled_data, sep=';', error_bad_lines=False)
    df_labeled_data = df_labeled_data.dropna(subset=['is_duplicate'])
    #df_labeled_data = df_labeled_data.loc[(df_labeled_data['is_duplicate'] == 2) | (df_labeled_data['is_duplicate'] == 0)]
    df_labeled_data = df_labeled_data.loc[df_labeled_data['is_duplicate'] == 0]

    df_labeled_data = df_labeled_data[['id_x', 'id_y', 'is_duplicate', 'name_x', 'name_y']] ##this is correct! (id_x, not doc_1 indexes)

    b = df_labeled_data.loc[df_labeled_data['id_x'] < df_labeled_data['id_y']] #should be fixed later
    c = df_labeled_data.loc[df_labeled_data['id_x'] > df_labeled_data['id_y']]
    c = c.rename(columns={'id_x': 'id_y', 'id_y': 'id_x', 'name_x': 'name_y', 'name_y': 'name_x'}, inplace=False)
    c = c[['id_x', 'id_y', 'name_x', 'name_y', 'is_duplicate']]
    df_labeled_data = b.append(c)

    return df_labeled_data

def import_labeled_data_2():
    labeled_data = "labeled_data/uk_companies_2_M_5.csv"
    df_labeled_data_imported = pd.read_csv(labeled_data, sep=',', error_bad_lines=False)
    df_labeled_data = df_labeled_data_imported.dropna(subset=['duplicate_id'])
    df_labeled_data = df_labeled_data[['id', 'duplicate_id']] ##this is correct! (id_x, not doc_1 indexes)
    df_labeled_data = df_labeled_data.rename(columns={'id': 'id_x', 'duplicate_id': 'id_y'}, inplace=False)
    df_labeled_data['is_duplicate'] = 2
    b = df_labeled_data.loc[df_labeled_data['id_x'] < df_labeled_data['id_y']] #should be fixed later
    c = df_labeled_data.loc[df_labeled_data['id_x'] > df_labeled_data['id_y']]
    c = c.rename(columns={'id_x': 'id_y', 'id_y': 'id_x'}, inplace=False)
    c = c[['id_x', 'id_y', 'is_duplicate']]
    df_labeled_data = b.append(c)

    df_labeled_data_2 = df_labeled_data_imported.dropna(subset=['not_duplicate'])
    df_labeled_data_2 = df_labeled_data_2[['id', 'not_duplicate']] ##this is correct! (id_x, not doc_1 indexes)
    df_labeled_data_2 = df_labeled_data_2.rename(columns={'id': 'id_x', 'not_duplicate': 'id_y'}, inplace=False)
    df_labeled_data_2['is_duplicate'] = 0
    d = df_labeled_data_2.loc[df_labeled_data_2['id_x'] < df_labeled_data_2['id_y']] #should be fixed later
    e = df_labeled_data_2.loc[df_labeled_data_2['id_x'] > df_labeled_data_2['id_y']]
    e = e.rename(columns={'id_x': 'id_y', 'id_y': 'id_x'}, inplace=False)
    e = e[['id_x', 'id_y', 'is_duplicate']]
    df_labeled_data_2 = d.append(e)

    df_labeled_data = df_labeled_data.append(df_labeled_data_2)
    return df_labeled_data


def import_labeled_data_3():
    labeled_data = "labeled_data/uk_companies_2_M_6.csv"
    df_labeled_data_imported = pd.read_csv(labeled_data, sep=',', error_bad_lines=False)

    df_labeled_data = df_labeled_data_imported.iloc[:649]
    df_labeled_data_2 = df_labeled_data_imported.iloc[43364:43650]
    df_labeled_data = df_labeled_data.append(df_labeled_data_2)
    df_labeled_data = df_labeled_data[df_labeled_data.is_related.isnull()]

    df_labeled_data = df_labeled_data[['id', 'duplicate_id']]  ##this is correct! (id_x, not doc_1 indexes)
    #df_labeled_data.loc[df_labeled_data.duplicate_id.isnull(), 'duplicate_id'] = -1
    df_labeled_data["duplicate_id"] = df_labeled_data["duplicate_id"].fillna(-1)

    duplicates_dict = {}
    for index, row in df_labeled_data.iterrows():
        duplicates_dict.setdefault(row['duplicate_id'], []).append(row['id'])

    duplicates_list = {}
    non_duplicates_list = {}
    for group, ids in duplicates_dict.items():
        if group != - 1:
            for id_1 in ids:
                for id_2 in ids:
                    if id_2 > id_1:
                        duplicates_list[id_1, id_2] = 2
        elif group == - 1:
            for id_1 in ids:
                for id_2 in ids:
                    if id_2 > id_1:
                        non_duplicates_list[id_1, id_2] = 0

    df_labeled_data = pd.DataFrame.from_dict(duplicates_list, orient='index')
    df_labeled_data_non_dupl = pd.DataFrame.from_dict(non_duplicates_list, orient='index')
    df_labeled_data = df_labeled_data.append(df_labeled_data_non_dupl)
    df_labeled_data['docs'] = df_labeled_data.index
    df_labeled_data[['id_x', 'id_y']] = pd.DataFrame(list(df_labeled_data['docs']), index=df_labeled_data.index)
    df_labeled_data['is_duplicate'] = df_labeled_data[0]
    df_labeled_data = df_labeled_data.drop(['docs', 0], axis=1)
    df_labeled_data = df_labeled_data.reset_index(drop=True)

    return df_labeled_data


def find_labeled_data_in_df(df_labeled_data, df):
    df_labeled_data_in_df = pd.merge(df_labeled_data, df,  how='left', left_on=['id_x'], right_on=['id'])
    df_labeled_data_in_df = df_labeled_data_in_df.dropna(subset=['id'])
    #df_labeled_data_in_df = df_labeled_data_in_df[['id_x', 'id_y', 'name_x', 'name_y', 'is_duplicate']]
    df_labeled_data_in_df = df_labeled_data_in_df[['id_x', 'id_y', 'is_duplicate']]
    df_labeled_data_in_df = pd.merge(df_labeled_data_in_df, df,  how='left', left_on=['id_y'], right_on=['id'])
    df_labeled_data_in_df = df_labeled_data_in_df.dropna(subset=['id'])
    df_labeled_data_in_df = df_labeled_data_in_df[['id_x', 'id_y', 'is_duplicate']]
    #df_labeled_data_in_df = df_labeled_data_in_df[['id_x', 'id_y', 'is_duplicate', 'name_x', 'name_y']]

    #labeled_positive = df_labeled_data_in_df['is_duplicate'].loc[(df_labeled_data_in_df['is_duplicate'] == 1) | (df_labeled_data_in_df['is_duplicate'] == 2)].count()
    labeled_positive = df_labeled_data_in_df['is_duplicate'].loc[df_labeled_data_in_df['is_duplicate'] == 2].count()
    labeled_negative = df_labeled_data_in_df['is_duplicate'].loc[df_labeled_data_in_df['is_duplicate'] == 0].count()
    #labeled_negative = df_labeled_data_in_df['is_duplicate'].loc[(df_labeled_data_in_df['is_duplicate'] == 0) | (df_labeled_data_in_df['is_duplicate'] == 1)].count()
    labeled_matches_count = df_labeled_data_in_df['is_duplicate'].count()
    return df_labeled_data_in_df, labeled_positive, labeled_negative, labeled_matches_count

def main(df):
    print('----Started to download the labeled data')
    #df_labeled_data = import_labeled_data_2()
    #df_labeled_data_2 = import_labeled_data()
    # df_labeled_data = df_labeled_data.append(df_labeled_data_2, ignore_index=True)
    df_labeled_data = import_labeled_data_3()

    start_time = time.time()
    print('----Identifying which labeled data is in imported df')
    df_labeled_data_in_df, labeled_positive, labeled_negative, labeled_matches_count = find_labeled_data_in_df(df_labeled_data, df)
    print("----//Merging labeled dataset with df took --- %s seconds ---" % (time.time() - start_time))
    del df_labeled_data

    return df_labeled_data_in_df, labeled_positive, labeled_negative, labeled_matches_count
