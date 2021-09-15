import pandas as pd
import numpy as np

def import_labeled_data():
    labeled_data = "~/Dropbox/Botva/TUM/Master_Thesis/object-identification/labeled_data_final_2.csv"
    df_labeled_data = pd.read_csv(labeled_data, sep=';', error_bad_lines=False)
    df_labeled_data = df_labeled_data.dropna(subset=['is_duplicate'])
    df_labeled_data = df_labeled_data[['id_x', 'id_y', 'is_duplicate']] ##this is correct! (id_x, not doc_1 indexes)

    return df_labeled_data

def add_results_estimation(df_matches_estimation, labeled_positive, labeled_negative):
    matches_with_labels_count = df_matches_estimation['is_duplicate'].count()

    #true_positive = df_matches_estimation['is_duplicate'].loc[(df_matches_estimation['is_duplicate'] == 1) | (df_matches_estimation['is_duplicate'] == 2)].count()
    true_positive = df_matches_estimation['is_duplicate'].loc[df_matches_estimation['is_duplicate'] == 2].count()
    false_positive = df_matches_estimation['is_duplicate'].loc[df_matches_estimation['is_duplicate'] == 0].count()
    #false_positive = df_matches_estimation['is_duplicate'].loc[(df_matches_estimation['is_duplicate'] == 0) | (df_matches_estimation['is_duplicate'] == 1)].count()

    false_negative = labeled_positive - true_positive
    true_negative = labeled_negative - false_positive

    return false_positive, false_negative, true_positive, true_negative

if __name__ == "__main__":
    column_names = ['try_number','dataset_size', 'matching_attribute', 'attribute_weight', 'attribute_threshold', 'matching_method',
                    'hash_type', 'shingles_size',
                    'hash_weight', 'signature_size', 'bands_number', 'total_time', 'signatures_creation_time',
                    'buckets_creation_time', 'finding_matches_time', 'number_of_matches', 'false_pos', 'false_neg',
                    'true_pos', 'true_neg', 'false_pos_rate', 'false_neg_rate', 'true_pos_rate', 'true_neg_rate']

    df_labeled_data = import_labeled_data()
    b = df_labeled_data.loc[df_labeled_data['id_x'] < df_labeled_data['id_y']] #should be fixed later
    c = df_labeled_data.loc[df_labeled_data['id_x'] > df_labeled_data['id_y']]
    c = c.rename(columns={'id_x': 'id_y', 'id_y': 'id_x'}, inplace=False)
    c = c[['id_x', 'id_y', 'is_duplicate']]
    df_labeled_data = b.append(c)

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

df_labeled_data_not_in_df = pd.merge(df_labeled_data, df_all_matches, how='left',
                                     left_on=['id_x', 'id_y'],
                                     right_on=['doc_1', 'doc_2'])
df_labeled_data_not_in_df = df_labeled_data_not_in_df[
    df_labeled_data_not_in_df['doc_1'].isnull()]
df_labeled_data_not_in_df = df_labeled_data_not_in_df[['id_x', 'id_y', 'is_duplicate']]
df_not_labeled = pd.merge(df_labeled_data_not_in_df, df, how='left', left_on=['id_x'],
                          right_on=['id'])
df_not_labeled = pd.merge(df_not_labeled, df, how='left', left_on=['id_y'],
                          right_on=['id'])
df_not_labeled.to_csv("df_not_labeled_{}_{}.csv".format(attribute.matching_attribute,

                      for attribute_threshold in attribute_thresholds:
start_time = time.time()
print("Started calculating results accuracy...")
df_matches_estimation_for_thresholds = df_matches_estimation[
    df_matches_estimation.match_score > attribute_threshold]
false_positive, false_negative, true_positive, true_negative = add_results_estimation(
    df_matches_estimation_for_thresholds, labeled_positive, labeled_negative)
results_calculation = round(time.time() - start_time, 6)
print("Calculating results accuracy took {} sec".format(results_calculation))

experiment = {'try_number': try_number,
              'dataset_size': dataset_size,
              'matching_attribute': attribute.matching_attribute,
              'attribute_weight': 1,
              'attribute_threshold': attribute_threshold,
              'matching_method': attribute.matching_method,
              'hash_type': attribute.hash_type,
              'shingles_size': attribute.shingle_size,
              'hash_weight': attribute.hash_weight,
              'signature_size': attribute.signature_size,
              'bands_number': attribute.bands_number,
              'total_time': all_time,
              'signatures_creation_time': signatures_creation_time,
              'buckets_creation_time': buckets_creation_time,
              'finding_matches_time': finding_matches_time,
              'number_of_matches': len(df_matches_estimation_for_thresholds),
              'false_pos': false_positive,
              'false_neg': false_negative,
              'true_pos': true_positive,
              'true_neg': true_negative,
              'false_pos_rate': round(
                  false_positive / (false_positive + true_negative), 8),
              'false_neg_rate': round(
                  false_negative / (false_negative + true_positive), 8),
              'true_pos_rate': round(
                  true_positive / (true_positive + false_negative), 8),
              'true_neg_rate': round(
                  true_negative / (true_negative + false_positive), 8)
              }

df_results = df_results.append(experiment, ignore_index=True)
print('------------------------------------------------')
print('------------------------------------------------')
del df_matches_estimation_for_thresholds

print("Started joining the result with the labeled data...")
df_matches_estimation = pd.merge(df_all_matches, df_labeled_data, how='left',
                                                                 left_on=['doc_1', 'doc_2'], right_on=['id_x', 'id_y'])
df_matches_estimation.to_csv("df_matches_full_{}_{}.csv".format(attribute.matching_attribute,
                                                                       str(datetime.datetime.now())))
print("...Joint the result with the labeled data")



df_results.to_csv("df_results_{}_{}.csv".format(matching_attribute, str(datetime.datetime.now())))

df_results_all_tries = pd.DataFrame(columns=column_names)

for try_number in range(number_of_tries):
    df_results = pd.DataFrame(columns=column_names)

df_results = finding_best_methods_for_atts(df, df_results, df_labeled_data_in_df, labeled_positive, labeled_negative,
                                           try_number)
df_results.to_csv("df_results_final_{}_{}.csv".format(str(try_number), str(datetime.datetime.now())))

df_results_all_tries = df_results_all_tries.append(df_results, ignore_index=True)
df_results_all_tries.to_csv("df_results_all_tries_{}.csv".format(str(datetime.datetime.now())))




