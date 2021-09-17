
def main(df_results, ):
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
    df_results.to_csv("df_results_{}_{}.csv".format(matching_attribute, str(datetime.datetime.now())))

