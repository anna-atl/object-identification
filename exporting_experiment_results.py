def main(df_matches_estimation, scenario_name, experiment_number, dataset_size, final_time, false_positive, false_negative, true_positive, true_negative):
    experiment_results = {'scenario_name': scenario_name,
                  'experiment_number': experiment_number,
                  'dataset_size': dataset_size,
                  'total_time': final_time,
                  'number_of_matches': len(df_matches_estimation),
                  'false_pos': false_positive,
                  'false_neg': false_negative,
                  'true_pos': true_positive,
                  'true_neg': true_negative,
                  'false_pos_rate (fall-out)': round(
                      false_positive / (false_positive + true_negative), 8),
                  'false_neg_rate (miss rate)': round(
                      false_negative / (false_negative + true_positive), 8),
                  'true_pos_rate (sensitivity)': round(
                      true_positive / (true_positive + false_negative), 8),
                  'true_neg_rate (specifity)': round(
                      true_negative / (true_negative + false_positive), 8)
                  }
    return experiment_results
    '''
    df_results = df_results.append(experiment, ignore_index=True)
    df_results.to_csv("df_results_{}_{}.csv".format(matching_attribute, str(datetime.datetime.now())))
    
    print('------------------------------------------------')
    print('------------------------------------------------')
    del df_matches_estimation_for_thresholds
    '''