def main(number_of_matches, scenario_name, experiment_number, dataset_size, final_time, labeled_number_of_matches, false_positive, false_negative, true_positive, true_negative, attribute_name, shingle_type, shingle_size, shingle_weight, buckets_type, signature_size, bands_number, attribute_threshold, attribute_weight):
    try:
        false_pos_rate = round(false_positive / (false_positive + true_negative), 3)
    except:
        false_pos_rate = 0

    try:
        false_neg_rate = round(false_negative / (false_negative + true_positive), 3)
    except:
        false_neg_rate = 0

    try:
        true_pos_rate = round(true_positive / (true_positive + false_negative), 3)
    except:
        true_pos_rate = 0

    try:
        true_neg_rate = round(true_negative / (true_negative + false_positive), 3)
    except:
        true_neg_rate = 0

    try:
        accuracy = round((true_positive + true_negative) / (true_positive + false_positive + false_negative + true_negative),3)
    except:
        accuracy = 0

    try:
        precision = round(true_positive / (true_positive + false_positive), 3)
    except:
        precision = 0

    try:
        recall = round(true_positive / (true_positive + false_negative), 3)
    except:
        recall = 0

    try:
        f1score = round(2*recall*precision / (recall + precision), 3)
    except:
        f1score = 0

    experiment_results = {'scenario_name': scenario_name,
                          'attribute_name': attribute_name,
                          'shingle_type': shingle_type,
                          'shingle_size': shingle_size,
                          'shingle_weight': shingle_weight,
                          'buckets_type': buckets_type,
                          'signature_size': signature_size,
                          'bands_number': bands_number,
                          'attribute_threshold': attribute_threshold,
                          'attribute_weight': attribute_weight,
                          'dataset_size': round(dataset_size,0),
                          'experiment_number': round(experiment_number,0),
                          'total_time': final_time,
                          'number_of_matches': number_of_matches,
                          'number_of_labeled_matches': labeled_number_of_matches,
                          'false_pos': false_positive,
                          'false_neg': false_negative,
                          'true_pos': true_positive,
                          'true_neg': true_negative,
                          'false_pos_rate (fall-out)': false_pos_rate,
                          'false_neg_rate (miss rate)': false_neg_rate,
                          'true_pos_rate (sensitivity)': true_pos_rate,
                          'true_neg_rate (specifity)': true_neg_rate,
                          'accuracy': accuracy,
                          'precision': precision,
                          'recall': recall,
                          'f1score': f1score
    }
    return experiment_results
