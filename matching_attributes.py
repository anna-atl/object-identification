
matching_attributes = {
    "number_of_tries": 1,
    "dataset_size_to_import": 500,
    "dataset_size": 1000000,
    "sum_score": "sum",
    "attribute_params":
        {
            "name": {
                "matching_attribute": "name_clean",
                "shingle_type": "shingle",
                "shingle_size": 3,
                "shingle_weight": 'weighted',
                "buckets_type": 'weighted minhash 1', #"minhash" / "weighted minhash 1/2" / "single bucket" / "no bucket",
                "signature_size": 50,
                "bands_number": 5,
                "comparison_method": 'weighted jaccard',
                "attribute_threshold": 0
            },
            "country": {
                "matching_attribute": "country_clean",
                "shingle_type": "shingle",
                "shingle_size": 3,
                "shingle_weight": 'weighted',
                "buckets_type": "no bucket",
                "signature_size": 0,
                "bands_number": 0,
                "comparison_method": 'weighted jaccard',
                "attribute_threshold": 0
            },
            "state": {
                "matching_attribute": "state_clean",
                "shingle_type": "shingle",
                "shingle_size": 3,
                "shingle_weight": 'weighted',
                "buckets_type": "no bucket",
                "signature_size": 0,
                "bands_number": 0,
                "comparison_method": 'weighted jaccard',
                "attribute_threshold": 0
            },
            "city": {
                "matching_attribute": "city_clean",
                "shingle_type": "shingle",
                "shingle_size": 3,
                "shingle_weight": 'weighted',
                "buckets_type": "no bucket",
                "signature_size": 0,
                "bands_number": 0,
                "comparison_method": 'weighted jaccard',
                "attribute_threshold": 0
            },
            "zip": {
                "matching_attribute": "zip_clean",
                "shingle_type": "shingle",
                "shingle_size": 3,
                "shingle_weight": 'weighted',
                "buckets_type": "no bucket",
                "signature_size": 0,
                "bands_number": 0,
                "comparison_method": 'weighted jaccard',
                "attribute_threshold": 0
            },
            "street": {
                "matching_attribute": "street_clean",
                "shingle_type": "shingle",
                "shingle_size": 3,
                "shingle_weight": 'weighted',
                "buckets_type": "no bucket",
                "signature_size": 0,
                "bands_number": 0,
                "comparison_method": 'weighted jaccard',
                "attribute_threshold": 0
            },
            "url": {
                "matching_attribute": "url_clean",
                "shingle_type": "shingle",
                "shingle_size": 3,
                "shingle_weight": 'weighted',
                "buckets_type": "no bucket",
                "signature_size": 0,
                "bands_number": 0,
                "comparison_method": 'weighted jaccard',
                "attribute_threshold": 0
            },
            "industry": {
                "matching_attribute": "industry_clean",
                "shingle_type": "shingle",
                "shingle_size": 3,
                "shingle_weight": 'weighted',
                "buckets_type": "no bucket",
                "signature_size": 0,
                "bands_number": 0,
                "comparison_method": 'weighted jaccard',
                "attribute_threshold": 0
            }
        }
}



