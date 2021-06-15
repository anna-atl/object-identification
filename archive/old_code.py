
def df_import_old():
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

def levenstein_distance(texts_1,texts_2):
    start_time = time.time()
    lvn_array = np.zeros((len(texts_1),len(texts_2)))
    print(lvn_array)
    text_1_num = 0
    text_2_num = 0

    for text_1 in texts_1:
        for text_2 in texts_2:
            lvn_array[text_1_num,text_2_num] = Levenshtein.ratio(text_1,text_2)
            text_2_num += 1
        text_2_num = 0
        text_1_num += 1
    print("Calculating levenstein distance took --- %s seconds ---" % (time.time() - start_time))
    return lvn_array

#creating shingles_dict
def create_shingles_dict(texts,k):
    '''
    This function creates a list and dict of shingles
    :param texts:one string column of a df (comp names), which is going to be divided in shingles
    :param k: shingle size
    :return: list of all shingles with order
    dict of shingles, shingle as a key, index of the shingle in the list as a value
    '''
    start_time = time.time()
    print("Started creating shingles...")
    shingles_set = set()

    for text in texts:
        if len(text) >= k:
            shingles = [text[i:i + k] for i in range(len(text) - k + 1)]
        else:
            shingles = [text + '_' * (k - len(text))]
        for shingle in shingles:
            shingles_set.add(shingle)

    shingles_set = sorted(shingles_set)
    shingles_dict = dict(zip(shingles_set, range(len(shingles_set))))
    print("Creating shingles took --- %s seconds ---" % (time.time() - start_time))
    return shingles_set, shingles_dict


#converting docs to shingles
def create_doc_shingles(texts,k,shingles_dict):
    '''
    :param texts:
    :param k:
    :param shingles_dict:
    :return:
    '''
    start_time = time.time()
    print("Started creating shingles for docs...")
    docs = [[] for i in range(len(texts))]
    texts = [x.strip(' ') for x in texts] #remove spaces

    for doc, text in zip(docs, texts):
        if len(text) >= k:
            shingles = [text[i:i + k] for i in range(len(text) - k + 1)]
        else:
            shingles = [text + '_' * (k - len(text))]
        for shingle in shingles:
            doc.append(shingles_dict[shingle])
    print("Converting docs to shingles took --- %s seconds ---" % (time.time() - start_time))
    return docs


def jaccard(list1, list2): #is not counting duplicate shingles
    intersection = len(set(list1).intersection(list2))
    union = (len(set(list1)) + len(set(list2))) - intersection
    return intersection/union

def create_matches(buckets_bands, docs):
    start_time = time.time()
    print("Started creating matches...")
    matches = {} #keys - tuple of duplicate docs and values - jacc of docs(lists of shingles(numbers))
    for buckets in buckets_bands:
        for key, values_list in buckets.items(): #values_list - doc indexes in one buckets
            for value_1 in values_list: #iterating through doc_indexes
                for value_2 in values_list:
                    if value_2 > value_1 and (value_1, value_2) not in matches:
                        matches.setdefault((value_1, value_2), []).append(jaccard(docs[value_1], docs[value_2])) #docs[value_1] - shingle numbers
    print("Creating matches (jaccard) took --- %s seconds ---" % (time.time() - start_time))
    return matches