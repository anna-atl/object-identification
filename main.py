import pandas as pd
#from wordcloud import WordCloud, STOPWORDS
import sys
import time
import numpy as np
import string
import collections
from textblob import TextBlob
import Levenshtein
#import binascii

import matplotlib.pyplot as plt
import random
pd.set_option('display.max_columns', None)

def df_prepare(df):
    '''
    This def cleans names from non text-number characters
    :param df: df with column name from raw datasets
    :return: df with cleaned names
    '''
    df = df.apply(lambda x: x.astype(str).str.upper())
    df['name_clean'] = df['name'].apply(lambda x: x.replace(';', ''))
    df = df.drop(df[df['name_clean'] == '#NAME?'].index, inplace=False)
    df['name_clean'] = df['name_clean'].apply(lambda x: x.replace('.', ''))
    df['name_clean'] = df['name_clean'].str.replace('[^0-9a-zA-Z]+', ' ')
    df['name_clean'] = df['name_clean'].str.replace(' +', ' ')
    df['name_clean'] = df['name_clean'].str.strip()
    df['name_clean'] = df['name_clean'].replace(r'^\s*$', np.NaN, regex=True) #i dont know why the one beofre doesnt work
    df = df.dropna()
    print(df)

    return df

def df_import():
    '''
    This function imports all dfs, keeps only comp names, created datasource column with datasource file info
    and merges all dfs together
    :return: merged df with columns name and datasource
    '''
    start_time = time.time()
    print('Started downloading datasets')

    # import FR data (adjustments for delimeters and encoding - latin)
    wordbook_name_1 = "~/Dropbox/Botva/TUM/Master_Thesis/datasets/processed_files/france_rna_processed.csv"
    df_1 = pd.read_csv(wordbook_name_1, encoding='latin-1', sep = ';', error_bad_lines=False) #error_bad_lines=False skips bad data
    df_1 = df_1[['name']]
    df_1['datasource'] = 'rna'
    df = df_1
    del df_1

    wordbook_name_2 = "~/Dropbox/Botva/TUM/Master_Thesis/datasets/raw_files/companies_sorted.csv"
    df_2 = pd.read_csv(wordbook_name_2, error_bad_lines=False)
    df_2 = df_2.rename(columns={"CompanyName": "name"})
    df_2 = df_2[['name']]
    df_2['datasource'] = 'peopledatalab'
    df = df.append(df_2)
    del df_2

    wordbook_name_3 = "~/Dropbox/Botva/TUM/Master_Thesis/datasets/raw_files/list-of-companies-in-austria.csv"
    df_3 = pd.read_csv(wordbook_name_3, error_bad_lines=False)
    df_3 = df_3[['name']]
    df_3['datasource'] = 'powrbot_austria'
    df = df.append(df_3)
    del df_3

    ''' this dataset is temporarly removed because of it's size
    wordbook_name_4 = "~/Dropbox/Botva/TUM/Master_Thesis/datasets/raw_files/BasicCompanyDataAsOneFile-2021-02-01.csv"
    df_4 = pd.read_csv(wordbook_name_4, error_bad_lines=False)
    df_4 = df_4.rename(columns={"CompanyName":"name"})
    df_4 = df_4[['name']]
    df_4['datasource'] = 'basiccomp'
    df = df.append(df_4)
    del df_4
    '''

    wordbook_name_5 = "~/Dropbox/Botva/TUM/Master_Thesis/datasets/raw_files/list-of-companies-in-belgium.csv"
    df_5 = pd.read_csv(wordbook_name_5, error_bad_lines=False)
    df_5 = df_5.rename(columns={"name;;": "name"})
    df_5 = df_5[['name']]
    df_5['datasource'] = 'powrbot_belgium'
    df = df.append(df_5)
    del df_5

    wordbook_name_6 = "~/Dropbox/Botva/TUM/Master_Thesis/datasets/raw_files/list-of-companies-in-france.csv"
    df_6 = pd.read_csv(wordbook_name_6, error_bad_lines=False)
    df_6 = df_6.rename(columns={"name;": "name"})
    df_6 = df_6[['name']]
    df_6['datasource'] = 'powrbot_france'
    df = df.append(df_6)
    del df_6

    wordbook_name_7 = "~/Dropbox/Botva/TUM/Master_Thesis/datasets/raw_files/list-of-companies-in-germany.csv"
    df_7 = pd.read_csv(wordbook_name_7, error_bad_lines=False)
    df_7 = df_7.rename(columns={"name;": "name"})
    df_7 = df_7[['name']]
    df_7['datasource'] = 'powrbot_germany'
    df = df.append(df_7)
    del df_7

    wordbook_name_8 = "~/Dropbox/Botva/TUM/Master_Thesis/datasets/raw_files/list-of-companies-in-united-kingdom.csv"
    df_8 = pd.read_csv(wordbook_name_8, error_bad_lines=False)
    df_8 = df_8.rename(columns={"name;;": "name"})
    df_8 = df_8[['name']]
    df_8['datasource'] = 'powrbot_uk'
    df = df.append(df_8)
    del df_8

    df = df_prepare(df)
    df = df.sort_values(by=['name'])
    df = df.reset_index(drop=True)
    print(df)
    print(df.dtypes)
    print("Importing datasets took --- %s seconds ---" % (time.time() - start_time))
    return df

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
    tokens = {} #token - unique word

    for text in texts:
        words = [word for word in text.split()]
        for word in words:
            if len(word) >= k:
                shingles = [word[i:i + k] for i in range(len(word) - k + 1)]
            else:
                shingles = [word + '_' * (k - len(word))]
            for shingle in shingles:
                shingles_set.add(shingle)
        for word_index, word in enumerate(reversed(words)):
            tokens.setdefault(word, []).append(word_index + 1)
    avg = {key: sum(value)/len(value)/len(value) for key, value in tokens.items()}
    tokens.update(avg)

    shingles_set = sorted(shingles_set)
    shingles_dict = dict(zip(shingles_set, range(len(shingles_set))))
    print("Creating shingles took --- %s seconds ---" % (time.time() - start_time))
    return shingles_set, shingles_dict, tokens

#converting docs to shingles
def create_doc_shingles(texts, k, shingles_dict, tokens):
    '''
    :param texts:
    :param k:
    :param shingles_dict:
    :return:
    '''
    start_time = time.time()
    print("Started creating shingles for docs...")
    docs = [[] for i in range(len(texts))]
    shingles_weights = [0] * len(shingles_dict) #check because when a text is smaller than k, a splace in the end is added. so this shingle might stay with 0 weight

    for doc, text in zip(docs, texts):
        words = [word for word in text.split()]
        for word in words:
            if len(word) >= k:
                shingles = [word[i:i + k] for i in range(len(word) - k + 1)]
            else:
                shingles = [word + '_' * (k - len(word))]
            for shingle in shingles:
                doc.append(shingles_dict[shingle])
                shingles_weights[shingles_dict[shingle]] = tokens[word]

    print("Converting docs to shingles took --- %s seconds ---" % (time.time() - start_time))
    return docs, shingles_weights

#creating signatures array
def create_signatures_array(docs, shingles_set, signature_size):
    start_time = time.time()
    print("Started creating signatures...")
    signatures = np.zeros((signature_size, len(docs))) #create a df with # rows = signature_size and #columns = docs
    shingles_shuffled = [i for i in range(len(shingles_set))]  #create list of shingles indexes for further randomizing

    for signature in signatures:   #iterating through rows of the signatures df
        random.shuffle(shingles_shuffled)
        for doc_index, doc in enumerate(docs): #for iterating over indexes in list as well
            doc_a = [shingles_shuffled[i] for i in doc] # --check this-- recreating shingles list of a doc with randomization
            signature[doc_index] = min(doc_a) #saving the smallest number for this randomization for this signature
    print("Creating signatures took --- %s seconds ---" % (time.time() - start_time))
    return signatures

def create_buckets(signatures, bands_number):
    start_time = time.time()
    print("Started creating buckets...")
    r = int(len(signatures)/bands_number) #rows per band

    buckets_bands = [{} for i in range(bands_number)] #create a list of dict which is number of buckets
    #buckets are going to be created (buckets - potential similar items)

    for band, buckets in zip(range(bands_number), buckets_bands): #buckets - dict:1,2,3,4,5. key - one bucket
        for i in range(len(signatures[0])): #iterating through docs
            buckets.setdefault(tuple(signatures[band*r:band*r+r, i]), []).append(i)
            #setdefault(key, value) creates a key if it doesnt exist with the value (here - list)
            #if the signatures of this bucket are same then key is the signature and values: doc index
            #if unique signature then only one doc_index will be as a value
        filtered = {key: item for key, item in buckets.items() if len(item) > 1} #filter out where one value (one doc with this signature)
        buckets.clear()
        buckets.update(filtered)
    print("Creating buckets took --- %s seconds ---" % (time.time() - start_time))
    return buckets_bands

def create_shingles_weights_new(texts, shingles_dict, k):
    shingles_weights = [0] * len(shingles_dict) #check because when a text is smaller than k, a splace in the end is added. so this shingle might stay with 0 weight
    tokens = {} #token - unique word

    for text in texts:
        words = [word for word in text.split()]
        for word_index, word in enumerate(reversed(words)):
            tokens.setdefault(word, []).append(word_index + 1)
    avg = {key: sum(value)/len(value)/len(value) for key, value in tokens.items()}
    tokens.update(avg)

    texts = [x.strip(' ') for x in texts] #remove spaces

    for text in texts:
        if len(text) >= k:
            shingles = [text[i:i + k] for i in range(len(text) - k + 1)]
        else:
            shingles = [text + ' ' * (k - len(text))]
        for shingle in shingles:
            doc.append(shingles_dict[shingle]) #doc - list of shingles(numbers)

            shingles_weights[shingles_dict[shingle]] = 1


    for shingle in shingles:
        doc.append(shingles_dict[shingle])
    return tokens

def jaccard_weighted(list1, list2, shingles_weights): #is not counting duplicate shingles
    intersection = set(list1).intersection(list2)
    union = set(list1 + list2)
    return sum([shingles_weights[i] for i in intersection])/sum([shingles_weights[i] for i in union])

def create_matches(buckets_bands, docs, shingles_weights):
    start_time = time.time()
    print("Started creating matches...")
    matches = {} #keys - tuple of duplicate docs and values - jacc of docs(lists of shingles(numbers))
    for buckets in buckets_bands:
        for key, values_list in buckets.items(): #values_list - doc indexes in one buckets
            for value_1 in values_list: #iterating through doc_indexes
                for value_2 in values_list:
                    if value_2 > value_1 and (value_1, value_2) not in matches:
                        matches.setdefault((value_1, value_2), []).append(jaccard_weighted(docs[value_1], docs[value_2], shingles_weights)) #docs[value_1] - shingle numbers
    print("Creating matches (jaccard) took --- %s seconds ---" % (time.time() - start_time))
    return matches

#generate df with all potential matches
def create_df_with_attributes(matches,df):
    print("Started adding matches attributes...")
    df_matches = pd.DataFrame.from_dict(matches, orient='index')
    df_matches['matches_tuple'] = df_matches.index
    df_matches[['doc_1', 'doc_2']] = pd.DataFrame(df_matches['matches_tuple'].tolist(), index=df_matches.index)
    df_matches = df_matches.drop(['matches_tuple'], axis=1)
    df_matches = df_matches.reset_index(drop=True)

    df['index'] = df.index

    df_matches_full = pd.merge(df_matches, df,  how='left', left_on=['doc_1'], right_on = ['index'])
    df_matches_full = df_matches_full.drop(['index'], axis=1)
    df_matches_full = pd.merge(df_matches_full, df,  how='left', left_on=['doc_2'], right_on = ['index'])
    df_matches_full = df_matches_full.drop(['index'], axis=1)
    return df_matches_full

def main(df, n):
    start_time = time.time()
    print ('Started working with the dataset (%s size):' % n)
    print(df)

    k = 3 #shingles size
    texts = df['name_clean'] #which column to use for minhash
    shingles_set, shingles_dict, tokens = create_shingles_dict(texts, k)

    docs, shingles_weights = create_doc_shingles(texts, k, shingles_dict, tokens)

    signature_size = 50
    signatures = create_signatures_array(docs, shingles_set, signature_size)

    bands_number = 5
    buckets_bands = create_buckets(signatures, bands_number)

    #threshold = 0.7
    matches = create_matches(buckets_bands, docs, shingles_weights)

    df_matches_full = create_df_with_attributes(matches, df)
    del df
    print(df_matches_full)
    print("Whole algorithm took --- %s seconds ---" % (time.time() - start_time))
    return time.time() - start_time, df_matches_full

if __name__ == "__main__":
    datasets_size = [1000000]
    time_spent = []
    df_matches_outputs = []
    df = df_import()

    for n in datasets_size:
        a, df_matches_full = main(df.head(n), n)
#        df_matches_full = df_matches_full.drop(columns=['name_x', 'name_y'], axis=1)
        df_matches_full.to_csv("df_matches_full_{}.csv".format(n))
        time_spent.append(a)
        df_matches_outputs.append(df_matches_full)

    for a, n in zip(time_spent, datasets_size):
        print('for {} dataset size it took {} seconds'.format(n, a))
        print('end')