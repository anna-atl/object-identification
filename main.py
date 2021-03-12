import pandas as pd
#from wordcloud import WordCloud, STOPWORDS
import sys
import numpy as np
import string
import collections
from textblob import TextBlob
import Levenshtein
#import binascii

import matplotlib.pyplot as plt
import random

def df_import():

    wordbook_name_1 = "~/Dropbox/Botva/TUM/Master_Thesis/datasets/raw_files/BasicCompanyDataAsOneFile-2021-02-01.csv"
    df_1 = pd.read_csv(wordbook_name_1, error_bad_lines=False)
    df_1 = df_1.head(100)
    df_1 = df_1.rename(columns={"CompanyName":"name"})
    df_1 = df_1[['name']]
    df_1['datasource'] = 'basiccomp'
    df = df_1
    del df_1

    wordbook_name_2 = "~/Dropbox/Botva/TUM/Master_Thesis/datasets/raw_files/companies_sorted.csv"
    df_2 = pd.read_csv(wordbook_name_2, error_bad_lines=False)
    df_2 = df_2.head(100)
    df_2 = df_2.rename(columns={"CompanyName": "name"})
    df_2 = df_2[['name']]
    df_2['datasource'] = 'peopledatalab'
    df = df.append(df_2)
    del df_2

    wordbook_name_3 = "~/Dropbox/Botva/TUM/Master_Thesis/datasets/raw_files/list-of-companies-in-austria.csv"
    df_3 = pd.read_csv(wordbook_name_3, error_bad_lines=False)
    df_3 = df_3.head(100)
    df_3 = df_3[['name']]
    df_3['datasource'] = 'powrbot_austria'
    df = df.append(df_3)
    del df_3

    # import FR data (adjustments for delimeters and encoding - latin)
    wordbook_name_4 = "~/Dropbox/Botva/TUM/Master_Thesis/datasets/processed_files/france_rna_processed.csv"
    df_4 = pd.read_csv(wordbook_name_4, encoding='latin-1', sep = ';', error_bad_lines=False)
    df_4 = df_4.head(100)
    df_4 = df_4[['name']]
    df_4['datasource'] = 'rna'
    df = df.append(df_4)
    del df_4

    wordbook_name_5 = "~/Dropbox/Botva/TUM/Master_Thesis/datasets/raw_files/list-of-companies-in-belgium.csv"
    df_5 = pd.read_csv(wordbook_name_5, error_bad_lines=False)
    df_5 = df_5.head(100)
    df_5 = df_5.rename(columns={"name;;": "name"})
    df_5 = df_5[['name']]
    df_5['datasource'] = 'powrbot_belgium'
    df = df.append(df_5)
    del df_5

    wordbook_name_6 = "~/Dropbox/Botva/TUM/Master_Thesis/datasets/raw_files/list-of-companies-in-france.csv"
    df_6 = pd.read_csv(wordbook_name_6, error_bad_lines=False)
    df_6 = df_6.head(100)
    df_6 = df_6.rename(columns={"name;": "name"})
    df_6 = df_6[['name']]
    df_6['datasource'] = 'powrbot_france'
    df = df.append(df_6)
    del df_6

    wordbook_name_7 = "~/Dropbox/Botva/TUM/Master_Thesis/datasets/raw_files/list-of-companies-in-germany.csv"
    df_7 = pd.read_csv(wordbook_name_7, error_bad_lines=False)
    df_7 = df_7.head(100)
    df_7 = df_7.rename(columns={"name;": "name"})
    df_7 = df_7[['name']]
    df_7['datasource'] = 'powrbot_germany'
    df = df.append(df_7)
    del df_7

    wordbook_name_8 = "~/Dropbox/Botva/TUM/Master_Thesis/datasets/raw_files/list-of-companies-in-united-kingdom.csv"
    df_8 = pd.read_csv(wordbook_name_8, error_bad_lines=False)
    df_8 = df_8.head(100)
    df_8 = df_8.rename(columns={"name;;": "name"})
    df_8 = df_8[['name']]
    df_8['datasource'] = 'powrbot_uk'
    df = df.append(df_8)
    del df_8

    df = df.reset_index(drop=True)

    print(df)
    print(df.dtypes)
    return df

def df_prepare(df):
    df = df.apply(lambda x: x.astype(str).str.upper())
    df['name'] = df['name'].apply(lambda x: x.replace('.',''))
    df['name'] = df['name'].str.replace('[^0-9a-zA-Z]+', ' ')
    df['name'] = df['name'].str.replace(' +', ' ')
    df['name_split'] = df['name'].str.split(' ')
    print(df)
    print(df.dtypes)

    return df

def frequent_words(df_processed):
    b = TextBlob("bonjour")
    b.detect_language()
    print(df_processed['name'])
    all_words = df_processed['name']
    all_words_cleaned = []

    for text in all_words:
        text = [x.strip(string.punctuation) for x in text]
        all_words_cleaned.append(text)

    text_words = [" ".join(text) for text in all_words_cleaned]
    final_text_words = " ".join(text_words)
    # final_text_words[:1000]

    print(all_words)
    stopwords = set(STOPWORDS)
    stopwords.update(["LE", "DE", "LA", "ET", "DES", "DU", "LES", "EN", "ET", "A", "POUR", "SUR", "SOU", "S", "D", "L"])

    wordcloud_names = WordCloud(stopwords=stopwords, background_color="white", max_font_size=50,
                                max_words=100).generate(final_text_words)

    # Lines 4 to 7
    plt.figure(figsize=(15, 15))
    plt.imshow(wordcloud_names, interpolation='bilinear')
    plt.axis("off")
    plt.show()
    filtered_words = [word for word in final_text_words.split() if word not in stopwords]
    counted_words = collections.Counter(filtered_words)

    word_count = {}

    for letter, count in counted_words.most_common(100):
        word_count[letter] = count

    for i, j in word_count.items():
        print('Word: {0}, count: {1}'.format(i, j))

    return word_count

def levenstein_distance(texts_1,texts_2):
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
    return lvn_array

#creating shingles_dict
def create_shingles_dict(texts,k):
    print("Started creating shingles...")
    shingles_list = set()

    for text in texts:
    #    print(text)
    #    text = text.encode()
        if len(text) >= k:
            shingles = [text[i:i + k] for i in range(len(text) - k + 1)]
        else:
            shingles = [text + ' ' * (k - len(text))]
    #    print(shingles)
        for shingle in shingles:
            if shingle not in shingles_list: #check, maybe if is not needed bc its a set
                shingles_list.add(shingle)

    shingles_list = sorted(shingles_list)
    shingles_dict = dict(zip(shingles_list,range(len(shingles_list))))
    return shingles_list, shingles_dict

#converting docs to shingles
def create_doc_shingles(texts,k):
    print("Started creating shingles for docs...")
    docs = [[] for i in range(len(texts))]

    for doc, text in zip(docs, texts):
        if len(text) >= k:
            shingles = [text[i:i + k] for i in range(len(text) - k + 1)]
        else:
            shingles = [text + ' ' * (k - len(text))]
        for shingle in shingles:
            doc.append(shingles_dict[shingle])
    return docs

#creating signatures array
def create_signatures_array(docs,shingles_list,signature_size):
    print("Started creating signatures...")
    signatures = np.zeros((signature_size, len(docs))) #create an array

    shingles_shuffled = [i for i in range(len(shingles_list))]

    for signature in signatures:
        random.shuffle(shingles_shuffled)
        for doc_index, doc in enumerate(docs):
            doc_a = [shingles_shuffled[i] for i in doc]
            signature[doc_index] = min(doc_a)
    return signatures

def create_buckets(signatures,bands_number):
    print("Started creating buckets...")
    r = int(len(signatures)/bands_number) #rows per band

    buckets_bands = [{} for i in range(bands_number)]

    for band, buckets in zip(range(bands_number),buckets_bands):
        for i in range(len(signatures[0])):
            buckets.setdefault(tuple(signatures[band*r:band*r+r,i]),[]).append(i)
        filtered = {key:item for key, item in buckets.items() if len(item)>1}
        buckets.clear()
        buckets.update(filtered)
    return buckets_bands

def jaccard(list1, list2):
    intersection = len(set(list1).intersection(list2))
    union = (len(set(list1)) + len(set(list2))) - intersection
    return intersection/union

def create_matches(buckets_bands):
    print("Started creating matches...")
    matches = {}
    for buckets in buckets_bands:
        for key, values_list in buckets.items():
            for value_1 in values_list:
                for value_2 in values_list:
                    if value_2 > value_1 and (value_1,value_2) not in matches:
                        matches.setdefault((value_1,value_2),[]).append(jaccard(docs[value_1], docs[value_2]))
                        #print(value_1)
                        #print(value_2)
    #                    print(texts[value_1])
    #                    print(texts[value_2])
    #                    print(jaccard(docs[value_1], docs[value_2]))
    return matches

#generate df with all potential matches
def create_df_with_attributes(matches,texts):
    print("Started adding matches attributes...")
    df_matches = pd.DataFrame.from_dict(matches, orient='index')
    df_matches['matches_tuple'] = df_matches.index
    df_matches[['doc_1','doc_2']] = pd.DataFrame(df_matches['matches_tuple'].tolist(), index=df_matches.index)
    df_matches = df_matches.drop(['matches_tuple'], axis=1)
    df_matches = df_matches.reset_index(drop=True)

    df_texts = pd.DataFrame(texts)
    df_texts['index'] = df_texts.index

    df_matches_full = pd.merge(df_matches, df_texts,  how='left', left_on=['doc_1'], right_on = ['index'])
    df_matches_full = df_matches_full.drop(['index'], axis=1)
    df_matches_full = pd.merge(df_matches_full, df_texts,  how='left', left_on=['doc_2'], right_on = ['index'])
    df_matches_full = df_matches_full.drop(['index'], axis=1)
    return df_matches_full

df = df_import()

df_processed = df_prepare(df)

k = 3 #shingles size
texts = df_processed['name']
shingles_list, shingles_dict = create_shingles_dict(texts,k)

docs = create_doc_shingles(texts,k)

signature_size = 50
signatures = create_signatures_array(docs,shingles_list,signature_size)

bands_number = 5
buckets_bands = create_buckets(signatures,bands_number)

#threshold = 0.7
matches = create_matches(buckets_bands)

df_matches_full = create_df_with_attributes(matches,texts)
print(df_matches_full)
