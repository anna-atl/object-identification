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
    # import FR data (adjustments for delimeters and encoding - latin)
    wordbook_name = "~/Dropbox/Botva/TUM/Master_Thesis/object-identification/datasets/processed_files/france_rna_processed.csv"
    df = pd.read_csv(wordbook_name, encoding='latin-1', sep = ';', error_bad_lines=False)
#    df = pd.read_csv("~/Dropbox/Botva/TUM/Master_Thesis/object-identification/datasets/raw_files/rna_waldec_20201201_dpt_01.csv", error_bad_lines=False)
#    df = df.astype(str)
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

df = df_import()

df_processed = df_prepare(df)
texts = df_processed['name']

n = 2
shingles_list = set()

for text in texts:
    #    print(text)
    #    text = text.encode()
    shingles = [text[i:i + n] for i in range(len(text) - n + 1)]
    #    print(shingles)
    for shingle in shingles:
        if shingle not in shingles_list:  # check, maybe if is not needed bc its a set
            shingles_list.add(shingle)

shingles_list = sorted(shingles_list)
shingles_dict = dict(zip(shingles_list, range(len(shingles_list))))



docs = [[] for i in range(len(df_processed['name']))]

#hashes_array = np.zeros((len(shingles_list), len(df_processed['name'])))

for doc, text in zip(docs, texts):
    shingles = [text[i:i + n] for i in range(len(text) - n + 1)]
    for shingle in shingles:
        doc.append(shingles_dict[shingle])



signature_size = 50 #signature size
#signatures = [[] for i in range(len(signature_size))]

signatures = np.zeros((signature_size, len(docs)))

#signatures = [[] for i in range(len(signature_size))]

shingles_shuffled = [i for i in range(len(shingles_list))]

for signature in signatures:
    random.shuffle(shingles_shuffled)
    for doc_index, doc in enumerate(docs):
        doc_a = [shingles_shuffled[i] for i in doc]
        signature[doc_index] = min(doc_a)

print(signatures)
