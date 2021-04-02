
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




def test_shingles_dict():
	R = [('ollo',2,['ol','lo']),('lolo',2,['lo','lo'])]
	texts = []
	results = []

	for i in range(len(R)):
		text, k, res = R[i]
		texts.append(text)
		results.append(res)

	for i in range(len(R)):
		text, k, res = R[i]


		if main.create_shingles_dict(texts, k) != results:
			raise Exception("Test {} failed".format(R[i]))
		else:
			print("Test {} successful".format(R[i]))


#	make appends
# list of inputs and outputs




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
