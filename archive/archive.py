
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




def jaccard_sum_test(list1, list2, shingles_frequency):
    intersection = set(list1).intersection(list2)
    union = set(list1 + list2)
    sum_int = sum([shingles_frequency[i] for i in intersection])
    sum_union = sum([shingles_frequency[i] for i in union])
    return len(intersection)/len(union), sum_int/sum_union

# if shingle not in shingles_list: #check, maybe if is not needed bc its a set




def test_create_shingles_dict_words(texts, k):
	shingles_set = set()
	for text in texts:
		words = [word for word in text.split()]
		for word in words:
			if len(word) >= k:
				shingles = [word[i:i + k] for i in range(len(word) - k + 1)]
			else:
				shingles = [word + '_' * (k - len(word))]
			for shingle in shingles:
				shingles_set.add(shingle)
	shingles_set = sorted(shingles_set)
	shingles_dict = dict(zip(shingles_set, range(len(shingles_set))))
	return shingles_set, shingles_dict

def test_shingles_dict_words():
	R = []

	t = TestShingles()
	t.texts = ["LTD ", "LTD", " L TD", "BP LTD"]
	t.k = 3
	#	t.shingles_list = ["LTD", "TD "] #with spaces
	t.shingles_list = ["LTD", "BPL", "PLT"]  # without spaces
	#	t.shingles_dict = {'LTD': 0, 'TD ': 1} #with spaces
	t.shingles_dict = {'LTD': 0, 'BPL': 1, 'PLT': 2}  # without spaces
	R.append(t)

	t = TestShingles()
	t.texts = ["LTD", "LTD"]
	t.k = 3
	t.shingles_list = ["LTD"]
	t.shingles_dict = {'LTD': 0}
	R.append(t)

	for t in R:
		a, b = test_create_shingles_dict_words(t.texts, t.k)
		if t.shingles_list != a or t.shingles_dict != b:
			raise Exception("Test {} failed with result {}".format(t.texts, b))
		else:
			print("Test {} successful".format(t.texts))




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
