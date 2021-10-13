import pandas as pd
import numpy as np

def test_nan():
	wordbook_name_8 = "~/Dropbox/Botva/TUM/Master_Thesis/datasets/raw_files/list-of-companies-in-united-kingdom.csv"
	# test mode:
	df_8 = pd.read_csv(wordbook_name_8, error_bad_lines=False, nrows=10)
	df_8['city'] = np.nan
	print(df_8.dtypes)

def test_list():
	a = [0,1,2]
	print(a[:5])


def test_set():
	shingles_list = set()
	shingles = ['de', 'de']
	for shingle in shingles:
		shingles_list.add(shingle)
	print(shingles_list)

def test_lists_append():
	shingles_frequency = [0]*6
	shingles = [1, 2, 5]

	for shingle in shingles:
		shingles_frequency[shingle] += 1
		print(shingles_frequency)

def test_lists_append():
	shingles_set = set()
	shingles = ['de', 'en', 'en']

	for shingle in shingles:
		shingles_set.add(shingle)

	shingles_dict = dict(zip(shingles_set, range(len(shingles_set))))
	print(shingles_set)
	print(shingles_dict)
	print(len(shingles_set))
	print(len(shingles))

	shingles_frequency = [0]*(len(shingles_dict))

	for shingle in shingles:
		shingles_frequency[shingles_dict[shingle]] += 1
		print(shingles_frequency)

def test_df():
    df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
    df['new'] = np.nan
    df['new_2'] = ''
    print(df)
    df = df.apply(lambda x: x.astype(str).str.upper())
    print(df)

    #df = np.zeros((2, 3))
    for column in df:
        print(column)

    for row, column in df:
        print(row)

def test_loops():
	l = [0, 1, 2]
	for a in l:
		a = 3
	print(l)

test_loops()
#test_lists_append()
#test_nan()
#test_list()