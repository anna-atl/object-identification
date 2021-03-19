import main
import pandas as pd
import numpy as np

class TestDfPreparation:
	df_input = pd.DataFrame(columns=['name'])
	df_output = pd.DataFrame(columns=['name'])
	df_result = pd.DataFrame(columns=['name'])

def test_df_prepare():
#	R = []
#	t = TestDfPreparation()
#	t.df_input.append(pd.Series(['lo.***l', 'lol', 'lol'], index=['name']))
	df_input = pd.DataFrame(np.array([['Lt.** *D'], [' ltd''//@'], ['%  '], ['#NAME?'], ['LTD ']]),columns=['name'])
	df_output = pd.DataFrame(np.array([['LT D'], ['LTD'], ['LTD']]),columns=['name_clean'])
	r = main.df_prepare(df_input)
	for i, j in zip(df_output['name_clean'], r['name_clean']):
		if i != j:
			raise Exception("Test {} failed with {} result".format(i, j))
		else:
			print("Test {} successful with {} result".format(i, j))

class TestShingles:
	texts = []
	k = 2
	shingles_list = []
	shingles_dict = {}
	docs = []
	signature_size = 50

def test_shingles_dict():
	R = []

	t = TestShingles()
	t.texts = ["LTD", "LTD"]
	t.k = 3
	t.shingles_list = ["LTD"]
	t.shingles_dict = {'LTD': 0}
	R.append(t)

	t = TestShingles()
	t.texts = ["LTD ", "LTD"]
	t.k = 3
	t.shingles_list = ["LTD", "TD "]
	t.shingles_dict = {'LTD': 0, 'TD ': 1}
	R.append(t)

	for t in R:
		a, b = main.create_shingles_dict(t.texts, t.k)
		if t.shingles_list != a or t.shingles_dict != b:
			raise Exception("Test {} failed".format(t.texts))
		else:
			print("Test {} successful".format(t.texts))

def test_shingles_doc():
	'''
	test texts in input should be upper case and without spaces before and after the string
	:return:
	'''
	R = []

	t = TestShingles()
	t.texts = ["LTD", "LTD"]
	t.k = 3
	t.shingles_dict = {'LTD': 0}
	t.docs = [[0],[0]]
	R.append(t)

	t = TestShingles()
	t.texts = ["LTD", "MPB",'LT D']
	t.k = 3
	t.shingles_dict = {'LTD': 0, 'MPB': 1, 'LT ': 2, 'T D': 3}
	t.docs = [[0],[1],[2,3]]
	R.append(t)

	for t in R:
		a = main.create_doc_shingles(t.texts, t.k, t.shingles_dict)
		if t.docs != a:
			raise Exception("Test {} failed".format(t.texts))
		else:
			print("Test {} successful".format(t.texts))

def test_jaccard():
	R = [("cat", "dog", 0), ("dog", "god", 1), ("aaa", "aab", 0.5)]   #in future we will need to fix it (2/3 instead of 1/2)
	for i in range(len(R)):
		w1, w2, res = R[i]
		if main.jaccard(w1, w2) != res:
			raise Exception("Test {} failed".format(R[i]))
		else:
			print("Test {} successful".format(R[i]))


#test_jaccard()
#test_shingles_dict()
#test_shingles_doc()
test_df_prepare()