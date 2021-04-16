import pandas as pd
import numpy as np
import string

class TestDfPreparation:
	df_input = pd.DataFrame(columns=['name'])
	df_output = pd.DataFrame(columns=['name'])
	df_result = pd.DataFrame(columns=['name'])

def test_df_prepare():
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
	tokens = {}

def test_shingles_dict():
	R = []

	t = TestShingles()
	t.texts = ["LTD ", "LTD", " LTD", "BP LTD"]
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
		a, b, c = main.create_shingles_dict(t.texts, t.k)
		if t.shingles_list != a or t.shingles_dict != b:
			raise Exception("Test {} failed with result {} and tokens {}".format(t.texts, b, c))
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
	t.docs = [[0], [0]]
	t.tokens = {'LTD': 0.25, 'BP': 2.0}
	R.append(t)

	t = TestShingles()
	t.texts = ["LTD", 'LT D', "MPB"]
	t.k = 3
	t.shingles_dict = {'LTD': 0, 'MPB': 1, 'LT_': 2, 'D__': 3}
	t.docs = [[0], [2, 3], [1]]
	t.tokens = {'LT': 0.2, 'D': 0.3, 'LTD': 0.1, 'MPB': 0.8}
	R.append(t)

	for t in R:
		a, b = main.create_doc_shingles(t.texts, t.k, t.shingles_dict, t.tokens)
		if t.docs != a:
			raise Exception("Test {} failed with result {} with {} weights".format(t.texts, a, b))
		else:
			print("Test {} successful with result {} with {} weights".format(t.texts, a, b))

def test_jaccard():
	R = [("cat", "dog", 0), ("dog", "god", 1), ("aaa", "aab", 0.5), ("aaa", "aabcd", 0.25)]   #in future we will need to fix it (2/3 instead of 1/2)
	for i in range(len(R)):
		w1, w2, res = R[i]
		if main.jaccard(w1, w2) != res:
			raise Exception("Test {} failed with {}".format(R[i], main.jaccard(w1, w2)))
		else:
			print("Test {} successful".format(R[i]))

def jaccard_sum_test(list1, list2, shingles_frequency):
    intersection = set(list1).intersection(list2)
    union = set(list1 + list2)
    sum_int = sum([shingles_frequency[i] for i in intersection])
    sum_union = sum([shingles_frequency[i] for i in union])
    return len(intersection)/len(union), sum_int/sum_union

def test_jaccard_sum_test():
	R = [([4, 2, 3], [3, 2, 1], [0, 10, 1, 2, 20], 0.5, 3/33)]
	for i in range(len(R)):
		w1, w2, fr, res, res_sum = R[i]
		t1, t2 = jaccard_sum_test(w1, w2, fr)
		if t1 != res or t2 != res_sum:
			raise Exception("Test {} failed with {}".format(R[i], jaccard_sum_test(w1, w2, fr)))
		else:
			print("Test {} successful".format(R[i]))

def test_jaccard_sum():
	R = [([4, 2, 3], [3, 2, 1], [0, 10, 1, 2, 20], 3/33)]
	for i in range(len(R)):
		w1, w2, fr, res_sum = R[i]
		if res_sum != main.jaccard_sum(w1, w2, fr):
			raise Exception("Test {} failed with {}".format(R[i], main.jaccard_sum(w1, w2, fr)))
		else:
			print("Test {} successful".format(R[i]))

def test_shingles_weights():
	R = [([4, 2, 3], [1/4, 1/2, 1/3])]
	for i in range(len(R)):
		l1, res = R[i]
		if res != main.create_shingles_weights(l1):
			raise Exception("Test {} failed with {}".format(R[i], main.create_shingles_weights(l1)))
		else:
			print("Test {} successful".format(R[i]))

def test_shingles_weights_new():
	R = [(['bc', 'bc ltd', 'bc ltd', 'ls', 'ls ltd'])]
	for i in range(len(R)):
		print(main.create_shingles_weights_new(R[i]))


import main

#test_jaccard()
#test_jaccard_sum()
#test_jaccard_sum_test()
#test_shingles_weights()
#test_shingles_weights_new()
#test_shingles_dict()
test_shingles_doc()
#test_df_prepare()
