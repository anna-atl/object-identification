import main

class TestShingles:
	texts = []
	k = 2
	shingles_list = []
	shingles_dict = {}
	docs = []
	signature_size = 50

def test_jaccard():
	R = [("cat", "dog", 0), ("dog", "god", 1), ("aaa", "aab", 0.5)]   #in future we will need to fix it (2/3 instead of 1/2)
	for i in range(len(R)):
		w1, w2, res = R[i]
		if main.jaccard(w1, w2) != res:
			raise Exception("Test {} failed".format(R[i]))
		else:
			print("Test {} successful".format(R[i]))

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
	R = []

	t = TestShingles()
	t.texts = ["LTD", "LTD"]
	t.k = 3
	t.shingles_dict = {'LTD': 0}
	t.docs = [[0],[0]]
	R.append(t)

	t = TestShingles()
	t.texts = ["LTD", "MPB"]
	t.k = 3
	t.shingles_dict = {'LTD': 0, 'MPB': 1}
	t.docs = [[0],[1]]
	R.append(t)

	for t in R:
		a = main.create_doc_shingles(t.texts, t.k, t.shingles_dict)
		if t.docs != a:
			raise Exception("Test {} failed".format(t.texts))
		else:
			print("Test {} successful".format(t.texts))

def test_shingles_doc():
	R = []

	t = TestShingles()
	t.texts = ["LTD", "LTD"]
	t.k = 3
	t.shingles_dict = {'LTD': 0}
	t.docs = [[0],[0]]
	R.append(t)

	t = TestShingles()
	t.texts = ["LTD", "MPB"]
	t.k = 3
	t.shingles_dict = {'LTD': 0, 'MPB': 1}
	t.docs = [[0],[1]]
	R.append(t)

	for t in R:
		a = main.create_doc_shingles(t.texts, t.k, t.shingles_dict)
		if t.docs != a:
			raise Exception("Test {} failed".format(t.texts))
		else:
			print("Test {} successful".format(t.texts))



#test_jaccard()
#test_shingles_doc()
test_shingles_doc()
