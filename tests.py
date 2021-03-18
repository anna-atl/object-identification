import main

def test_jaccard():
	R = [("cat", "dog", 0), ("dog", "god", 1), ("aaa", "aab", 0.5)]   #in future we will need to fix it (2/3 instead of 1/2)
	for i in range(len(R)):
		w1, w2, res = R[i]
		if main.jaccard(w1, w2) != res:
			raise Exception("Test {} failed".format(R[i]))
		else:
			print("Test {} successful".format(R[i]))

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


def f(x):   # x negative, subtracts 1, otherwise adds 1
	if x > 0:
		return x+1
	else:
		return x-1

def test_f():
	R = [(0, 1)]
	for i in range(len(R)):
		x, res = R[i]
		if  f(x) != res:
			raise Exception("Test {} failed".format(i))

test_jaccard()
test_shingles_dict()