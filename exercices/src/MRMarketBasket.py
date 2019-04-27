from mrjob.job import MRJob
from itertools import combinations

# python3 src/MRMarketBasket.py dataset/items.txt
class MRMarketBasket(MRJob):

	def generta_combinations(self, items):
		result = []
		items.sort()

		for i in range(len(items)):
			for j in range(i + 1, len(items)):
				for l in range(j + 1, len(items)):
					a = items[i]
					b = items[j]
					c = items[l]
					result.append((a, b, c))

		return result

	def mapper(self, _, line):
		items = line.split(',')

		# combinationsList = self.generta_combinations(items)
		combinationsList = combinations(items, 4)

		for combination in combinationsList:
			yield combination, 1

	def reducer(self, key, values):
		yield key, sum(values)

if __name__ == '__main__':
	MRMarketBasket.run()
