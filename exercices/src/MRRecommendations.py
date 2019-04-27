from mrjob.job import MRJob
from mrjob.step import MRStep

# python3 src/MRRecommendations.py dataset/recommendation.csv
class MRRecommendations(MRJob):
	def steps(self):
		return [
			MRStep(
				mapper = self.mapper_products_by_user,
				reducer = self.reducer_products_by_user),
			MRStep(
				mapper = self.mapper_stripes,
			  reducer = self.reducer_stripes)
		]

	def mapper_products_by_user(self, _, line):
		(userID, itemID) = line.split(',')
		yield userID, itemID

	def reducer_products_by_user(self, userID, values):
		items = list(values)
		yield userID, items

	def mapper_stripes(self, userID, items):
		for item in items:
			map = {}

			for j in items:
				if j != item:
					if j not in map:
						map[j] = 0
					map[j] = map[j] + 1

			yield item, map

	def reducer_stripes(self, item, values):
		stripes = list(values)

		final = {}

		for map in stripes:
			for k, v in map.items():
				if k not in final:
					final[k] = 0
				final[k] = final[k] + int(v)
		yield item, self.topN(final, 3)

	def topN(self, final, N):
		items = []

		for k,v in final.items():
			items.append((k, v))

		items = sorted(items, key = self.by_value, reverse = True)

		size = min(N, len(items))

		return items[:size]

	def by_value(self, item):
		return item[1]

if __name__ == '__main__':
	MRRecommendations.run()
