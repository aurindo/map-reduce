from mrjob.job import MRJob
from mrjob.step import MRStep

# python3 src/MRMovingAverageTotalSales.py dataset/sales.csv
class MRMovies(MRJob):
	def steps(self):
		return [
			MRStep(mapper=self.mapper_get_movies_by_score,
				reducer=self.reducer_get_movie_avg),
			MRStep(reducer = self.reducer_get_movies_ordered)
		]

	def mapper_get_movies_by_score(self, _, line):
		userID, movieID, rating, timestamp = line.split()
		yield movieID, float(rating)

	def reducer_get_movie_avg(self, key, values):
		scores = list(values)
		avg = sum(scores) / len(scores)
		yield None, (avg, key)

	def reducer_get_movies_ordered(self, key, values):
		items = list(values)
		items.sort()

		for item in items:
			yield item[1], item[0]

if __name__ == '__main__':
	MRMovies.run()
