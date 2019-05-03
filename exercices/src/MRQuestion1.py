from mrjob.job import MRJob
from mrjob.step import MRStep

# python3 src/MRQuestion1.py dataset/movies.txt
# Qual o filme de maior popularidade? R.: 50
# Qual o filme menos popular? R.: 1407
class MRQuestion1(MRJob):
	def steps(self):
		return [
			MRStep(mapper=self.mapper_get_movies_by_score,
				reducer=self.reducer_get_movie_avg),
			MRStep(reducer = self.reducer_get_movies_ordered)
		]

	def mapper_get_movies_by_score(self, _, line):
		_, movieID, rating, _ = line.split()
		yield movieID, float(rating)

	def reducer_get_movie_avg(self, key, values):
		scores = list(values)
		yield None, (sum(scores), key)

	def reducer_get_movies_ordered(self, key, values):
		items = list(values)
		items.sort()

		for item in items:
			yield item[1], item[0]

if __name__ == '__main__':
	MRQuestion1.run()
