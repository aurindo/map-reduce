from mrjob.job import MRJob
from mrjob.step import MRStep

# python3 src/MRQuestion3.py dataset/movies.data
# Lista dos DEZ filmes mais populares, ordenados de forma descedente.
# "50"    2541.0
# "100"   2111.0
# "181"   2032.0
# "258"   1936.0
# "174"   1786.0
# "127"   1769.0
# "286"   1759.0
# "1"     1753.0
# "98"    1673.0
# "288"   1645.0
class MRQuestion3(MRJob):
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
		items.sort(reverse=True)
		for i in range(10):
		  yield items[i][1], items[i][0]

if __name__ == '__main__':
	MRQuestion3.run()
