from mrjob.job import MRJob
from mrjob.step import MRStep

# python3 src/MRQuestion2.py dataset/common-friends.txt
# Qual o heroi mais popular? R.: Existem varios herois com 502 amigos
class MRQuestion2(MRJob):
	def steps(self):
		return [
			MRStep(
				mapper=self.mapper_count_friends,
				reducer=self.reducer_order_by_number_of_friends),
			MRStep(
				reducer=self.reducer_sort_counts)
			]

	def mapper_count_friends(self, _, line):
		values = line.split(' ')
		yield values[0], int(len(values) - 1)

	def reducer_order_by_number_of_friends(self, key, values):
		numberOfFriends = list(values)
		yield None, (numberOfFriends[0], key)

	def reducer_sort_counts(self, _, friends_counts):
		for count, key in sorted(friends_counts, reverse=False):
			yield (count, key)	

if __name__ == '__main__':
	MRQuestion2.run()
