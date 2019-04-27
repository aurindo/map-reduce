from mrjob.job import MRJob
import pdb

# [FAST] python3 src/MRCommonFriends.py dataset/common-friends.txt
# python3 src/MRCommonFriends.py dataset/Marvel-graph.txt
class MRCommonFriends(MRJob):
	def mapper(self, key, line):
		values = line.split(' ')
		
		if len(values) > 1:
			person = values[0]
			friends = values[1:]

			for friend in friends:
				pair = self.build_sorted_key(person, friend)
				
				yield pair, friends

	def reducer(self, key, values):
		friends = list(values)		
		result = []
		if len(friends) > 1:
			result = self.intersection(friends[0], friends[1])
		yield key, len(result)
		
	def intersection(self, user1friends, user2friends):
			if not user1friends:
					return None

			if not user2friends:
					return None    

			if len(user1friends) < len(user2friends):
					return self.intersect(user1friends, user2friends)
			else:
					return self.intersect(user2friends, user1friends)

	def intersect(self, small, large):
		result = []

		for n in small:
			if n in large:
				result.append(n)

		return result

	def build_sorted_key(self, person1, person2):
		if person1 < person2:
			return (person1, person2)
		else:
			return (person2, person1)

if __name__ == '__main__':
	MRCommonFriends.run()
