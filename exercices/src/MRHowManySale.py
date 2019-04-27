from mrjob.job import MRJob
import pdb
from mrjob.step import MRStep

class MRHowManySale(MRJob):
	def mapper(self, key, line):
		_, product, _, _, _, _, _, country, _, _, _, _ = line.split(",")
		yield (country, product), product

	def reducer_country_product(self, key, values):		
		products = list(values)
		yield key, len(products)

	def reducer_country_product_quantity(self, key, values):
		country = key[0]
		product = key[1]
		quantity = list(values)
		yield country, (product, quantity)

	def reducer_by_countries(self, key, values):
		yield key, list(values)

	def steps(self):
		return [
			MRStep(mapper=self.mapper,
				reducer=self.reducer_country_product),
			MRStep(reducer = self.reducer_country_product_quantity),
			MRStep(reducer = self.reducer_by_countries),
		]

if __name__ == '__main__':
	MRHowManySale.run()
