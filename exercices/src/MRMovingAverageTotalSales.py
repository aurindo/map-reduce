from mrjob.job import MRJob
import pdb
from datetime import datetime
from mrjob.step import MRStep

class MRMovingAverageTotalSales(MRJob):

	def parseStringToDate(self, dateStr):
		return datetime.strptime(dateStr, '%m/%d/%y').strftime('%y/%m/%d')

	def mapper(self, _, line):
		transaction_date, product, price, _, _, _, _, country, _, _, _, _ = line.split(",")
		date = transaction_date.split(" ")
		yield (country, product, self.parseStringToDate(date[0])), (float(price))

	def reducer_sum_prices(self, key, values):
		yield (key), sum(values)

	def reducer_date_price(self, key, values):
		yield (key[0], key[1]), (key[2], list(values)[0])

	def reducer_moving_avarage(self, key, values):
		self.windowSize = 3

		data_price = list(values)
		print(key, data_price)
		
		sum = 0.0
		outputValue = 0

		for i in range(0, len(data_price)):
			sum += data_price[i][1]

			if i < self.windowSize:
				movingAverage = sum / (i + 1)
			else:
				sum -= data_price[i - self.windowSize][1]
				movingAverage = sum / self.windowSize

			timestamp = data_price[i][0]
			outputValue = str(timestamp) + ', ' + str(data_price[i][1]) + ', ' + str(movingAverage)

			yield key, outputValue

	def steps(self):
		return [
			MRStep(mapper=self.mapper,
				reducer=self.reducer_sum_prices),
			MRStep(reducer = self.reducer_date_price),
			MRStep(reducer = self.reducer_moving_avarage),
		]

if __name__ == '__main__':
	MRMovingAverageTotalSales.run()
