from mrjob.job import MRJob
import time

class Lab3part1(MRJob):
	def mapper(self, _, line):
		try:
			fields = line.split(' ')
			if len(fields) == 2:
				date = str(fields[0])
				ammount = int(fields[1])
			
				yield(date, (ammount,1))
				
		except:
			pass
	def combiner(self, feature, values):
		count = 0
		total = 0
		for value in values:
			count += value[1]
			total += value[0]
		yield (feature, (total, count) )

	def reducer(self, feature, values):
		count = 0
		total = 0
		for value in values:
			count += value[1]
			total += value[0]
		yield (feature, total/count)

if __name__=='__main__':
	Lab3part1.run()
