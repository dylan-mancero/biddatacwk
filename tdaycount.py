from mrjob.job import MRJob
import time

class Lab3part1(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(',')
            if len(fields) == 7 :
                time_epoch = int(fields[0])/1000
                day = time.strftime("%d",time.gmtime(time_epoch)) #returns day of the month
                yield (day, 1)
        except:
            pass
            #do nothing

    def combiner(self, day, counts):
        yield (day, sum(counts))

    def reducer(self, day, counts):
        yield (day, sum(counts))


if __name__ == '__main__':
    Lab3part1.run()
