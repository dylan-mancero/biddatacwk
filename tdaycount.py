from mrjob.job import MRJob
import time

class tdaycount(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(',')
            if len(fields) == 7 :
                time_epoch = int(fields[6])
                month = time.strftime("%B",time.gmtime(time_epoch)) 
                yield (month, 1)
        except:
            pass
            #do nothing

    def combiner(self, month, counts):
        yield (month, sum(counts))

    def reducer(self, month, counts):
        yield (month, sum(counts))


if __name__ == '__main__':
    tdaycount.run()
