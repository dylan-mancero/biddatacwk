from mrjob.job import MRJob
import time

class Cpart1(MRJob):
    def mapper(self,_,line):
        try:
            fields = line.split(',')
            if len(fields)==9:
                miner = fields[2]
                size = int(fields[4])
                yield(miner,size)
        except:
            pass
        
    def combiner(self,miner,count):
        yield (miner, sum(count))

    def reducer(self,miner,count):
        yield (miner, sum(count))

if __name__ == '__main__':
    Cpart1.run()
