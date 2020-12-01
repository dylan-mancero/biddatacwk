from mrjob.job import MRJob


class top_movements(MRJob):
    def mapper(self, _, line):
        try:
            #one mapper, we need to first differentiate among both types
            fields = line.split(',')
            if len(fields) == 9:
                #this should be a company sector line
                miner = fields[2]
                size = int(fields[4])
              
               
                yield (None,(miner,size))
        except :
            pass

    def combiner(self, _, values):
        sorted_values = sorted(values, reverse = True, key = lambda tup:tup[1])
        i = 0
        for value in sorted_values:
            yield ("top", value)
            i += 1
            if i >= 10:
                break

    def reducer(self, _, values):
        sorted_values = sorted(values, reverse = True, key = lambda tup:tup[1])
        i = 0
        for value in sorted_values:
            yield (value[0],value[1])
            i += 1
            if i >= 10:
                break
if __name__ == '__main__':

    top_movements.run()

