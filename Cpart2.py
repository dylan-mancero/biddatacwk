from mrjob.job import MRJob

class Cpart2(MRJob):
    def mapper(self,_,line):
        try:
            fields = line.split()
            if len(fields)==2:
                key = fields[0][1:-1]
                value = int(fields[1])
                yield(None,(key,value))
        except:
            pass
    def combiner(self,_,values):
        sortedvals = sorted(values,reverse=True,key = lambda tup:tup[1])
        i = 0
        for value in sortedvals:
            yield("top",value)
            i+=1
            if i==10:
                break
    def reducer(self,_,values):
        sortedvals = sorted(values,reverse=True,key = lambda tup:tup[1])
        i = 0
        for value in sortedvals:
            yield(value[0],value[1])
            i+=1
            if i==10:
                break

if __name__=='__main__':
    Cpart2.run()