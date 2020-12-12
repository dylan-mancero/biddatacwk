from mrjob.job import MRJob
class partBpart2(MRJob):
    def mapper(self,_,line):
        try:
            if len(line.split('\t'))==2:
                fields = line.split('\t')
                address = fields[0][1:-1]
                ether = int(fields[1])
                yield(address,(ether,1))

            elif len(line.split(','))==5:
                fields = line.split(',')
                address = fields[0]
                yield(address,(None,2))


        except:
            pass
    
    def reducer(self,address,values):
        matched = False
        cash = 0
        for value in values:
            if (value[1]==1):
                cash = (value[0])
            if (value[1]==2):
                matched = True
        if (matched and cash>0):
            yield(address, cash)
if __name__ =='__main__':
    partBpart2.run()
                
        