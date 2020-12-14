from mrjob.job import MRJob
import time
class gasfilter(MRJob):
    def mapper(self,_,line):
        try:
            if len(line.split(','))==7:
                fields = line.split(',')
                address = fields[2]
                gas = int(fields[4])
                date = str(time.strft("%Y - %m", time.gmtime(int(fields[6]))))
                yield(address,(gas,date,1))

            elif len(line.split(','))==5:
                fields = line.split(',')
                address = fields[0]
                yield(address,(None,None,2))


        except:
            pass
    
    def reducer(self,address,values):
        matched = False
        gas = []
        date =[]
        for value in values:
            if (value[2]==1):
                gas.append(value[0])
                date.append(value[1])
            if (value[2]==2):
                matched = True
        if matched == True:
            for i in range(len(gas)):
                yield(address,str(gas[i])+str(date[i]))

if __name__ =='__main__':
    gasfilter.run()
                
        

