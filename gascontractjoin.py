from mrjob.job import MRJob
import time
class gasfilter(MRJob):
    def mapper(self,_,line):
        topContracts = ["0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444",
        "0xfa52274dd61e1643d2205169732f29114bc240b3",
        "0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef",	
        "0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8",	
        "0xbfc39b6f805a9e40e77291aff27aee3c96915bdd",	
        "0xabbb6bebfa05aa13e908eaa492bd7a8343760477",	
        "0x341e790174e3a4d35b65fdc067b6b5634a61caea",	
        "0x94e17901b6dfae329c63edd59447e2882e55aca6",
        "0x07c62a47ebe0fa853bb83375e488896ce71266df"]
        try:
            if len(line.split(','))==7:
                fields = line.split(',')
                address = fields[2]
                gas = int(fields[4])
                date = str(time.strftime("%Y - %m", time.gmtime(int(fields[6]))))
                if address in topContracts:
                    yield(date,(gas,1))


        except:
            pass
    def combiner(self,feature,values):
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

if __name__ =='__main__':
    gasfilter.run()
                
        

