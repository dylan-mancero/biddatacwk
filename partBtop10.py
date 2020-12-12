from mrjob.job import MRJob

class partBtop10(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split('\t')#splitting by tabs because the dataset is (address value)
            if len(fields) == 2:
                #this should be a company sector line
                service = fields[0][1:-1]# getting rid of the quotations marks
                amount = int(fields[1])
                yield (None, (service,amount))
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
    partBtop10.run()

