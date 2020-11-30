#LAB 1. Basic Wordcount
from mrjob.job import MRJob
import re

#this is a regular expression that finds all the words inside a String
WORD_REGEX =re.compile(r"\b\w+\b")

#this line declares the calss Lab1m that extends the MRJob format.

class Lab1(MRJob):

    def mapper(self, _, line):
        words = WORD_REGEX.findall(line)
        for word in words:
            yield (word.lower(), 1)

    def reducer(self, word,counts):
        yield (word, sum(counts))

#this class will define two additional methods: the mapper method goes here


#and the reducer method goes after this line


if __name__=='__main__':
    Lab1.run()
