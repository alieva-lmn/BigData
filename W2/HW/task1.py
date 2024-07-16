from mrjob.job import MRJob
import re

class WordFrequencies(MRJob):

    def mapper(self, _, line):
        word = re.findall(r'\b\w+\b', line.lower())

        for i in word:
            yield i, 1

    def reducer(seld, key, value):
        yield key, sum(value)


if __name__ == "__main__":
    WordFrequencies.run()