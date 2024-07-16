from mrjob.job import MRJob

class CardSuitsCounter(MRJob):

    def mapper(self, _, line):
        key = line.split(",")[1]
        value = line.split(",")[0]

        yield key, 1

    def reducer(self, key, value):
        yield key, sum(value)


if __name__ == "__main__":
    CardSuitsCounter.run()