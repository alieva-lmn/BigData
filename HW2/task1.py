from mrjob.job import MRJob

class HighTemperatureDetector(MRJob):

    def mapper(self, _, line):
        part1 = line.split(",")
        airport = part1[0]

        date_and_temp = part1[1].split("\t")
        date = date_and_temp[0]
        temp = date_and_temp[1]

        if float(temp) > 36.6:
            yield (airport, date), 1

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == "__main__":
    HighTemperatureDetector.run()