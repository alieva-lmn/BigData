from mrjob.job import MRJob

class AverageAge(MRJob):
    def mapper(self, _, line):
        city = line.split(',')[1].split()[0]
        age = int(line.split(',')[1].split()[1])

        yield city, (age, 1)
    
    def reducer(self, key, values):
        PeopleAge = 0
        PeopleCount = 0

        for i, j in values:
            PeopleAge += i
            PeopleCount += j

        yield key, PeopleAge/PeopleCount


if __name__ == "__main__":
    AverageAge.run()