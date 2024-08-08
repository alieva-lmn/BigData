from mrjob.job import MRJob

class FindMinMax(MRJob):
    def mapper(self, _, line):
        gender = line.split(',')[1].split()[0]
        count_of_projects = int(line.split(',')[1].split()[1])
        yield gender, count_of_projects

    def reducer(self, key, values):
        min_value = next(values)
        max_value = next(values)

        for i in values:
            if i < min_value:
                min_value = i
            if i > max_value:
                max_value = i

        yield key, (min_value, max_value)

if __name__ == "__main__":
    FindMinMax.run()

