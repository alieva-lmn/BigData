from mrjob.job import MRJob

class NumberOfVoters(MRJob):

    def mapper(self, _, line):
        city, time, voters = line.split(",")
        yield city, (time, int(voters))

    def reducer(self, key, values):
        voter_list = list(values)
        max_voters = max(voter_list)
        min_voters = min(voter_list)
        yield key, (max_voters, min_voters)


if __name__ == "__main__":
    NumberOfVoters.run()