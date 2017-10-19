import pandas as pd
from mapreduce import mapreduce
import numpy as np
from pymongo import *

############# util ###########################################################
def jaccard_similarity(n_common, n1, n2):
    # http://en.wikipedia.org/wiki/Jaccard_index
    numerator = n_common
    denominator = n1 + n2 - n_common
    if denominator == 0:
        return 0.0
    return numerator / denominator


def combinations(iterable, r):
    # http://docs.python.org/2/library/itertools.html#itertools.combinations
    # combinations('ABCD', 2) --> AB AC AD BC BD CD
    # combinations(range(4), 3) --> 012 013 023 123
    pool = tuple(iterable)
    n = len(pool)
    if r > n:
        return
    indices = list(range(r))
    yield tuple(pool[i] for i in indices)
    while True:
        for i in reversed(list(range(r))):
            if indices[i] != i + n - r:
                break
        else:
            return
        indices[i] += 1
        for j in range(i+1, r):
            indices[j] = indices[j-1] + 1
        yield tuple(pool[i] for i in indices)
######## mapreduce function ####################################################


def main():
    # Get the ratings from ratings.csv
    ratings = read_netflix_ratings()

    # Initialize MapReduce
    map_reducer = mapreduce()
    pipeline = map_reducer.parallelize(ratings, 128)

    # DO NOT MODIFY THIS!
    similar_table = pipeline.map(mapper1) \
        .reduceByKey(reducer) \
        .flatMap(mapper2) \
        .reduceByKey(reducer) \
        .flatMap(mapper3) \
        .reduceByKey(reducer) \
        .flatMap(mapper4) \
        .reduceByKey(reducer) \
        .flatMap(mapper5)

    for item in similar_table.collect():
        print(item)


"""
TASK 0: Read in the netflix ratings from 'ratings.csv'
Input: None
Output: Two dimensional array where each record has a userId,movieId,rating,timestamp
"""


def read_netflix_ratings():
    # YOUR CODE HERE
    pass


def reducer(a, b):
    return a + b


def mapper1(record):
    """
    :param record: "user_id, movie_id, rating, timestamp"
    :return: (key, value)
              key: movie_id
              value: [(user_id, rating)]
    """
    # YOUR CODE HERE
    pass

def mapper2(record):
    """
    :param record: (movie_id, [(user_id, rating), ...])
    :return: [(user_id, [(movie_id, rating, num_rater)]), ...]
    """
    # YOUR CODE HERE
    pass


def mapper3(record):
    """
    to generate tuple that rated by the same user
    :param record: (user_id, [(movie_id, rating, num_rater), ...])
    :return: [( ( movie_id1, movie_id2 ), [(num_rater1, num_rater2)] ), ...]
    """
    # YOUR CODE HERE
    pass


def mapper4(record):
    """
    :param record: (key, value)
                     key: (movie_id1, movie_id2)
                     value: [(num_rater1, num_rater2), ...]
    :return: [(key, value)] or []
               key: movie_id
               value: [(movie_id2, jaccard)]
    """
    # YOUR CODE HERE
    pass

def mapper5(record):
    """
    :param record: (key, value)
                    key: movie_id1
                    value: [(movie_id2, jaccard), ...]
    :return: (key, value)
              key: movie_id1
              value: top n item
    """
    # YOUR CODE HERE
    pass


#Calls main() when you run the program
if __name__ == '__main__':
    main()
