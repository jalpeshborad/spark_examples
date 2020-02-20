# coding: utf-8
# -*- coding: utf-8 -*-

__author__ = "Jalpesh Borad"
__email__ = "jalpeshborad@gmail.com"

"""
Broadcasting example:

Broadcast movie names to display movies instead of IDs
"""

import os
import sys
from math import sqrt

from base.context import get_spark_context
from base.decorators import time_taken
from configs.config import DATA_DIR

sc = get_spark_context("MovieRecommendation")


def get_movie_names():
    movies = {}
    with open(os.path.join(DATA_DIR, "ml-10M100K/movies.dat"), "r", encoding='ascii', errors='ignore') as f:
        for _line in f:
            fields = _line.split("::")
            movies[int(fields[0])] = fields[1]
    return movies


def parse_line(line):
    fields = line.split("::")
    return int(fields[0]), (int(fields[1]), float(fields[2]))


def is_duplicate(_rdd):
    user_id, ratings = _rdd
    movie_1 = ratings[0]
    movie_2 = ratings[1]
    return movie_1 < movie_2


def make_movie_pairs(_rdd):
    user_id, ratings = _rdd
    movie_1, rating_1 = ratings[0]
    movie_2, rating_2 = ratings[1]
    return (movie_1, movie_2), (rating_1, rating_2)


def compute_cosine_similarity(rating_pairs):
    num_pairs = 0
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY in rating_pairs:
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        num_pairs += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0
    if denominator:
        score = numerator / denominator

    return score, num_pairs


@time_taken
def main():
    print("Broadcasting movie names to cluster...")
    movie_names = sc.broadcast(get_movie_names())

    file_data = sc.textFile("s3://jalpesh-data/movielens/ratings.dat")
    ratings = file_data.map(parse_line)

    'Discard lower ratings'
    ratings = ratings.filter(lambda x: x[1][1] > 2.5).partitionBy(100)

    joined_rdd = ratings.join(ratings)
    unique_pairs = joined_rdd.filter(is_duplicate)
    paired_rdd = unique_pairs.map(make_movie_pairs)

    grouped_rdd = paired_rdd.groupByKey()
    similarity_rdd = grouped_rdd.mapValues(compute_cosine_similarity).cache()

    if len(sys.argv) > 1:

        score_threshold = 0.97
        co_occurrence_threshold = 50

        movie_id = int(sys.argv[1])

        filtered_results = similarity_rdd.filter(
            lambda x: movie_id in x[0] and x[1][0] > score_threshold and x[1][1] > co_occurrence_threshold)

        results = filtered_results.map(lambda x: (x[1], x[0])).sortByKey(ascending=False).take(10)

        print(f"Top 10 similar movies for {movie_names.value[movie_id][0]}")
        for result in results:
            sim, pair = result

            suggested_movie = pair[0]
            if suggested_movie == movie_id:
                suggested_movie = pair[1]
            print(f"==> Movie: {movie_names.value[suggested_movie][0]}")


if __name__ == '__main__':
    main()
