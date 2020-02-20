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
from base.context import get_spark_context
from base.decorators import time_taken
from configs.config import DATA_DIR

sc = get_spark_context("MovieRecommendationGenre")


def generate_iter(x, y):
    return (x,) + (y,)


def get_movie_names():
    movies = {}
    with open(os.path.join(DATA_DIR, "ml-10M100K/movies.dat"), "r", encoding='ascii', errors='ignore') as f:
        for _line in f:
            fields = _line.split("::")
            movies[int(fields[0])] = fields[1], tuple(fields[2].replace("\n", "").split("|"))
    return movies


def parse_line(line):
    fields = line.split("::")
    return int(fields[0]), (int(fields[1]), float(fields[2]))


def is_duplicate(_rdd):
    user_id, genres = _rdd
    movie_1 = genres[0][0]
    movie_2 = genres[1][0]
    return movie_1 < movie_2


def make_movie_pairs(_rdd):
    user_id, ratings = _rdd
    movie_1, genre_1 = ratings[0]
    movie_2, genre_2 = ratings[1]
    return (user_id, movie_1, movie_2), ((genre_1, genre_2), 1)


def compute_jaccord_similarity(rating_pairs):
    """ Genre based similarity"""
    g_x = tuple(rating_pairs[0][0])
    g_y = tuple(rating_pairs[0][1])
    _intersection = set(g_x).intersection(set(g_y))
    _union = set(g_x).union(set(g_y))
    score = len(_intersection) / len(_union)
    return score, 1


def reduce_score_and_occurrence(x, y):
    _score_0, _count_0 = x
    _score_1, _count_1 = y
    return _score_0 + _score_1, _count_0 + _count_1


@time_taken
def main():
    print("Broadcasting movie names to cluster...")
    movie_names = sc.broadcast(get_movie_names())

    file_data = sc.textFile(os.path.join(DATA_DIR, "ml-10M100K/ratings.dat")).sample(False, 0.1)
    # file_data = sc.textFile("s3://jalpesh-data/movielens/ratings.dat").sample(False, 0.001)
    ratings = file_data.map(parse_line)

    'Discard lower ratings'
    ratings = ratings.filter(lambda x: x[1][1] > 1.5).mapValues(lambda x: x[0]).partitionBy(100)

    "(user_id, movie_id) ==> (user_id, (movie_id, [genres]))"
    ratings = ratings.mapValues(lambda x: (x, movie_names.value[x][1]))

    "(user_id, (movie_id, [genres])) ==> (user_id, ((movie_id, [genres]), (movie_id, [genres]))"
    joined_rdd = ratings.join(ratings)

    unique_pairs = joined_rdd.filter(is_duplicate)

    "(user_id, ((movie_id, [genres]), (movie_id, [genres])) ==> (user_id, movie_id, movie_id), ([genres], [genres])"
    paired_rdd = unique_pairs.map(make_movie_pairs)
    grouped_rdd = paired_rdd.reduceByKey(generate_iter)

    similarity_rdd = grouped_rdd.mapValues(compute_jaccord_similarity)

    similarity_rdd_modified = similarity_rdd.map(lambda x: ((x[0][1], x[0][2]), x[1]))\
        .reduceByKey(reduce_score_and_occurrence)

    """For future uses"""
    # similarity_rdd_sorted = similarity_rdd.sortByKey()
    # similarity_rdd_sorted.saveAsTextFile(os.path.join(DATA_DIR, "exported/movie_similarity.export"))

    if len(sys.argv) > 1:

        score_threshold = 0
        co_occurrence_threshold = 0

        movie_id = int(sys.argv[1])

        filtered_results = similarity_rdd_modified.filter(
            lambda x: movie_id in x[0] and x[1][0] > score_threshold and x[1][1] > co_occurrence_threshold)

        results = filtered_results.map(lambda x: (x[1], x[0]))
        results = results.sortByKey(ascending=False).take(10)

        print(f"Top 10 similar movies for {movie_names.value[movie_id][0]}")
        for result in results:
            sim, pair = result

            suggested_movie = pair[0]
            if suggested_movie == movie_id:
                suggested_movie = pair[1]
            print(f"==> Movie: {movie_names.value[suggested_movie][0]} ::: Score: {round(sim[0], 2)} "
                  f"and Co-Occurrence: {sim[1]}")


if __name__ == '__main__':
    main()
