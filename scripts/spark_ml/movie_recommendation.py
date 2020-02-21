# coding: utf-8
# -*- coding: utf-8 -*-

__author__ = "Jalpesh Borad"
__email__ = "jalpeshborad@gmail.com"

import os

from pyspark.sql import Row
from pyspark.mllib.recommendation import Rating, ALS

from base.context import get_spark_context
from base.decorators import time_taken
from configs.config import DATA_DIR
from scripts.rdd.movie_recommendation import get_movie_names


sc = get_spark_context("ALSMovieRecommendation")


def generate_rating_data_set(x):
    fields = x.split("::")
    return Rating(int(fields[0]), int(fields[1]), float(fields[2]))


@time_taken
def main():
    print("Broadcasting movie names...")
    movie_names = sc.broadcast(get_movie_names())

    file_path = os.path.join(DATA_DIR, "ml-10M100K/ratings.dat")
    input_data = sc.textFile(file_path).map(generate_rating_data_set).sample(False, 0.1).cache()

    _rank = 10
    _iter = 20
    model = ALS.train(input_data, _rank, _iter)

    recommendation_rdd = model.recommendProducts(2, 10)

    print("Data for UserID: 2")
    for _movie in input_data.filter(lambda x: x[0] == 2).collect():
        print(movie_names.value[_movie[1]])
    print("================================")
    print("Recommended Movies for UserID: 2")
    for _movie in recommendation_rdd:
        print(movie_names.value[_movie[1]])


if __name__ == '__main__':
    main()
