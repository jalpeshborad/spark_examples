# coding: utf-8
# -*- coding: utf-8 -*-

__author__ = "Jalpesh Borad"
__email__ = "jalpeshborad@gmail.com"

"""
Broadcasting example:

Broadcast movie names to display movies instead of IDs
"""

import os

from base.context import get_spark_context
from base.decorators import time_taken
from configs.config import DATA_DIR


sc = get_spark_context("PopularMovie")


def get_movie_names():
    movies = {}
    with open(os.path.join(DATA_DIR, "ml-10M100K/movies.dat"), "r") as f:
        for _line in f:
            fields = _line.split("::")
            movies[int(fields[0])] = fields[1]
    return movies


@time_taken
def main():
    movie_names = sc.broadcast(get_movie_names())

    file_data = sc.textFile(os.path.join(DATA_DIR, "ml-10M100K/ratings.dat"))
    records = file_data.map(lambda x: (int(x.split("::")[1]), 1))
    summed_rdd = records.reduceByKey(lambda x, y: x + y)
    sorted_rdd = summed_rdd.map(lambda x: (x[1], x[0])).sortByKey()
    sorted_rdd_with_names = sorted_rdd.map(lambda x: (movie_names.value[x[1]], x[0]))
    result = sorted_rdd_with_names.collect()
    for val in result:
        print(f"Movie name: {val[0]} >> Views: {val[1]}")


if __name__ == '__main__':
    main()
