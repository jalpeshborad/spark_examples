# coding: utf-8
# -*- coding: utf-8 -*-

__author__ = "Jalpesh Borad"
__email__ = "jalpeshborad@gmail.com"

import os

from pyspark.sql import Row

from base.context import get_spark_session, get_spark_context
from base.decorators import time_taken
from configs.config import DATA_DIR

from scripts.rdd.popular_movie import get_movie_names

spark = get_spark_session("FriendsByAge")
sc = get_spark_context("FriendsByAge")

file_path = os.path.join(DATA_DIR, "ml-10M100K/ratings.dat")


def generate_data_set(x):
    fields = x.split("::")
    return Row(user_id=int(fields[0]), movie_id=int(fields[1]), raing=float(fields[2]))


@time_taken
def main():
    movie_names = sc.broadcast(get_movie_names())
    input_file = spark.sparkContext.textFile(file_path).sample(False, 0.01)
    ratings = input_file.map(generate_data_set)

    ratings_df = spark.createDataFrame(ratings)

    top_movies = ratings_df.groupBy("movie_id").count().orderBy("count", ascending=False).cache()
    top_movies.show()

    top_10_movies = top_movies.take(10)
    for _movie in top_10_movies:
        print(f"{movie_names.value[_movie['movie_id']]}")

    spark.stop()


if __name__ == '__main__':
    main()
