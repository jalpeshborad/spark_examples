# coding: utf-8
# -*- coding: utf-8 -*-

__author__ = "Jalpesh Borad"
__email__ = "jalpeshborad@gmail.com"

import os

from pyspark.sql import Row

from base.context import get_spark_session
from base.decorators import time_taken
from configs.config import DATA_DIR

spark = get_spark_session("FriendsByAge")

file_path = os.path.join(DATA_DIR, "fakefriends.csv")


def generate_data_set(x):
    fields = x.split(",")
    return Row(id=fields[0], name=fields[1], age=fields[2], friends=fields[3])


@time_taken
def main():
    input_file = spark.sparkContext.textFile(file_path)
    friends = input_file.map(generate_data_set)

    friends_df = spark.createDataFrame(friends).cache()
    friends_df.createOrReplaceTempView("friends")

    filtered_df = spark.sql("SELECT * FROM friends WHERE age > 18 and age < 30")
    for _row in filtered_df.collect():
        print(_row)

    friends_df.groupBy("age").count().orderBy("age").show()
    spark.stop()


if __name__ == '__main__':
    main()
