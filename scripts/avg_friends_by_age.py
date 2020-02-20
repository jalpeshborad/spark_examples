# coding: utf-8
# -*- coding: utf-8 -*-

__author__ = "Jalpesh Borad"
__email__ = "jalpeshborad@gmail.com"

import os

from base.context import get_spark_context
from base.decorators import time_taken
from configs.config import DATA_DIR


sc = get_spark_context("AverageNoOfFriendsByAge")


def parse_line(line: str):
    line_ele = line.split(",")
    age = int(line_ele[2])
    no_of_friends = int(line_ele[3])
    return age, no_of_friends


@time_taken
def main():
    file_data = sc.textFile(os.path.join(DATA_DIR, 'fakefriends.csv'))
    rdd = file_data.map(parse_line)
    reduced_rdd = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    avg_friends = reduced_rdd.mapValues(lambda x: x[0] / x[1]).collect()
    for val in sorted(avg_friends, key=lambda x: x[0]):
        print(f"Age: {val[0]} ==> Avg count of Friends: {int(val[1])}")


if __name__ == '__main__':
    main()
