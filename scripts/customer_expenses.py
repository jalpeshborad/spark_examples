# coding: utf-8
# -*- coding: utf-8 -*-

__author__ = "Jalpesh Borad"
__email__ = "jalpeshborad@gmail.com"

import os

from base.context import get_spark_context
from base.decorators import time_taken
from configs.config import DATA_DIR


sc = get_spark_context("CustomerExpenses")


def parsed_line(line):
    elements = line.split(",")
    return int(elements[0]), float(elements[2])


@time_taken
def main():
    file_data = sc.textFile(os.path.join(DATA_DIR, "customer-orders.csv"))
    records = file_data.map(parsed_line)
    summed_rdd = records.reduceByKey(lambda x, y: x + y)
    sorted_rdd = summed_rdd.map(lambda x: (x[1], x[0])).sortByKey()
    result = sorted_rdd.collect()
    for val in result:
        print(f"Customer ID: {val[1]} spent {round(val[0], 2)} amount")


if __name__ == '__main__':
    main()
