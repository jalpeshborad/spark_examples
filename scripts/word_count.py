# coding: utf-8
# -*- coding: utf-8 -*-

__author__ = "Jalpesh Borad"
__email__ = "jalpeshborad@gmail.com"

import re
import os

from base.context import get_spark_context
from base.decorators import time_taken
from configs.config import DATA_DIR


sc = get_spark_context("WordCount")


def normalize_words(text):
    return re.compile(r"\W+", re.UNICODE).split(text.lower())


@time_taken
def main():
    file_data = sc.textFile(os.path.join(DATA_DIR, "Book.txt"))
    tokens = file_data.flatMap(normalize_words)
    # result = tokens.countByValue()
    # Not Scalable when we need sorted results
    result = tokens.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
    sorted_result = result.map(lambda x: (x[1], x[0])).sortByKey().collect()
    for val in sorted_result:
        if val[0]:
            print(val[1], "\t\t", val[0])


if __name__ == '__main__':
    main()
