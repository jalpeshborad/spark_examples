# coding: utf-8
# -*- coding: utf-8 -*-

__author__ = "Jalpesh Borad"
__email__ = "jalpeshborad@gmail.com"

import os
import collections

from base.context import get_spark_context
from base.decorators import time_taken
from configs.config import DATA_DIR

sc = get_spark_context("RatingDistribution")


@time_taken
def main():
    file_data = sc.textFile(os.path.join(DATA_DIR, "ml-10M100K/ratings.dat"))
    ratings = file_data.map(lambda x: x.split("::")[2])
    result = ratings.countByValue()
    result = collections.OrderedDict(sorted(result.items()))
    for k, v in result.items():
        print(f"Rating: {k} ==> Count: {v}")


if __name__ == '__main__':
    main()
