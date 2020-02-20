# coding: utf-8
# -*- coding: utf-8 -*-

__author__ = "Jalpesh Borad"
__email__ = "jalpeshborad@gmail.com"

import re
import os

from base.context import get_spark_context
from base.decorators import time_taken
from configs.config import DATA_DIR


sc = get_spark_context("PopularSuperHero")


def get_co_occurrences(line):
    val = line.split()
    return int(val[0]), len(val) - 1


def parsed_names(line):
    fields = line.split('"')
    return int(fields[0]), fields[1]


@time_taken
def main():
    name_data = sc.textFile(os.path.join(DATA_DIR, "marvel/Marvel-Names.txt"))
    names_rdd = name_data.map(parsed_names)

    file_data = sc.textFile(os.path.join(DATA_DIR, "marvel/Marvel-Graph.txt"))
    paired_rdd = file_data.map(get_co_occurrences)
    friends_of_hero = paired_rdd.reduceByKey(lambda x, y: x + y)
    flipped_rdd = friends_of_hero.map(lambda x: (x[1], x[0]))
    most_popular = flipped_rdd.max()
    most_popular_name = names_rdd.lookup(most_popular[1])[0]
    print(f"{most_popular_name} is the most popular superhero with {most_popular[0]} co-occurrences")


if __name__ == '__main__':
    main()
