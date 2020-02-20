# coding: utf-8
# -*- coding: utf-8 -*-

__author__ = "Jalpesh Borad"
__email__ = "jalpeshborad@gmail.com"

import os

from base.context import get_spark_context
from base.decorators import time_taken
from configs.config import DATA_DIR

sc = get_spark_context("PopularSuperHero")

start_character_id = 5306
target_character_id = 14
WHITE = 2
GRAY = 1
BLACK = 0

hit_counter = sc.accumulator(0)


def convert_to_bfs(line):
    fields = line.split()
    hero = int(fields[0])
    connections = [int(con_id) for con_id in fields[1:]]
    color = WHITE
    distance = 9999

    if hero == start_character_id:
        distance = 0
        color = GRAY

    return hero, (connections, distance, color)


def get_co_occurrences(line):
    val = line.split()
    return int(val[0]), len(val) - 1


def parsed_names(line):
    fields = line.split('"')
    return int(fields[0]), fields[1]


def get_init_rdd():
    file_data = sc.textFile(os.path.join(DATA_DIR, "marvel/Marvel-Graph.txt"))
    return file_data.map(convert_to_bfs)


def bfs_map(node):
    char_id = node[0]
    data = node[1]
    connections, distance, color = data
    results = []
    if color == GRAY:
        for conn in connections:
            new_char_id = conn
            new_distance = distance + 1
            new_color = GRAY
            if conn == target_character_id:
                hit_counter.add(1)

            new_record = new_char_id, ([], new_distance, new_color)
            results.append(new_record)
        color = BLACK
    results.append((char_id, (connections, distance, color)))
    return results


def bfs_reduce(data1, data2):
    edges1 = data1[0]
    edges2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]

    distance = 9999

    edges = edges1 + edges2

    distance = min(distance, distance1, distance2)
    color = min(color1, color2)

    return edges, distance, color


@time_taken
def main():
    init_rdd = get_init_rdd()

    for iteration in range(0, 100):
        print("Running BFS iteration# " + str(iteration + 1))

        # Create new vertices as needed to darken or reduce distances in the
        # reduce stage. If we encounter the node we're looking for as a GRAY
        # node, increment our accumulator to signal that we're done.
        mapped = init_rdd.flatMap(bfs_map)

        # Note that mapped.count() action here forces the RDD to be evaluated, and
        # that's the only reason our accumulator is actually updated.
        print("Processing " + str(mapped.count()) + " values.")

        if hit_counter.value > 0:
            print("Hit the target character! From " + str(hit_counter.value) \
                  + " different direction(s).")
            break

        # Reducer combines data for each character ID, preserving the darkest
        # color and shortest path.
        init_rdd = mapped.reduceByKey(bfs_reduce)
        print(init_rdd.top(10))


if __name__ == '__main__':
    main()
