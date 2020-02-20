# coding: utf-8
# -*- coding: utf-8 -*-

__author__ = "Jalpesh Borad"
__email__ = "jalpeshborad@gmail.com"

import os

from base.context import get_spark_context
from base.decorators import time_taken
from configs.config import DATA_DIR


sc = get_spark_context("MinimumTemperature")


def parse_line(line: str):
    line_ele = line.split(",")
    station, entry_type = line_ele[0], line_ele[2]
    temperature = float(line_ele[3]) * 0.1 * (9 / 5) + 32
    return station, entry_type, temperature


@time_taken
def main():
    file_data = sc.textFile(os.path.join(DATA_DIR, "1800.csv"))
    parsed_lines = file_data.map(parse_line)
    min_temp_data = parsed_lines.filter(lambda x: "TMIN" in x[1])
    station_temp = min_temp_data.map(lambda x: (x[0], x[2]))
    min_temp = station_temp.reduceByKey(lambda x, y: min(x, y))
    result = min_temp.collect()
    for val in result:
        print(f"Minimum temperature at Station: {val[0]} was {round(val[1], 2)} F")


if __name__ == '__main__':
    main()
