# coding: utf-8
# -*- coding: utf-8 -*-

__author__ = "Jalpesh Borad"
__email__ = "jalpeshborad@gmail.com"

import os

from base.context import get_spark_streaming_session
from base.decorators import time_taken
from configs.config import DATA_DIR

ssc = get_spark_streaming_session("WordCount")
input_path = os.path.join(DATA_DIR, "logs")


@time_taken
def main():
    ssc.checkpoint(os.path.join(DATA_DIR, "checkpoint"))
    input_stream = ssc.textFileStream(input_path)
    processed = input_stream.flatMap(lambda x: x.split()).map(lambda x: (x, 1))
    count = processed.reduceByKey(lambda x, y: x + y).updateStateByKey(lambda x, y: sum(x, y or 0))\
        .transform(lambda x: x.sortByKey())
    print(count.pprint())

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopGraceFully=True)


if __name__ == '__main__':
    main()
