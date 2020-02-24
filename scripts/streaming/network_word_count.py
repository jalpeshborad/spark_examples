# coding: utf-8
# -*- coding: utf-8 -*-

__author__ = "Jalpesh Borad"
__email__ = "jalpeshborad@gmail.com"

import os

from base.context import get_spark_streaming_session, get_spark_session
from base.decorators import time_taken
from configs.config import DATA_DIR

ssc = get_spark_streaming_session("NetworkWordCount")
host, port = "localhost", 8000


@time_taken
def main():
    """Legacy park streaming"""
    ssc.checkpoint(os.path.join(DATA_DIR, "checkpoint"))
    input_stream = ssc.socketTextStream(host, port)
    processed = input_stream.flatMap(lambda x: x.split()).map(lambda x: (x, 1))
    count = processed.reduceByKey(lambda x, y: x + y).updateStateByKey(lambda x, y: sum(x, y or 0))
    print(count.pprint())

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopGraceFully=True)


if __name__ == '__main__':
    main()
