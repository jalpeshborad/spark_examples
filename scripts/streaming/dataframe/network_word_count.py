# coding: utf-8
# -*- coding: utf-8 -*-

__author__ = "Jalpesh Borad"
__email__ = "jalpeshborad@gmail.com"

from pyspark.sql.functions import explode, split

from base.context import get_spark_session
from base.decorators import time_taken, catch_keyboard_interrupt

spark = get_spark_session("NetworkWordCount")
host, port = "localhost", 8000


@catch_keyboard_interrupt
@time_taken
def main():
    """Legacy park streaming"""
    input_stream = spark.readStream.format("socket").option("host", host).option("port", port).load()
    words = input_stream.select(explode(split(input_stream.value, " ")).alias("word"))
    word_count = words.groupBy("word").count()
    result = word_count.writeStream.outputMode("complete").format("console").start()
    result.awaitTermination()
    spark.stop()


if __name__ == '__main__':
    main()
