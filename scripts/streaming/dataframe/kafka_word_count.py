# coding: utf-8
# -*- coding: utf-8 -*-

__author__ = "Jalpesh Borad"
__email__ = "jalpeshborad@gmail.com"

import os
from pyspark.sql.functions import explode, split

from base.context import get_spark_session
from base.decorators import time_taken, catch_keyboard_interrupt
from configs.config import DATA_DIR, CHECKPOINT_DIR, BROKERS

spark = get_spark_session("KafkaStreaming")
input_path = os.path.join(DATA_DIR, "logs")
check_point_dir = os.path.join(CHECKPOINT_DIR, "kafka/streaming")


@catch_keyboard_interrupt
@time_taken
def main():
    """Legacy park streaming"""
    input_stream = spark.readStream.format("kafka").option("kafka.bootstrap.servers", BROKERS)\
        .option("subscribe", "structured_streaming").load()
    words = input_stream.select(explode(split(input_stream.value, " ")).alias("word"))
    word_count = words.groupBy("word").count()
    result = word_count.writeStream.outputMode("complete").format("console")\
        .option("checkpointLocation", check_point_dir).start()
    result.awaitTermination()
    spark.stop()


if __name__ == '__main__':
    main()
