# coding: utf-8
# -*- coding: utf-8 -*-

__author__ = "Jalpesh Borad"
__email__ = "jalpeshborad@gmail.com"

import os
from pyspark.sql.functions import col, from_json, get_json_object
from pyspark.sql.types import StructField, StructType, IntegerType, DateType, StringType, MapType

from base.context import get_spark_session
from base.decorators import time_taken, catch_keyboard_interrupt
from configs.config import DATA_DIR, CHECKPOINT_DIR, BROKERS

spark = get_spark_session("KafkaStreaming")
input_path = os.path.join(DATA_DIR, "logs")
check_point_dir = os.path.join(CHECKPOINT_DIR, "kafka/streaming_0")

table_schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("dob", IntegerType()),
    StructField("address", StringType()),
    StructField("gender", StringType()),
])

schema = StructType([
    StructField("before", table_schema),
    StructField("after", table_schema),
    StructField("op", StringType())
])


@catch_keyboard_interrupt
@time_taken
def main():
    """Legacy park streaming"""
    input_stream = spark.readStream.format("kafka").option("kafka.bootstrap.servers", BROKERS) \
        .option("startingOffsets", "earliest")\
        .option("failOnDataLoss", False)\
        .option("subscribe", "uninstall.data.t01").load()

    input_stream = input_stream.select(
        get_json_object(input_stream.value.cast("string"), "$.payload").alias("value"))\
        .select(from_json(col("value"), schema).alias("payload")).select(col("payload").getItem("after").alias("value"))

    payload = input_stream.select(col("value").getItem("id").alias("id"),
                           col("value").getItem("name").alias("name"),
                           col("value").getItem("dob").alias("dob"),
                           col("value").getItem("address").alias("address"),
                           col("value").getItem("gender").alias("gender"),
                           )
    words = payload.groupBy("name").count().orderBy("count", ascending=False)

    result = words.writeStream.outputMode("complete").format("console")\
        .option("checkpointLocation", check_point_dir).start()

    result.awaitTermination()
    spark.stop()


if __name__ == '__main__':
    main()
