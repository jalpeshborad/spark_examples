# coding: utf-8
# -*- coding: utf-8 -*-

__author__ = "Jalpesh Borad"
__email__ = "jalpeshborad@gmail.com"

from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession


def get_spark_context(app_name="Default"):
    return SparkContext.getOrCreate(SparkConf().setMaster("local[*]").setAppName(app_name))


def get_spark_session(app_name="Default"):
    return SparkSession.builder.appName(app_name).getOrCreate()
