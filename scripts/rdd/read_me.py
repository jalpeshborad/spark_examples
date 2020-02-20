# coding: utf-8
# -*- coding: utf-8 -*-

__author__ = "Jalpesh Borad"
__email__ = "jalpeshborad@gmail.com"

from base.context import get_spark_context

sc = get_spark_context()

rdd = sc.textFile('/usr/local/opt/spark/README.md')
print(rdd.count())
