# coding: utf-8
# -*- coding: utf-8 -*-

__author__ = "Jalpesh Borad"
__email__ = "jalpeshborad@gmail.com"

"""
Apache spark is a very fast and general engine for large scale data processing.
    It can be used for batched data processing as well as Real time data processing.

Runs 100x faster that Hadoop MapReduce in memory and 10x faster on disk.
It creates DAG (Directed acyclic graph) for job execution which optimizes work-flows.

Components of Spark:
    Spark Core
        > Spark Streaming
        > Spark SQL
        > MLLib
        > GraphX (SparkGraph)

RDD (Resilient Distributed data-set) is one of the core concept of Spark.
    >> It is a [BIG] Data set which can be distributed across number of executor nodes.
       Also, if one of the workers goes down, Spark re-assigns the job to other worker, so it is resilient.
    >> Operations:
        >>> Transformations [Lazily evaluated]:
            > map, flatmap, sample, distinct, union, filter, union, intersection, subtract, cartesian etc.
        >>> Actions (Kicks in DAG creation and execution)
            > collect, count, countByValue, take, top, reduce etc.
            > It loads results to the machine where driver program is being run,
             so after calling you can manipulate/re-present the data the way you want by using 
             respective programming language.
            
        `Nothing actually happens in driver program unless action is called.`
    
    >> Key Value RDDs
        With Key/Value data, use mapValues() and flatMapValues() if transformations doesn't affect keys, 
        because it is more efficient.
        It allows spark to maintain partitioning of original RDD instead of shuffling data around,
        which can be expensive when running on cluster.
    
    Type-casting of values is very important when you want to do arithmetic operations on RDDs.

The SparkContext:
    It is created by Driver Program.
    It is responsible to make RDDs resilient and distributed
    It creates RDDs
    >> SparkShell automatically created Context object as "sc" variable

Broadcast Variables:
    broadcasts objects to the executors such that they are always there whenever needed
    sc.broadcast() to ship off whatever you want
    use .value() to get the object back
    
Accumulators:
    It is a variable that allows many executors to increment a shared variable.
"""
