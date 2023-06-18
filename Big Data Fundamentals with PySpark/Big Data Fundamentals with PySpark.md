# Big Data Fundamentals with PySpark

# Introduction to Big Data analysis with Spark


## Understanging SparkContext - 1

A SparkContext represents the entry point to Spark functionality. It's like a key to your car. PySpark automatically creates a SparkContext for you in the PySpark Shell (so you don't have to create it by yourself) and is exposed via a variable sc.

## Interactive Use of PySpark - 2

PySpark shell is an interactive shell for basic testing and debugging but it is quite powerful. The easiest way to demonstrate the power of PySpark’s shell is to start using it. In this exercise, you'll load a simple list containing numbers ranging from 1 to 100 in the PySpark shell.

The most important thing to understand here is that we are not creating any SparkContext object because PySpark automatically creates the SparkContext object named sc, by default in the PySpark shell.

## Loading Data in PySpark Shell - 3

In PySpark, we express our computation through operations on distributed collections that are automatically parallelized across the cluster. In the previous exercise, you have seen an example of loading a list as parallelized collections and in this exercise, you'll load the data from a local file in PySpark shell.

# Programming in PySpark RDD’s

## RDDs from Parallelized collections - 1

Resilient Distributed Dataset (RDD) is the basic abstraction in Spark. It is an immutable distributed collection of objects. RDD can be created by parallelizing a collection of objects (list, set, etc) or by loading an external dataset from the storage (HDFS, S3, etc).

## RDDs from External Datasets - 2

PySpark can easily create RDDs from files that are stored in external storage devices such as HDFS (Hadoop Distributed File System), Amazon S3 buckets, etc. PySpark refers to these files as "external datasets".

## Partitions in your data - 3

SparkContext's textFile() method takes an optional second argument called minPartitions for specifying the minimum number of partitions. In this exercise, you'll create an RDD named myRDD_part with 6 partitions and then compare that with myRDD that you created in the previous exercise. Refer to the "Understanding Partition" slide in video 2.1 to know the methods for creating and getting the number of partitions in an RDD.

## Map and Collect - 4

The main method by which you can manipulate data in PySpark is using map(). The map() transformation takes in a function and applies it to each element in the RDD. It can be used to do any number of things, from fetching the website associated with each URL in our collection to just squaring the numbers. 


## Filter and Count - 5

The RDD transformation filter() returns a new RDD containing only the elements that satisfy a particular function. It is useful for filtering large datasets based on a keyword.

## ReduceBykey and Collect - 6

One of the most popular pair RDD transformations is reduceByKey() which operates on key, value (k,v) pairs and merges the values for each key

## SortByKey and Collect - 7

Another pair RDD transformation available to you is sortByKey(), which sorts the elements in the RDD based on the key.

