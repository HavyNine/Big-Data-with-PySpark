### Introduction to Big Data analysis with Spark ###

## 1. Understanding SparkContext

# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Create spark_session

spark = SparkSession.builder.getOrCreate()

# Print spark_session
print(spark)

# Print the version of spark
print(spark.version)

# Print the Python version of spark
print(spark.sparkContext.pythonVer)

# Print the master of spark
print(spark.sparkContext.master)

## 2. Interactive Use of PySpark

# Create a Python list of numbers from 1 to 100
numb = range(1, 100)

# Load the list into PySpark
spark_data = spark.sparkContext.parallelize(numb)

## 3. Loading Data in PySpark Shell

# Load a local file into PySpark shell
lines = spark.sparkContext.textFile('file:///home/repl/exemple_read.csv')


### Programming in PySpark RDD’s ###

## 1. RDDs from Parallelized collections

# Create an RDD from a list of words
RDD = spark.sparkContext.parallelize(["Spark", "is", "a", "framework", "for", "Big Data processing"])

# Print out the type of the created object
print("The type of RDD is", type(RDD))

## 2. RDDs from External Datasets

# Print the file_path
print("The file_path is", file_path)

# Create a fileRDD from file_path
fileRDD = spark.sparkContext.textFile(file_path)

# Check the type of fileRDD
print("The file type of fileRDD is", type(fileRDD))

## 3. Partitions in your data

# Check the number of partitions in fileRDD
print("Number of partitions in fileRDD is", fileRDD.getNumPartitions())

# Create a fileRDD_part from file_path with 5 partitions
fileRDD_part = spark.sparkContext.textFile(file_path, minPartitions=5)

# Check the number of partitions in fileRDD_part
print("Number of partitions in fileRDD_part is", fileRDD_part.getNumPartitions())

