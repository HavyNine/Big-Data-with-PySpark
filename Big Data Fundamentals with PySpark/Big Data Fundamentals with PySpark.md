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

## CountingBykeys - 8

For many datasets, it is important to count the number of keys in a key/value dataset. For example, counting the number of countries where the product was sold or to show the most popular baby names. In this simple exercise, you'll use the Rdd that you created earlier and count the number of unique keys in that pair RDD.

## Create a base RDD and transform it - 9

The volume of unstructured data in existence is growing every year. Unstructured data is the data that does not have a predefined data model or organization such as text files and images. In this 3 part exercise, you will write a code that calculates the most common words from Complete Works of William Shakespeare.

Here are the brief steps for writing the word counting program:

* Create a base RDD from Complete_Shakespeare.txt file.
* Use RDD transformation to create a long list of words from each element of the base RDD.
* Remove stop words from your data.
* Create pair RDD where each element is a pair tuple of ('w', 1)
* Group the elements of the pair RDD by key (word) and add up their values.
* Swap the keys (word) and values (counts) so that keys is count and value is the word.
* Finally, sort the RDD by descending order and print the 10 most frequent words and their frequencies.

## Remove stop words and reduce the dataset - 10

After splitting the lines in the file into a long list of words in the previous exercise, in the next step, you'll remove stop words from your data. Stop words are common words that are often uninteresting. For example "I", "the", "a" etc., are stop words.

## Print word frequencies - 11

After combining the two datasets and removing stop words, in the last step, you'll print the word frequencies of the top 10 most frequent words. You could have retrieved all the elements at once using collect() but it is bad practice and not recommended. RDDs can be huge: you may run out of memory and crash your computer..

What if we want to return the top 10 words? For this, first you'll need to swap the key (word) and values (counts) so that keys is count and value is the word. After you swap the key and value in the tuple, you'll sort the pair RDD based on the key (count). This way it is easy to sort the RDD based on the key rather than the key using sortByKey operation in PySpark. Finally, you'll return the top 10 words from the sorted RDD.


## RDD to DataFrame - 12

Similar to RDDs, DataFrames are immutable and distributed in nature. You can create DataFrame from RDDs. SparkSession provides an easy method to create DataFrame from an existing RDD. Even though RDDs are a fundamental data structure in Spark, working with data in DataFrame is easier than RDD, and so understanding of how to convert RDD to DataFrame is necessary.

## Loading CSV into DataFrame - 13

In the previous exercise, you have seen a method of creating DataFrame but generally, loading data from CSV file is the most common method of creating DataFrames. In this exercise, you'll create a PySpark DataFrame from a people.csv file that is already provided to you as a file_path and confirm the created object is a PySpark DataFrame.


## Inspecting data in PySpark DataFrame - 14

Inspecting data is very crucial before performing analysis such as plotting, modeling, training etc., In this simple exercise, you'll inspect the data in the people_df DataFrame that you have created in the previous exercise using basic DataFrame operators.

## PySpark DataFrame subsetting and cleaning - 15

After data inspection, it is often necessary to clean the data which mainly involves subsetting, renaming the columns, removing duplicated rows etc., In this simple exercise, you'll practice subsetting, renaming the columns and removing duplicate rows. Pyspark DataFrame API provides a number of functions to do this task effortlessly.

## Filtering your DataFrame - 16

In the previous exercise, you have subset the data using select() operator which is mainly used to subset the DataFrame column-wise. What if you want to subset the DataFrame based on a condition (for example, select all rows where the sex is Female). In this exercise, you will filter the rows in the people_df DataFrame in which 'sex' is female and male and create two different datasets. Finally, you'll count the number of rows in each of those datasets.

