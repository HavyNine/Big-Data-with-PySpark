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

## Running SQL Queries Programmatically - 17

DataFrames can easily be manipulated using SQL queries in PySpark. The sql() function on a SparkSession enables applications to run SQL queries programmatically and returns the result as another DataFrame. In this exercise, you'll create a temporary table of the people_df DataFrame that you created previously, then construct a query to select the names of the people from the temporary table and assign the result to a new DataFrame.

## SQL Queries for filtering Table - 18

There are more sophisticated queries you can construct to obtain the result you're looking for. In this exercise, you'll use the SQL query to pass a more complex query and assign the result to a new DataFrame. You'll also create a temporary table of the people_df DataFrame that you created previously. The SQL query selects the names of the people from the temporary table and returns it as another DataFrame.

## PySpark DataFrame visualization - 19

After creating dataframes from the raw dataset, in the real world, we usually want to explore insights from datasets with visualization. Although there are several visualization packages in Python, in this course, we will be using the built-in visualization method df.show(). df.show(n=30, truncate=False) prints n rows of the dataframe. To display all rows, set n to the number of rows in the dataframe. truncate=False ensures that long strings in the rows will not be truncated. In this exercise, you'll explore the data in the people_df DataFrame created at the beginning of this chapter using these techniques.

## Part 1: Create a DataFrame from CSV file - 20

Every 4 years, the soccer fans throughout the world celebrates a festival called “Fifa World Cup” and with that, everything seems to change in many countries. In this exercise, you'll begin to explore the dataset carefully to have a better understanding of the data that you'll be working with throughout the course.

## Part 2: SQL Queries on DataFrame - 21

After creating DataFrames from CSV files, you can easily run SQL queries on them by registering them as temporary tables. Remember, you can run SQL queries over DataFrames that were created using either read.csv() or sqlContext.read.csv() methods. In the previous exercise, you have already seen an example of this. In this exercise, you'll create a temporary table of the people_df DataFrame that you created previously, then construct a query to select the names of the people from the temporary table and assign the result to a new DataFrame.

## Part 3: Data visualization - 22

Data visualization is important for exploratory data analysis (EDA). It is equally important when it comes to communicating the results of your analysis. In this exercise, you'll create plots that visualize the count of countries who participated in either the 2010 or 2014 World Cup. You'll also create a pie chart visualization of the teams with most world cup final victories.

# Machine Learning with PySpark MLlib

### Colaborative Filtering

## PySpark MLlib algorithms - 1

PySpark MLlib provides a number of options when it comes to building machine learning models. In this exercise, you will get introduced to the algorithms currently supported in PySpark MLlib. The goal here is to understand the syntax for implementing these algorithms and get a high level understanding of the steps involved in building a machine learning model. 

## Loading Movie Lens dataset into RDDs - 2

Collaborative filtering is a technique for recommender systems wherein users' ratings and interactions with various products are used to recommend new ones. With the advent of Machine Learning and parallelized processing of data, Recommender systems have become widely popular in recent years, and are utilized in a variety of areas including movies, music, news, books, research articles, search queries, social tags. In this 3-part exercise, your goal is to develop a simple movie recommendation system using PySpark MLlib using a subset of MovieLens 100k dataset.

In the first part, you'll first load the MovieLens data (ratings.csv) into RDD and from each line in the RDD which is formatted as userId,movieId,rating,timestamp, you'll need to map the MovieLens data to a Ratings object (userID, productID, rating) after removing timestamp column and finally you'll split the RDD into training and test RDDs.


## Model training and predictions - 3

After splitting the data into training and test data, in the second part of the exercise, you'll train the ALS algorithm using the training data. PySpark MLlib's ALS algorithm has the following mandatory parameters - rank (the number of latent factors in the model) and iterations (number of iterations to run). After training the ALS model, you can use the model to predict the ratings from the test data. For this, you will provide the user and item columns from the test dataset and finally print the first 2 rows of predictions.

## Model evaluation using MSE - 4

After generating the predicted ratings from the test data using ALS model, in the final part of the exercise, you'll prepare the data for calculating Mean Squared Error (MSE) of the model. The MSE is the average value of (original rating – predicted rating)^2 for all users and indicates the absolute fit of the model to the data. To do this, first, you'll map the ratings from the test RDD and predictions RDDs into a tuple of the form ((user, product), rating)). Then, you'll join the ratings and prediction tuples together. Finally, you'll calculate the MSE by using values from the joined RDD.

### Classification

## Loading the data - 5

Logistic Regression is a popular method to predict a categorical response. Probably one of the most common applications of the logistic regression is the message or email spam classification. In this 3-part exercise, you'll create an email spam classifier with logistic regression using Spark MLlib. Here are the brief steps for creating a spam classifier.

- Create an RDD of strings representing email.
- Run MLlib’s feature extraction algorithms to convert text into an RDD of vectors.
- Call a classification algorithm on the RDD of vectors to return a model object to classify new points.
- Evaluate the model on a test dataset using one of MLlib’s evaluation functions.

## Feature hashing and LabelPoint - 6

After splitting the emails into words, our raw data set of 'spam' and 'non-spam' is currently composed of 1-line messages consisting of spam and non-spam messages. In order to classify these messages, we need to convert text into features.

In the second part of the exercise, you'll first create a HashingTF() instance to map text to vectors of 200 features, then for each message in 'spam' and 'non-spam' files you'll split them into words, and each word is mapped to one feature. These are the features that will be used to decide whether a message is 'spam' or 'non-spam'. Next, you'll create labels for features. For a valid message, the label will be 0 (i.e. the message is not spam) and for a 'spam' message, the label will be 1 (i.e. the message is spam). Finally, you'll combine both the labeled datasets.

## Logistic Regression model training - 7

After creating labels and features for the data, we’re ready to build a model that can learn from it (training). But before you train the model, in this final part of the exercise, you'll split the data into training and test, run Logistic Regression model on the training data, and finally check the accuracy of the model trained on training data.