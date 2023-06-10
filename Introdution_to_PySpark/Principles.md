# Using Spark in Python

## SparkContext

Creating the connection is as simple as creating an instance of the SparkContext class. The class constructor takes a few optional arguments that allow you to specify the attributes of the cluster you're connecting to.

An object holding all these attributes can be created with the SparkConf() constructor. Take a look at the [documentation](http://spark.apache.org/docs/latest/configuration.html#available-properties) for all the details!

## Using DataFrames

The Spark DataFrame is a feature that allows you to create and work with DataFrame objects. These DataFrames are actually RDDs of Row objects, where each Row object is structured data, such as a tuple. Spark DataFrames allow you to interface with DataFrames from other languages, such as R and Python. You can also use SQL to query it!

The Spark DataFrame was designed to behave a lot like a SQL table (a table with variables in the columns and observations in the rows). Not only are they easier to understand, DataFrames are also more optimized for complicated operations than RDDs.

To start working with Spark DataFrames, you first have to create a *SparkSession* object from your *SparkContext*. You can think of the *SparkContext* as your connection to the cluster and the
*SparkSession* as your interface with that connection.

## Creating a SparkSession - 1

We've already created a *SparkSession* for you called *spark*, but what if you're not sure there already is one? Creating multiple *SparkSessions* and *SparkContexts* can cause issues, so it's best practice to use the *SparkSession.builder.getOrCreate()* method. This returns an existing *SparkSession* if there's already one in the environment, or creates a new one if necessary!

## Viewing tables - 2

Once you've created a *SparkSession*, you can start poking around to see what data is in your cluster!

Your *SparkSession* has an attribute called *catalog* which lists all the data inside the cluster. This attribute has a few methods for extracting different pieces of information.

One of the most useful is the *.listTables()* method, which returns the names of all the tables in your cluster as a list.

## Are you query-ious? - 3

One of the advantages of the DataFrame interface is that you can run SQL queries on the tables in your Spark cluster. 

Running a query on this table is as easy as using the *.sql()* method on your SparkSession. This method takes a string containing the query and returns a DataFrame with the results!

If you look closely, you'll notice that the table *names* is actually a DataFrame containing table names! To distinguish this from a normal DataFrame, Spark's developers have called it a *temporary view*.

Use the DataFrame method *.show()* to print.


## Pandafy a Spark DataFrame - 4

Suppose you've run a query on your huge dataset and aggregated it down to something a little more manageable.

Sometimes it makes sense to then take that table and work with it locally using a tool like pandas. Spark DataFrames make that easy with the *.toPandas()* method. Calling this method on a Spark DataFrame returns the corresponding pandas DataFrame.

## Put some Spark in your data - 5

In the last section, you saw how to move data from Spark to pandas. However, maybe you want to go the other direction, and put a pandas DataFrame into a Spark cluster!

The SparkSession class has a method for this as well. The *.createDataFrame()* method takes a pandas DataFrame and returns a Spark DataFrame.

The output of this method is stored locally, not in the SparkSession catalog. This means that you can use all the Spark DataFrame methods on it, but you can't access the data in other contexts.

For example, a SQL query (using the .sql() method) that references your DataFrame will throw an error. To access the data in this way, you have to save it as a temporary table.

You can do this using the **.createTempView()** Spark DataFrame method, which takes as its only argument the name of the temporary table you'd like to register. This method registers the DataFrame as a table in the catalog, but as this table is temporary, it can only be accessed from the specific SparkSession used to create the Spark DataFrame.

There is also the method **.createOrReplaceTempView()**. This safely creates a new temporary table if nothing was there before, or updates an existing table if one was already defined. You'll use this method to avoid running into problems with duplicate tables.

![Alt text](image.png)


