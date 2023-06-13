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

## Dropping the middle man - 6 

Now you know how to put data into Spark via pandas, but you're probably wondering why deal with pandas at all? Wouldn't it be easier to just read a text file straight into Spark? Of course it would!

Luckily, your SparkSession has a **.read** attribute which has several methods for reading different data sources into Spark DataFrames. Using these you can create a DataFrame from a .csv file just like with regular pandas DataFrames!

## Creating Columns - 7

Let's look at performing column-wise operations. In Spark you can do this using the .withColumn() method, which takes two arguments. First, a string with the name of your new column, and second the new column itself.

The new column must be an object of class Column. Creating one of these is as easy as extracting a column from your DataFrame using df.colName.

Updating a Spark DataFrame is somewhat different than working in pandas because the Spark DataFrame is ***immutable***. This means that it can't be changed, and so columns can't be updated in place.

Thus, all these methods return a new DataFrame. To overwrite the original DataFrame you must reassign the returned DataFrame using the method like so:

```python
df = df.withColumn("newCol", df.oldCol + 1)
```

The above code creates a DataFrame with the same columns as df plus a new column, newCol, where every entry is equal to the corresponding entry from oldCol, plus one.

To overwrite an existing column, just pass the name of the column as the first argument!

## Filtering Data - 8

The Spark variant of SQL's WHERE is the **.filter()** method. This method takes either a Spark Column of boolean (True/False) values or the WHERE clause of a SQL expression as a string.

The **.filter()** method returns only rows that satisfy the conditions you specify. It's alias for **.where()**.

For example, the following code would filter the flights DataFrame to only retain rows where the *air_time* was greater than 120 minutes. The two expressions will produce the same output:

```python
flights.filter(flights.air_time > 120).show()
flights.filter("air_time > 120").show()
```

## Selecting - 9

The Spark variant of SQL's SELECT is the **.select()** method. This method takes multiple arguments - one for each column you want to select. These arguments can either be the column name as a string (one for each column) or a column object (using the **df.colName** syntax). When you pass a column object, you can perform operations like addition or subtraction on the column to change the data contained in it, much like inside **.withColumn()**.

The difference between **.select()** and **.withColumn()** methods is that **.select()** returns only the columns you specify, while **.withColumn()** returns all the columns of the DataFrame in addition to the one you defined. It's often a good idea to drop columns you don't need at the beginning of an operation so that you're not dragging around extra data as you're wrangling. In this case, you would use **.select()** and not **.withColumn()**.

## Selecting II - 10

Similar to SQL, you can also use the **.select()** method to perform column-wise operations. When you're selecting a column using the **df.colName** notation, you can perform any column operation and the **.select()** method will return the transformed column. For example,

```python
flights.select(flights.air_time/60)
```

returns a column of flight durations in hours instead of minutes. You can also use the **.alias()** method to rename a column you're selecting. So if you wanted to **.select()** the column *duration_hrs* (which isn't in your DataFrame) you could do

```python
flights.select((flights.air_time/60).alias("duration_hrs"))
```

The equivalent Spark DataFrame method **.selectExpr()** takes SQL expressions as a string:

```python
flights.selectExpr("air_time/60 as duration_hrs")
```

with the SQL **as** keyword being equivalent to the **.alias()** method. To select multiple columns, you can pass multiple strings.

## Aggregating - 11

All of the common aggregation methods, like *.min()*, *.max()*, and *.count()* are GroupedData methods. These are created by calling the *.groupBy()* DataFrame method. You'll learn exactly what that means in a moment. For now, all you have to do to use these functions is call that method on your DataFrame. For example, to find the minimum value of a column, *col*, in a DataFrame, *df*, you could do

```python
df.groupBy().min("col").show()
```

This creates a GroupedData object (so you can use the *.min()* method), then finds the minimum value in *col*, and returns it as a DataFrame.

## Aggregating II - 12

When you use *.groupBy()* you can still use any of the common aggregation methods on the resulting GroupedData object.


## Grouping and Aggregating I - 13

Part of what makes aggregating so powerful is the addition of groups. PySpark has a whole class devoted to grouped data frames: **pyspark.sql.GroupedData**, which you saw in the last exercise.

You've learned how to create a grouped DataFrame by calling the *.groupBy()* method on a DataFrame with no arguments.

Now you'll see that when you pass the name of one or more columns in your DataFrame to the *.groupBy()* method, the aggregation methods behave like when you use a **GROUP BY** statement in a SQL query!

## Grouping and Aggregating II - 14

In addition to the GroupedData methods you've already seen, there is also the *.agg()* method. This method lets you pass an aggregate column expression that uses any of the aggregate functions from the **pyspark.sql.functions** submodule.

This submodule contains many useful functions for computing things like standard deviations. All the aggregation functions in this submodule take the name of a column in a GroupedData table.

Remember, a SparkSession called **spark** is already in your workspace, along with the Spark DataFrame **df**.

