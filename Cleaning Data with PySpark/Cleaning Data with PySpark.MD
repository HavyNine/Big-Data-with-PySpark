# Cleaning Data with PySpark

## DataFrame details

### Defining a schema - 1

Creating a defined schema helps with data quality an import performance. As mentioned during the lesson, we'll create a simple schema to read in the following columns:

- Name
- Age
- City
The Name and City columns are StringType() and the Age column is an **IntegerType()**.

### Using lazy processing - 2

Lazy processing operations will usually return in about the same amount of time regardless of the actual quantity of data. Remember that this is due to Spark not performing any transformations until an action is requested.

For this exercise, we'll be defining a Data Frame (aa_dfw_df) and add a couple transformations. Note the amount of time required for the transformations to complete when defined vs when the data is actually queried. These differences may be short, but they will be noticeable. When working with a full Spark cluster with larger quantities of data the difference will be more apparent.

### Understanding Parquet - 3
#### Saving a DataFrame in Parquet format

When working with Spark, you'll often start with CSV, JSON, or other data sources. This provides a lot of flexibility for the types of data to load, but it is not an optimal format for Spark. The Parquet format is a columnar data store, allowing Spark to use predicate pushdown. This means Spark will only process the data necessary to complete the operations you define versus reading the entire dataset. This gives Spark more flexibility in accessing the data and often drastically improves performance on large datasets.

In this exercise, we're going to practice creating a new Parquet file and then process some data from it.

### SQL and Parquet - 4

Parquet files are perfect as a backing data store for SQL queries in Spark. While it is possible to run the same queries directly via Spark's Python functions, sometimes it's easier to run SQL queries alongside the Python options.

For this example, we're going to read in the Parquet file we created in the last exercise and register it as a SQL table. Once registered, we'll run a quick query against the table (aka, the Parquet file).


## Manipulating DataFrames in the real world

### Filtering column content with Python - 1

You've looked at using various operations on DataFrame columns - now you can modify a real dataset. The DataFrame voter_df contains information regarding the voters on the Dallas City Council from the past few years. This truncated DataFrame contains the date of the vote being cast and the name and position of the voter. Your manager has asked you to clean this data so it can later be integrated into some desired reports. The primary task is to remove any null entries or odd characters and return a specific set of voters where you can validate their information.

This is often one of the first steps in data cleaning - removing anything that is obviously outside the format. For this dataset, make sure to look at the original data and see what looks out of place for the VOTER_NAME column.

### modifying DataFrame columns - 2

Previously, you filtered out any rows that didn't conform to something generally resembling a name. Now based on your earlier work, your manager has asked you to create two new columns - first_name and last_name. She asks you to split the VOTER_NAME column into words on any space character. You'll treat the last word as the last_name, and all other words as the first_name. You'll be using some new functions in this exercise including .split(), .size(), and .getItem(). The .getItem(index) takes an integer value to return the appropriately numbered item in the column. The functions .split() and .size() are in the pyspark.sql.functions library.

Please note that these operations are always somewhat specific to the use case. Having your data conform to a format often matters more than the specific details of the format. Rarely is a data cleaning task meant just for one person - matching a defined format allows for easier sharing of the data later (ie, Paul doesn't need to worry about names - Mary already cleaned the dataset).

### When() - 3

The .when() function allows you to when() -> then() style conditional statements to DataFrame operations. You'll want to use this whenever you want to add an if/else statement to DataFrame operations.

### When / Otherwise - 4

This requirement is similar to the last, but now you want to add multiple values based on the voter's position. Modify your voter_df DataFrame to add a random number to any voting member that is defined as a Councilmember. Use 2 for the Mayor and 0 for anything other position.

### User defined functions - 5

You've seen some of the power behind Spark's built-in string functions when it comes to manipulating DataFrames. However, once you reach a certain point, it becomes difficult to process the data in a without creating a rat's nest of function calls. Here's one place where you can use User Defined Functions to manipulate our DataFrames.

### Adding an ID field - 6

When working with data, you sometimes only want to access certain fields and perform various operations. In this case, find all the unique voter names from the DataFrame and add a unique ID number. Remember that Spark IDs are assigned based on the DataFrame partition - as such the ID values may be much greater than the actual number of rows in the DataFrame.

With Spark's lazy processing, the IDs are not actually generated until an action is performed and can be somewhat random depending on the size of the dataset.

### IDs with different partitions - 7

You've just completed adding an ID field to a DataFrame. Now, take a look at what happens when you do the same thing on DataFrames containing a different number of partitions.

To check the number of partitions, use the method **.rdd.getNumPartitions()** on a DataFrame.

### More ID tricks - 8

Once you define a Spark process, you'll likely want to use it many times. Depending on your needs, you may want to start your IDs at a certain value so there isn't overlap with previous runs of the Spark task. This behavior is similar to how IDs would behave in a relational database. You have been given the task to make sure that the IDs output from a monthly Spark task start at the highest value from the previous month.


## Improving Performance

### Caching a DataFrame - 1

You've been assigned a task that requires running several analysis operations on a DataFrame. You've learned that caching can improve performance when reusing DataFrames and would like to implement it.

You'll be working with a new dataset consisting of airline departure information. It may have repetitive data and will need to be de-duplicated.

### Removing a DataFrame from cache - 2

You've finished the analysis tasks with the airlines DataFrame, but have some other processing to do. You'd like to remove the DataFrame from the cache to prevent any excess memory usage on your cluster.

### File import performance - 3

You've been given a large set of data to import into a Spark DataFrame. You'd like to test the difference in import speed by splitting up the file.

### Reading Spark configurations - 4

You've recently configured a cluster via a cloud provider. Your only access is via the command shell or your python code. You'd like to verify some Spark settings to validate the configuration of the cluster.

### Writng Spark configurations - 5

You've recently configured a cluster via a cloud provider. Your only access is via the command shell or your python code. You'd like to verify some Spark settings to validate the configuration of the cluster.

### Normal join - 6

You've been given two DataFrames to combine into a single useful DataFrame. Your first task is to combine the DataFrames normally and view the execution plan.

### Broadcast join - 7

Remember that table joins in Spark are split between the cluster workers. If the data is not local, various shuffle operations are required and can have a negative impact on performance. Instead, we're going to use Spark's broadcast operations to give each node a copy of the specified data.

A couple tips:

- Broadcast the smaller DataFrame. The larger the DataFrame, the more time required to transfer to the worker nodes.
- On small DataFrames, it may be better skip broadcasting and let.Spark figure out any optimization on its own.
- If you look at the query execution plan, a broadcastHashJoin indicates you've successfully configured broadcasting.

### Comparing boradcast vs normal join - 8

You've created two types of joins, normal and broadcasted. Now your manager would like to know what the performance improvement is by using Spark optimizations. If the results are promising, you'll be given more opportunity to tweak the Spark setup as needed.

## Complex processing and data pipelines

### Quick pipeline - 1

Before you parse some more complex data, your manager would like to see a simple pipeline example including the basic steps. For this example, you'll want to ingest a data file, filter a few rows, add an ID column to it, then write it out as JSON data.

### Data handling techniques 

### Removing commented lines - 2

Your boss would like you to perform some complex parsing on a new dataset. The data represents annotation data for the ImageNet dataset, but focusing specifically on dog breeds and identifying them in images. Before any actual analysis can occur, you'll need to clear out several components of invalid / incorrect data. The general schema of the document is unknown so you'd like to import the rows into a single column, allowing for quick analysis.

To start, you need to remove all commented rows in the dataset.

### Removing invalid rows - 3

Now that you've successfully removed the commented rows, you have received some information about the general format of the data. There should be at minimum 5 tab separated columns in the DataFrame. Remember that your original DataFrame only has a single column, so you'll need to split the data on the tab (\t) characters.

### Splitting into columns - 4

You've cleaned up your data considerably by removing the invalid rows from the DataFrame. Now you want to perform some further transformations by generating specific meaningful columns based on the DataFrame content.

### Further parsing - 5

You've molded this dataset into a significantly different format than it was before, but there are still a few things left to do. You need to prep the column data for use in later analysis and remove a few intermediary columns.

### Validate rows via join - 6

Another example of filtering data is using joins to remove invalid entries. You'll need to verify the folder names are as expected based on a given DataFrame named valid_folders_df. The DataFrame split_df is as you last left it with a group of split columns.

### Examining invalid rows - 7

You've successfully filtered out the rows using a join, but sometimes you'd like to examine the data that is invalid. This data can be stored for later processing or for troubleshooting your data sources.

You want to find the difference between two DataFrames and store the invalid rows.

### Dog parsing - 8

You've done a considerable amount of cleanup on the initial dataset, but now need to analyze the data a bit deeper. There are several questions that have now come up about the type of dogs seen in an image and some details regarding the images. You realize that to answer these questions, you need to process the data into a specific type. Before you can use it, you'll need to create a schema / type to represent the dog details.

### Per image count - 9

Your next task in building a data pipeline for this dataset is to create a few analysis oriented columns. You've been asked to calculate the number of dogs found in each image based on your dog_list column created earlier. You have also created the DogType which will allow better parsing of the data within some of the data columns.

### Percentage dog pixels - 10

The final task for parsing the dog annotation data is to determine the percentage of pixels in each image that represents a dog (or dogs). You'll need to use the various techniques you've learned in this course to help calculate this information and add it as columns for later analysis.

To calculate the percentage of pixels, first calculate the total number of pixels representing each dog then sum them for the image. You can calculate the bounding box with the formula:

(Xend - Xstart) * (Yend - Ystart)

NOTE: You can ignore the possibility of overlapping bounding boxes in this instance.