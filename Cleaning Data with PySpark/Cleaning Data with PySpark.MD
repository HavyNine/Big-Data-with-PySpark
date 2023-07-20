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

## Improving Performance

## Improving Performance