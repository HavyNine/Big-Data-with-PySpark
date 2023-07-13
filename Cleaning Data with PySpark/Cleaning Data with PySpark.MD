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

## Manipulating DataFrames in the real world

## Improving Performance

## Improving Performance