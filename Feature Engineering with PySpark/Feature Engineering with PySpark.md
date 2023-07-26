# Exploratory Data Analysis

## 1. Check Version

Checking the version of which Spark and Python installed is important as it changes very quickly and drastically. Reading the wrong documentation can cause lots of lost time and unnecessary frustration!

## 2. Load in the data

Reading in data is the first step to using PySpark for data science! Let's leverage the new industry standard of parquet files!

## 3. What are we predicting?

Which of these fields (or columns) is the value we are trying to predict for?

- TAXES
- SALESCLOSEPRICE
- DAYSONMARKET
- LISTPRICE

----
- From the listed columns above, identify which one we will use as our dependent variable $Y$.
- Using the loaded data set df, filter it down to our dependent variable with select(). Store this dataframe in the variable Y_df.
- Display summary statistics for the dependent variable using describe() on Y_df and calling show() to display it.

## 4. Verifying Data Load

Let's suppose each month you get a new file. You know to expect a certain number of records and columns. In this exercise we will create a function that will validate the file loaded.

- Create a data validation function check_load() with parameters df a dataframe, num_records as the number of records and num_columns the number of columns.
- Using num_records create a check to see if the input dataframe df has the same amount with count().
- Compare input number of columns the input dataframe has withnum_columns by using len() on columns.
- If both of these return True, then print Validation Passed

## 5. Verifying DataTypes

In the age of data we have access to more attributes than we ever had before. To handle all of them we will build a lot of automation but at a minimum requires that their datatypes be correct. In this exercise we will validate a dictionary of attributes and their datatypes to see if they are correct. This dictionary is stored in the variable validation_dict and is available in your workspace.

- Using df create a list of attribute and datatype tuples with dtypes called actual_dtypes_list.
- Iterate through actual_dtypes_list, checking if the column names exist in the dictionary of expected dtypes validation_dict.
- For the keys that exist in the dictionary, check their dtypes and print those that match.


## 6. Visually Inspecting Data / EDA

### Using Corr()

The old adage 'Correlation does not imply Causation' is a cautionary tale. However, correlation does give us a good nudge to know where to start looking promising features to use in our models. Use this exercise to get a feel for searching through your data for the first time, trying to find patterns.

A list called columns containing column names has been created for you. In this exercise you will compute the correlation between those columns and 'SALESCLOSEPRICE', and find the maximum.

- Use a for loop iterate through the columns.
- In each loop cycle, compute the correlation between the current column and 'SALESCLOSEPRICE' using the corr() method.
- Create logic to update the maximum observed correlation and with which column.
- Print out the name of the column that has the maximum correlation with 'SALESCLOSEPRICE'.

### Using Visualizations: distplot

Understanding the distribution of our dependent variable is very important and can impact the type of model or preprocessing we do. A great way to do this is to plot it, however plotting is not a built in function in PySpark, we will need to take some intermediary steps to make sure it works correctly. In this exercise you will visualize the variable the 'LISTPRICE' variable, and you will gain more insights on its distribution by computing the skewness.

The matplotlib.pyplot and seaborn packages have been imported for you with aliases plt and sns.

- Sample 50% of the dataframe df with sample() making sure to not use replacement and setting the random seed to 42.
- Convert the Spark DataFrame to a pandas.DataFrame() with toPandas().
- Plot a distribution plot using seaborn's distplot() method.
- Import the skewness() function from pyspark.sql.functions and compute it on the aggregate of the 'LISTPRICE' column with the agg() method. Remember to collect() your result to evaluate the computation.


### Using Visualizations: lmplot

Creating linear model plots helps us visualize if variables have relationships with the dependent variable. If they do they are good candidates to include in our analysis. If they don't it doesn't mean that we should throw them out, it means we may have to process or wrangle them before they can be used.

seaborn is available in your workspace with the customary alias sns.

- Using the loaded data set df filter it down to the columns 'SALESCLOSEPRICE' and 'LIVINGAREA' with select().
- Sample 50% of the dataframe with sample() making sure to not use replacement and setting the random seed to 42.
- Convert the Spark DataFrame to a pandas.DataFrame() with toPandas().
- Using 'SALESCLOSEPRICE' as your dependent variable and 'LIVINGAREA' as your independent, plot a linear model plot using seaborn lmplot().