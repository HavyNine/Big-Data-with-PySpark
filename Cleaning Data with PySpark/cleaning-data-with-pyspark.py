### Cleaning Data with PySpark ###

# DataFrame details 

## Defining a schema - 1

# Import the pyspark.sql.types library
from pyspark.sql.types import *

# Define a new schema using the StructType method
people_schema = StructType([
    # Define a StructField for each field
    StructField('name', StringType(), False),
    StructField('age', IntegerType(), False),
    StructField('city', StringType(), False)
])

# Using lazy processing - 2

# Load the CSV file
aa_dfw_df = spark.read.format('csv').options(Header=True).load('AA_DFW_2018.csv.gz')

# Add the airport column using the F.lower() method
aa_dfw_df = aa_dfw_df.withColumn('airport', F.lower(aa_dfw_df['Destination Airport']))

# Drop the Destination Airport column
aa_dfw_df = aa_dfw_df.drop(aa_dfw_df['Destination Airport'])

# Show the DataFrame
aa_dfw_df.show()

# Saving a DataFrame in Parquet format - 3

# View the row count of df1 and df2
print("df1 Count: %d" % df1.count())
print("df2 Count: %d" % df2.count())

# Combine the DataFrames into one
df3 = df1.union(df2)

# Save the df3 DataFrame in Parquet format
df3.write.parquet('AA_DFW_ALL.parquet', mode='overwrite')

# SQL and Parquet - 4

# Read the Parquet file into a new DataFrame and run a count
flights_df = spark.read.parquet('AA_DFW_ALL.parquet')

# Register the temp table
flights_df.createOrReplaceTempView('flights')

# Run a SQL query of the average flight duration
avg_duration_df = spark.sql('SELECT avg(flight_duration) from flights').show()

## Manipulating DataFrames in the real world - 5

# Show the distinct VOTER_NAME entries
voter_df.select('VOTER_NAME').distinct().show(40, truncate=False)

# Filter voter_df where the VOTER_NAME is 1-20 characters in length
voter_df = voter_df.filter('length(VOTER_NAME) > 0 and length(VOTER_NAME) < 20')

# Filter out voter_df where the VOTER_NAME contains an underscore
voter_df = voter_df.filter(~ F.col('VOTER_NAME').contains('_'))

# Show the distinct VOTER_NAME entries again
voter_df.select('VOTER_NAME').distinct().show(40, truncate=False)

## Modifying DataFrame columns - 6

# Add a new column called splits separated on whitespace
voter_df = voter_df.withColumn('splits', F.split(voter_df['VOTER_NAME'], '\s+'))

# Create a new column called first_name based on the first item in splits
voter_df = voter_df.withColumn('first_name', voter_df['splits'].getItem(0))

# Get the last entry of the splits list and create a column called last_name
voter_df = voter_df.withColumn('last_name', voter_df['splits'].getItem(F.size('splits') - 1))

## When clauses  - 7

# Add a column to voter_df for any voter with the title **Councilmember**
voter_df = voter_df.withColumn('random_val', F.when(voter_df['TITLE'] == 'Councilmember', F.rand()))

## When/otherwise clauses - 8

# Add a column to voter_df for a voter based on their position
voter_df = voter_df.withColumn('random_val',
                                 F.when(voter_df['TITLE'] == 'Councilmember', F.rand())
                                    .when(voter_df['TITLE'] == 'Mayor', 2)
                                    .otherwise(0))

# Show some of the DataFrame rows, noting whether the when clause worked
voter_df.show()

# Use the .filter() clause with random_val
voter_df.filter(voter_df['random_val'] == 0).show()

## User defined functions - 9

# Define the method
def getFirstAndMiddle(names):
    # Return a space separated string of names
    return ' '.join(names[0:2])

# Define the method as a UDF
udfFirstAndMiddle = F.udf(getFirstAndMiddle, StringType())

# Create a new column using your UDF
voter_df = voter_df.withColumn('first_and_middle_name', udfFirstAndMiddle(voter_df['splits']))

# Show the DataFrame
voter_df.show()



## Improving Performance

## Improving Performance