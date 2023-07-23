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

## adding an ID field - 10

# Select all the unique council voters
voter_df = voter_df.select(['VOTER_NAME', 'first_name', 'last_name', 'first_and_middle_name']).distinct()

# Count the rows in voter_df
print("\nThere are %d rows in the voter_df DataFrame.\n" % voter_df.count())

# Add a ROW_ID
voter_df = voter_df.withColumn('ROW_ID', F.monotonically_increasing_id())

# Show the rows with 10 highest IDs in the set
voter_df.orderBy(voter_df['ROW_ID'].desc()).show(10)

## IDs with different partitions - 11

# Print the number of partitions in each DataFrame
print("\nThere are %d partitions in the voter_df DataFrame.\n" % voter_df.rdd.getNumPartitions())
print("\nThere are %d partitions in the voter_df_single DataFrame.\n" % voter_df_single.rdd.getNumPartitions())

# Add a ROW_ID field to each DataFrame
voter_df = voter_df.withColumn('ROW_ID', F.monotonically_increasing_id())
voter_df_single = voter_df_single.withColumn('ROW_ID', F.monotonically_increasing_id())

# Show the top 10 IDs in each DataFrame
voter_df.orderBy(voter_df['ROW_ID'].desc()).show(10)
voter_df_single.orderBy(voter_df_single['ROW_ID'].desc()).show(10)

## More ID tricks - 12

# Determine the highest ROW_ID and save it in previous_max_ID
previous_max_ID = voter_df_march.orderBy(voter_df_march['ROW_ID'].desc()).first()[3]

# Add a ROW_ID column to voter_df_april starting at the desired value
voter_df_april = voter_df_april.withColumn('ROW_ID', F.monotonically_increasing_id() + previous_max_ID)

# Show the ROW_ID from both DataFrames and compare
voter_df_march.select('ROW_ID').orderBy(voter_df_march['ROW_ID'].desc()).show(10)
voter_df_april.select('ROW_ID').orderBy(voter_df_april['ROW_ID'].desc()).show(10)


## Improving Performance

# Caching a DataFrame - 13

start_time = time.time()

# Add caching to the unique rows in departures_df
departures_df = departures_df.distinct().cache()

# Count the unique rows in departures_df, noting how long the operation takes
print("Counting %d rows took %f seconds" % (departures_df.count(), time.time() - start_time))

# Count the rows again, noting the variance in time of a cached DataFrame
start_time = time.time()
print("Counting %d rows again took %f seconds" % (departures_df.count(), time.time() - start_time))

# File import performance - 14

# Import the full and split files into DataFrames
full_df = spark.read.csv('departures_full.txt.gz')
split_df = spark.read.csv('departures_013.txt.gz')

# Print the count and run time for each DataFrame
start_time_a = time.time()
print("Total rows in full DataFrame:\t%d" % full_df.count())
print("Time to run: %f" % (time.time() - start_time_a))

start_time_b = time.time()
print("Total rows in split DataFrame:\t%d" % split_df.count())
print("Time to run: %f" % (time.time() - start_time_b))

# Reading Spark configurations

# Name of the Spark application instance
app_name = spark.conf.get('spark.app.name')

# Driver TCP port
driver_tcp_port = spark.conf.get('spark.driver.port')

# Number of join partitions
num_partitions = spark.conf.get('spark.sql.shuffle.partitions')

# Show the results
print("Name: %s" % app_name)
print("Driver TCP port: %s" % driver_tcp_port)
print("Number of partitions: %s" % num_partitions)

# Writing Spark configurations - 15

# Store the number of partitions in variable
before = departures_df.rdd.getNumPartitions()

# Configure Spark to use 500 partitions
spark.conf.set('spark.sql.shuffle.partitions', 500)

# Recreate the DataFrame using the departures data file
departures_df = spark.read.csv('departures.txt.gz').distinct()

# Print the number of partitions for each instance
print("Partition count before change: %d" % before)
print("Partition count after change: %d" % departures_df.rdd.getNumPartitions())

# Normal joins - 16

# Join the flights_df and aiports_df DataFrames
normal_df = flights_df.join(airports_df, \
    flights_df['Destination Airport'] == airports_df['IATA'])

# Show the query plan
normal_df.explain()

# Using broadcasting on Spark joins - 17

# Import the broadcast method from pyspark.sql.functions
from pyspark.sql.functions import broadcast

# Join the flights_df and aiports_df DataFrames using broadcasting
broadcast_df = flights_df.join(broadcast(airports_df), \
    flights_df['Destination Airport'] == airports_df['IATA'])

# Show the query plan and compare against the original
broadcast_df.explain()

## Complex processing and data pipelines

# Quick pipeline - 18

# Import the data to a DataFrame
departures_df = spark.read.csv('2015-departures.csv.gz', header=True)
departures_df.show(1)
# Remove any duration of 0
departures_df = departures_df.filter(departures_df['Actual elapsed time (Minutes)'] > 0)

# Add an ID column
departures_df = departures_df.withColumn('id', F.monotonically_increasing_id())

# Write the file out to JSON format
departures_df.write.json('output.json', mode='overwrite')

## Removing commented lines - 19

# Import the file to a DataFrame and perform a row count
annotations_df= spark.read.csv('annotations.csv.gz', sep='|')
full_count = annotations_df.count()

# Count the number of rows beginning with '#'
comment_count = annotations_df.where(col('_c0').startswith('#')).count()

# Import the file to a new DataFrame, without commented rows
no_comments_df = spark.read.csv('annotations.csv.gz', sep='|', comment='#')

# Count the new DataFrame and verify the difference is as expected
no_comments_count = no_comments_df.count()
print("Full count: %d\nComment count: %d\nRemaining count: %d" % (full_count, comment_count, no_comments_count))


## Removing invalid rows - 20

# Split _c0 on the tab character and store the list in a variable
tmp_fields = F.split(annotations_df['_c0'], '\t')

# Create the colcount column on the DataFrame
annotations_df = annotations_df.withColumn('colcount', F.size(tmp_fields))

# Remove any rows containing fewer than 5 fields
annotations_df_filtered = annotations_df.filter(~ (annotations_df['colcount'] < 5))

# Count the number of rows
final_count = annotations_df_filtered.count()
print("Initial count: %d\nFinal count: %d" % (initial_count, final_count))

## Splitting into columns - 21

# Split the content of _c0 on the tab character (aka, '\t')
split_cols = F.split(annotations_df['_c0'], '\t')

# Add the columns folder, filename, width, and height
split_df = annotations_df.withColumn('folder', split_cols.getItem(0))
split_df = split_df.withColumn('filename',  split_cols.getItem(1))
split_df = split_df.withColumn('width',  split_cols.getItem(2))
split_df = split_df.withColumn('height',  split_cols.getItem(3))


# Add split_cols as a column
split_df = split_df.withColumn('split_cols', split_cols)

## Futher parsing - 22

def retriever(cols, colcount):
  # Return a list of dog data
  return cols[4:colcount]

# Define the method as a UDF
udfRetriever = F.udf(retriever, ArrayType(StringType()))

split_df.show()
# Create a new column using your UDF
split_df = split_df.withColumn('dog_list', udfRetriever(split_df.split_cols,split_df.colcount))

# Remove the original column, split_cols, and the colcount
split_df = split_df.drop('split_cols').drop('_c0').drop('colcount')