import pandas as pd
import numpy as np


## 1. Creating a SparkSession ##
from pyspark.sql import SparkSession

# Create my_spark
my_spark = SparkSession.builder.getOrCreate()

# Print my_spark
print(my_spark)

## 2. Viewing tables ##

# Print the tables in the catalog
print(my_spark.catalog.listTables())

## 3. Queries ##

# Query the data
query = "FROM flights SELECT * LIMIT 10"

# Get the first 10 rows of flights
flights10 = my_spark.sql(query)

# Show the results
flights10.show()

## 4. Pandafy a Spark DataFrame ##

# Query the data
query = "SELECT origin, dest, COUNT(*) as N FROM flights GROUP BY origin, dest"

# Run the query
flight_counts = my_spark.sql(query)

# Convert the results to a pandas DataFrame
pd_counts = flight_counts.toPandas()

# Print the head of pd_counts
print(pd_counts.head())


## 5. Put some Spark in your data ##

# Create pd_temp
pd_temp = pd.DataFrame(np.random.random(10))

# Create spark_temp from pd_temp
spark_temp = my_spark.createDataFrame(pd_temp)

# Examine the tables in the catalog
print(my_spark.catalog.listTables())

# Add spark_temp to the catalog
spark_temp.createOrReplaceTempView("temp")

# Examine the tables in the catalog again
print(my_spark.catalog.listTables())

## 6. Dropping the middle man ##

# Don't change this file path
file_path = "../you_file.csv"

# Read in the airports data
airports = my_spark.read.csv(file_path, header=True)

# Show the data
airports.show()


## 7. Creating columns ##

# Create the DataFrame flights
flights = my_spark.table("flights")

# Show the head
flights.show()

# Add duration_hrs
flights = flights.withColumn("duration_hrs", flights.air_time/60)

## 8. Filtering Data ##

# Filter flights by passing a string
long_flights1 = flights.filter("distance > 1000")

# Filter flights by passing a column of boolean values
long_flights2 = flights.filter(flights.distance > 1000)

# Print the data to check they're equal
long_flights1.show()
long_flights2.show()

## 9. Selecting ##

# Select the first set of columns
selected1 = flights.select("tailnum", "origin", "dest")

# Select the second set of columns
temp = flights.select(flights.origin, flights.dest, flights.carrier)

# Define first filter
filterA = flights.origin == "SEA"

# Define second filter

filterB = flights.dest == "PDX"

# Filter the data, first by filterA then by filterB
selected2 = temp.filter(filterA).filter(filterB)
selected2.show()

## 10. Selecting II ##

# Define avg_speed
avg_speed = (flights.distance/(flights.air_time/60)).alias("avg_speed")

# Select the correct columns
speed1 = flights.select("origin", "dest", "tailnum", avg_speed)

# Create the same table using a SQL expression
speed2 = flights.selectExpr("origin", "dest", "tailnum", "distance/(air_time/60) as avg_speed")

## 11. Aggregating ##

# Find the shortest flight from PDX in terms of distance
flights.filter(flights.origin == "PDX").groupBy().min("distance").show()

# Find the longest flight from SEA in terms of duration
flights.filter(flights.origin == "SEA").groupBy().max("air_time").show()

## 12. Aggregating II ##

# Average duration of Delta flights
flights.filter(flights.carrier == "DL").filter(flights.origin == "SEA").groupBy().avg("air_time").show()

# Total hours in the air
flights.withColumn("duration_hrs", flights.air_time/60).groupBy().sum("duration_hrs").show()