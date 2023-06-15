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

## 13. Grouping and Aggregating I ##

# Group by tailnum
by_plane = flights.groupBy("tailnum")

# Number of flights each plane made
by_plane.count().show()

# Group by origin
by_origin = flights.groupBy("origin")

# Average duration of flights from PDX and SEA
by_origin.avg("air_time").show()

## 14. Grouping and Aggregating II ##

# Import pyspark.sql.functions as F
import pyspark.sql.functions as F

# Group by month and dest
by_month_dest = flights.groupBy("month", "dest")

# Average departure delay by month and destination
by_month_dest.avg("dep_delay").show()

# Standard deviation of departure delay
by_month_dest.agg(F.stddev("dep_delay")).show()

## 15. Joining ##

# Examine the data
print(airports.show())

# Rename the faa column
airports = airports.withColumnRenamed("faa", "dest")

# Join the DataFrames
flights_with_airports = flights.join(airports, on="dest", how="leftouter")

# Examine the data again
print(flights_with_airports.show())

## 16. Joining II ##

# Examine the data
print(airports.show())

# Rename the faa column
airports = airports.withColumnRenamed("faa","dest")

# Rename year column
planes = planes.withColumnRenamed("year","plane_year")

# Join the DataFrames
model_data = flights.join(airports,"dest","leftouter")

# Examine the new DataFrame
print(model_data.show())


## 17. String to integer ##

#Cast the columns to integers
flights = flights.withColumn("arr_delay", flights.arr_delay.cast("integer"))
flights = flights.withColumn("air_time", flights.air_time.cast("integer"))
flights = flights.withColumn("month", flights.month.cast("integer"))

## 18. Create the column ##

#Create the column plane_age
model_data = model_data.withColumn("plane_age", model_data.year - model_data.plane_year)


## 19. Making a Boolean ##

#Create is_late
model_data = model_data.withColumn("is_late", model_data.arr_delay > 0)

#Convert to an integer
model_data = model_data.withColumn("label", model_data.is_late.cast("integer"))

#Remove missing values
model_data = model_data.filter("arr_delay is not NULL and dep_delay is not NULL and air_time is not NULL and plane_year is not NULL")

## 20. Carrier ##

#Create a StringIndexer
carr_indexer = StringIndexer(inputCol="carrier", outputCol="carrier_index")

#Create a OneHotEncoder
carr_encoder = OneHotEncoder(inputCol="carrier_index", outputCol="carrier_fact")

## 21. Assemble a vector ##

#Make a VectorAssembler
vec_assembler = VectorAssembler(inputCols=["month", "air_time", "carrier_fact", "plane_age"], outputCol="features")

## 22. Create the pipeline ##

#Import Pipeline
from pyspark.ml import Pipeline

#Make the pipeline
flights_pipe = Pipeline(stages=[dest_indexer, dest_encoder, carr_indexer, carr_encoder, vec_assembler])

## 23. Transform the data ##

#Fit and transform the data
piped_data = flights_pipe.fit(model_data).transform(model_data)

## 24. Split the data ##

#Split the data into training and test sets
training, test = piped_data.randomSplit([.6, .4])

## 25. Create the model ##

#Import LogisticRegression
from pyspark.ml.classification import LogisticRegression

#Create a LogisticRegression Estimator
lr = LogisticRegression()

## 26. Create the evaluator ##

#Import the evaluation submodule
from pyspark.ml.evaluation import BinaryClassificationEvaluator

#Create a BinaryClassificationEvaluator
evaluator = BinaryClassificationEvaluator(metricName="areaUnderROC")

## 27. Make a grid ##

#Import the tuning submodule
from pyspark.ml.tuning import ParamGridBuilder

#Create the parameter grid
grid = ParamGridBuilder()

# Add the hyperparameter
grid = grid.addGrid(lr.regParam,np.arange(0, .1, .01))
grid = grid.addGrid(lr.elasticNetParam, [0,1]).build()

# Build the grid
grid = grid.build()

## 28. Make the validator ##

#Import the CrossValidator
from pyspark.ml.tuning import CrossValidator

#Create the CrossValidator
cv = CrossValidator(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator)

## 29. Fit the model(s) ##

#Call lr.fit()
best_lr = lr.fit(training)

#Print best_lr
print(best_lr)

## 30. Evaluate the model ##

#Use the model to predict the test set
test_results = best_lr.transform(test)

#Evaluate the predictions
print(evaluator.evaluate(test_results))