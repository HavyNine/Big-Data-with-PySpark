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


## Manipulating DataFrames in the real world

## Improving Performance

## Improving Performance