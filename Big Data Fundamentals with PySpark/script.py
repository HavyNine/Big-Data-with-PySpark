### Introduction to Big Data analysis with Spark ###

## 1. Understanding SparkContext

# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Create spark_session

spark = SparkSession.builder.getOrCreate()

# Print spark_session
print(spark)

# Print the version of spark
print(spark.version)

# Print the Python version of spark
print(spark.sparkContext.pythonVer)

# Print the master of spark
print(spark.sparkContext.master)

## 2. Interactive Use of PySpark

# Create a Python list of numbers from 1 to 100
numb = range(1, 100)

# Load the list into PySpark
spark_data = spark.sparkContext.parallelize(numb)

## 3. Loading Data in PySpark Shell

# Load a local file into PySpark shell
lines = spark.sparkContext.textFile('file:///home/repl/exemple_read.csv')


### Programming in PySpark RDD’s ###

## 1. RDDs from Parallelized collections

# Create an RDD from a list of words
RDD = spark.sparkContext.parallelize(["Spark", "is", "a", "framework", "for", "Big Data processing"])

# Print out the type of the created object
print("The type of RDD is", type(RDD))

## 2. RDDs from External Datasets

# Print the file_path
print("The file_path is", file_path)

# Create a fileRDD from file_path
fileRDD = spark.sparkContext.textFile(file_path)

# Check the type of fileRDD
print("The file type of fileRDD is", type(fileRDD))

## 3. Partitions in your data

# Check the number of partitions in fileRDD
print("Number of partitions in fileRDD is", fileRDD.getNumPartitions())

# Create a fileRDD_part from file_path with 5 partitions
fileRDD_part = spark.sparkContext.textFile(file_path, minPartitions=5)

# Check the number of partitions in fileRDD_part
print("Number of partitions in fileRDD_part is", fileRDD_part.getNumPartitions())


## 4. Map and Collect

# Create map() transformation to cube numbers
cubedRDD = numbRDD.map(lambda x: x**3)

# Collect the results
numbers_all = cubedRDD.collect()

# Print the numbers from numbers_all
for numb in numbers_all:
    print(numb)

## 5. Filter and Count

# Filter the fileRDD to select lines with Spark keyword
fileRDD_filter = fileRDD.filter(lambda line: 'Spark' in line)

# How many lines are there in fileRDD?
print("The total number of lines with the keyword Spark is", fileRDD_filter.count())

# Print the first four lines of fileRDD
for line in fileRDD_filter.take(4):
    print(line)

## 6. ReduceBykey and Collect

# Create PairRDD Rdd with key value pairs
Rdd = spark.sparkContext.parallelize([(1, 2), (3, 4), (3, 6), (4, 5)])

# Apply reduceByKey() operation on Rdd
Rdd_Reduced = Rdd.reduceByKey(lambda x, y: x + y)

# Iterate over the result and print the output
for num in Rdd_Reduced.collect():
    print("Key {} has {} Counts".format(num[0], num[1]))

## 7. SortByKey and Collect

# Sort the reduced RDD with the key by descending order
Rdd_Reduced_Sort = Rdd_Reduced.sortByKey(ascending=False)

# Iterate over the result and print the output
for num in Rdd_Reduced_Sort.collect():
    print("Key {} has {} Counts".format(num[0], num[1]))

## 8. CountingBykeys

# Transform the rdd with countByKey()
total = Rdd.countByKey()

# What is the type of total?
print("The type of total is", type(total))

# Iterate over the total and print the output
for k, v in total.items():
    print("key", k, "has", v, "counts")

## 9. Create a base RDD and transform it

# Create an RDD from a list of words
baseRDD = sc.textFile(file_path)

# Split the lines of baseRDD into words
splitRDD = baseRDD.flatMap(lambda x: x.split())

# Count the total number of words
print("Total number of words in splitRDD:", splitRDD.count())

## 10. Remove stop words and reduce the dataset

# Convert the words in lower case and remove stop words from stop_words
splitRDD_no_stop = splitRDD.filter(lambda x: x.lower() not in stop_words)

# Create a tuple of the word and 1
splitRDD_no_stop_words = splitRDD_no_stop.map(lambda w: (w, 1))

# Count of the number of occurences of each word
resultRDD = splitRDD_no_stop_words.reduceByKey(lambda x, y: x + y)

## 11. Print word frequencies

# Display the first 10 words and their frequencies
for word in resultRDD.take(10):
    print(word)

# Swap the keys and values
resultRDD_swap = resultRDD.map(lambda x: (x[1], x[0]))

# Sort the keys in descending order
resultRDD_swap_sort = resultRDD_swap.sortByKey(ascending=False)

# Show the top 10 most frequent words and their frequencies
for word in resultRDD_swap_sort.take(10):
    print("{} has {} counts".format(word[1], word[0]))

## 12. RDD to DataFrame

# Create an RDD from the list
rdd = sc.parallelize(sample_list)

# Create a PySpark DataFrame
names_df = spark.createDataFrame(rdd, schema=['name', 'age'])

# Check the type of people_df
print("The type of names_df is", type(names_df))

## 13. Loading CSV into DataFrame

# Load the Dataframe
people_df = spark.read.csv(file_path, header=True, inferSchema=True)

# Check the type of people_df
print("The type of people_df is", type(people_df))

## 14. Inspecting data in PySpark DataFrame

# Print the first 10 observations
people_df.show(10)

# Count the number of rows
print("There are {} rows in the people_df DataFrame.".format(people_df.count()))

# Count the number of columns and their names
print("There are {} columns in the people_df DataFrame and their names are {}".format(len(people_df.columns), people_df.columns))

## 15. PySpark DataFrame subsetting and cleaning

# Select name, sex and date of birth columns
people_df_sub = people_df.select('name', 'sex', 'date of birth')

# Print the first 10 observations from people_df_sub
people_df_sub.show(10)

# Remove duplicate entries from people_df_sub
people_df_sub_nodup = people_df_sub.dropDuplicates()

# Count the number of rows
print("There were {} rows before removing duplicates, and {} rows after removing duplicates".format(people_df_sub.count(), people_df_sub_nodup.count()))

## 16. Filtering your DataFrame

# Filter people_df to select females 
people_df_female = people_df.filter(people_df.sex == "female")

# Filter people_df to select males
people_df_male = people_df.filter(people_df.sex == "male")

# Count the number of rows 
print("There are {} rows in the people_df_female DataFrame and {} rows in the people_df_male DataFrame".format(people_df_female.count(), people_df_male.count()))

## 17. Running SQL Queries Programmatically

# Register the DataFrame as a table
people_df.createOrReplaceTempView("people")

# Construct a query to select the names of the people from the 'people' table
query = '''SELECT name FROM people'''

# Assign the result of Spark's query to people_df_names
people_df_names = spark.sql(query)

# Print the top 10 names of the people
people_df_names.show(10)

## 18. SQL queries for filtering Table

# Filter the people to select female sex
people_female_df = spark.sql('SELECT * FROM people WHERE sex=="female"')

# Filter the people table DataFrame to select male sex
people_male_df = spark.sql('SELECT * FROM people WHERE sex=="male"')

# Count the number of rows in both DataFrames
print("There are {} rows in the people_female_df and {} rows in the people_male_df DataFrames".format(people_female_df.count(), people_male_df.count()))

## 19. PySpark DataFrame visualization

# Check the column names of names_df
print("The column names of names_df are", names_df.columns)

# Convert to Pandas DataFrame
df_pandas = names_df.toPandas()

# Create a horizontal bar plot
df_pandas.plot(kind='barh', x='name', y='age', colormap='winter_r')
plt.show()


## 20. Part 1: Create a DataFrame from CSV file

# Load the Dataframe
fifa_df = spark.read.csv(file_path, header=True, inferSchema=True)

# Check the schema of columns
fifa_df.printSchema()

## 21. Part 2: SQL Queries on DataFrame

# Create a temporary view of fifa_df
fifa_df.createOrReplaceTempView("fifa_df_table")

# Construct the "query"
query = '''SELECT * FROM fifa_df_table WHERE Nationality = "Germany"'''

# Apply the SQL "query"
fifa_df_germany = spark.sql(query)

# Generate basic statistics
fifa_df_germany.describe().show()

## 22. Part 3: Data visualization

# Convert fifa_df to fifa_df_germany_pandas DataFrame
fifa_df_germany_pandas = fifa_df_germany.toPandas()

# Plot the 'Age' density of Germany Players
fifa_df_germany_pandas.plot(kind='density',  figsize=(14, 10), linewidth=3)
plt.xlabel('Age')
plt.show()


### PySpark MLlib algorithms ###

## 1. Import the library for ALS

# Import the library for ALS
from pyspark.mllib.recommendation import ALS

# Import the library for Logistic Regression
from pyspark.mllib.classification import LogisticRegressionWithLBFGS

# Import the library for Kmeans
from pyspark.mllib.clustering import KMeans

## 2. Loading Movie Lens dataset into RDDs

# Load the data into RDD
data = sc.textFile(file_path)

# Split the RDD
ratings = data.map(lambda l: l.split(','))

# Transform the ratings RDD
ratings_final = ratings.map(lambda line: Rating(int(line[0]), int(line[1]), float(line[2])))

# Split the data into training and test
training_data, test_data = ratings_final.randomSplit([0.8, 0.2])

## 3. Model training and predictions

# Create the ALS model on the training data
model = ALS.train(training_data, rank=10, iterations=10)

# Drop the ratings column
testdata_no_rating = test_data.map(lambda p: (p[0], p[1]))

# Predict the model
predictions = model.predictAll(testdata_no_rating)

# Return the first 2 rows of the RDD
predictions.take(2)

## 4. Model evaluation using MSE

# Prepare ratings data
rates = ratings_final.map(lambda r: ((r[0], r[1]), r[2]))

# Prepare predictions data
preds = predictions.map(lambda r: ((r[0], r[1]), r[2]))

# Join the ratings data with predictions data
rates_and_preds = rates.join(preds)

# Calculate and print MSE
MSE = rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
print("Mean Squared Error of the model for the test data = {:.2f}".format(MSE))

## 5. Loading spam and non-spam data

# Load the datasets into RDDs
spam_rdd = sc.textFile(file_path_spam)

non_spam_rdd = sc.textFile(file_path_non_spam)

# Split the email messages into words
spam_words = spam_rdd.flatMap(lambda email: email.split(' '))
non_spam_words = non_spam_rdd.flatMap(lambda email: email.split(' '))

# Print the first element in the split RDD
print("The first element in spam_words is", spam_words.first())
print("The first element in non_spam_words is", non_spam_words.first())

## 6. Feature hashing and LabelPoint

# Create a HashingTf instance with 200 features
tf = HashingTF(numFeatures=200)

# Map each word to one feature
spam_features = tf.transform(spam_words)
non_spam_features = tf.transform(non_spam_words)

# Label the features: 1 for spam, 0 for non-spam
spam_samples = spam_features.map(lambda features:LabeledPoint(1, features))
non_spam_samples = non_spam_features.map(lambda features:LabeledPoint(0, features))

# Combine the two datasets
samples = spam_samples.join(non_spam_samples)

## 7. Logistic Regression model training

# Split the data into training and testing
train_samples,test_samples = samples.randomSplit([0.8, 0.2])

# Train the model
model = LogisticRegressionWithLBFGS.train(train_samples)

# Create a prediction label from the test data
predictions = model.predict(test_samples.map(lambda x: x.features))

# Combine original labels with the predicted labels
labels_and_preds = test_samples.map(lambda x: x.label).zip(predictions)

# Check the accuracy of the model on the test data
accuracy = labels_and_preds.filter(lambda x: x[0] == x[1]).count() / float(test_samples.count())
print("Model accuracy : {:.2f}".format(accuracy))


### clustering ###

## 8. Loading and parsing the 5000 points data
clusterRDD = sc.textFile(file_path)

# Split the RDD based on tab
rdd_split = clusterRDD.map(lambda x: x.split("\t"))

# Transform the split RDD by creating a list of integers
rdd_split_int = rdd_split.map(lambda x: [int(x[0]), int(x[1])])

# Count the number of rows in RDD
print("There are {} rows in the rdd_split_int dataset".format(rdd_split_int.count()))