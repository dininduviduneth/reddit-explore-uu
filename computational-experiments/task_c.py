
from pyspark.sql import SparkSession
from pyspark.sql.functions import length, corr
import time


spark_session = SparkSession.builder\
        .master("spark://localhost:7077") \
        .appName("task_c")\
        .getOrCreate()

#Start counting time
start = time.time()

#Insert analysis code here:
comments_json = spark_session.read.format('json').load("hdfs://130.238.28.245:9000/RC_2011-08")


# ## Filtering dataset with those reddit posts that have score greater than 100

# Filter the DataFrame to only include comments with a score greater than 100
df_filtered_10 = comments_json.filter(comments_json.score > 10)
df_filtered_100 = comments_json.filter(comments_json.score > 100)
df_filtered_250 = comments_json.filter(comments_json.score > 250)
df_filtered_500 = comments_json.filter(comments_json.score > 500)


# ## Getting the length of each reddit post

# Create a new column for the length of the comment
df_with_length_10 = df_filtered_10.withColumn("comment_length", length(df_filtered_10.body))
df_with_length_100 = df_filtered_100.withColumn("comment_length", length(df_filtered_100.body))
df_with_length_250 = df_filtered_250.withColumn("comment_length", length(df_filtered_250.body))
df_with_length_500 = df_filtered_500.withColumn("comment_length", length(df_filtered_500.body))


# Calcuating correlation

# Calculate the correlation between comment length and score
correlation_10 = df_with_length_10.select(corr("comment_length", "score")).collect()[0][0]
correlation_100 = df_with_length_100.select(corr("comment_length", "score")).collect()[0][0]
correlation_250 = df_with_length_250.select(corr("comment_length", "score")).collect()[0][0]
correlation_500 = df_with_length_500.select(corr("comment_length", "score")).collect()[0][0]


# Priniting results

# Print the correlation coefficient
print("Correlation between comment length and score(>10):", correlation_10)
print("Correlation between comment length and score(>100):", correlation_100)
print("Correlation between comment length and score(>250):", correlation_250)
print("Correlation between comment length and score(>500):", correlation_500)


#Stop counting time
end = time.time()

#Measure total time to run analysis
result = end-start

#Print result
print(result)

#Kill spark session
spark_session.stop()