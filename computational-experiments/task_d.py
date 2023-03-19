from pyspark.sql import SparkSession
import time

spark_session = SparkSession.builder\
        .master("spark://localhost:7077") \
        .appName("Dinindu_Task-B")\
        .config("spark.cores.max", 4)\
        .getOrCreate()

#Start counting time
start = time.time()

''''Insert analysis code here:''''
# A JSON dataset is pointed to by path.
comments_json = spark_session.read.format('json').load("hdfs://130.238.28.245:9000/RC_2011-08")

# Create an Spark SQL view for comments_json with the name "comments"
comments_json.createOrReplaceTempView("comments")

# To find the Top 10 authors who has the highest aggregate scores
spark_session.sql(
    "SELECT author, SUM(score) as total_score, COUNT(created_utc) as comments_count, AVG(score) as score_per_comment \
     FROM comments \
     WHERE author!= '[deleted]' \
     GROUP BY author \
     ORDER BY total_score DESC"
).show(25)

#Stop counting time
end = time.time()

#Measure total time to run analysis
result = end-start

#Print result
print(result)

#Kill spark session
spark_session.stop()

