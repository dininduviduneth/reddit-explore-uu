from pyspark.sql import SparkSession
import time
spark_session = SparkSession.builder\
        .master("spark://localhost:7077") \
        .appName("task_a")\
        .getOrCreate()

#Start counting time
start = time.time()

# Create an RDD object called rdd_en and store the textFile content in it
comments_rdd = spark_session.sparkContext.textFile("hdfs://130.238.28.27:9000/data/RC_2009-09", use_unicode=True)
# Read the first 10 comments
comments_rdd.take(5)

#Stop counting time
end = time.time()

#Measure total time to run analysis
result = end-start

#Print result
print(result)

#Kill spark session
spark_session.stop()
