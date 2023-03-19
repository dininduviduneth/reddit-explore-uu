from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
import json
import time

spark_session = SparkSession.builder\
        .master("spark://localhost:7077") \
        .appName("ludvig-westerholm_popularity-by-keyword")\
        .config("spark.cores.max", 4)\
        .getOrCreate()

print('Enter the word you want to search:')
search_keyword = input()

#Start counting time
start = time.time()

'''Insert analysis code here:'''
# Create json dataframe

comments_json = spark_session.read.format('json').load("hdfs://130.238.28.245:9000/RC_2011-08")

def top_scores_with_keyword(df, keyword):
    filtered_df = df.filter(df.body.contains(keyword)) #filters to only contain posts with the given keyword
    top_scores_df = filtered_df.orderBy(desc("score")).limit(10) #orders the list by score
    return top_scores_df.select("id").rdd.flatMap(lambda x: x).collect() #returns a list of id's that has the highest score given the keyword

print(top_scores_with_keyword(comments_json, search_keyword))

#Stop counting time
end = time.time()

#Measure total time to run analysis
result = end-start

#Print result
print(result)

#Kill spark session
spark_session.stop()
