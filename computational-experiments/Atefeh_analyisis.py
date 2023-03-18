from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from operator import add
import json
from time import time
spark_session = SparkSession\
        .builder\
        .master("spark://localhost:7077") \
        .appName("AtefehAramian")\
        .config("spark.cores.max", 4)\
        .getOrCreate()

spark_context = spark_session.sparkContext
#Start counting time
start = time.time()

#Insert analysis code here:
comments_rdd = spark_context.textFile("hdfs://130.238.28.245:9000/RC_2011-08", use_unicode=True)
edited_id = comments_rdd.map(lambda line: json.loads(line)).map(lambda x: (x['author'], x['edited']))
list_of_edited_comand_author = edited_id.filter(lambda x: x[1] == True).map(lambda x: x[0])
list_of_edited_comand_author.take(5)
#Stop counting time
end = time.time()

#Measure total time to run analysis
result = end-start

#Print result
print(result)

#Kill spark session
spark_session.stop()
