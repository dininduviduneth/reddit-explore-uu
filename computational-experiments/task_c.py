from pyspark.sql import SparkSession
import time
spark_session = SparkSession.builder\
        .master("spark://localhost:7077") \
        .appName("task_c")\
        .getOrCreate()

#Start counting time
start = time.time()

#Insert analysis code here:



#Stop counting time
end = time.time()

#Measure total time to run analysis
result = end-start

#Print result
print(result)

#Kill spark session
spark_session.stop()
