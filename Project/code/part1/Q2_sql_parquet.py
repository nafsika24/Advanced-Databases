from pyspark.sql import SparkSession
import csv
import time

begin = time.time()
spark = SparkSession.builder.appName("Q2_sql_csv").getOrCreate()
ratings = spark.read.parquet("hdfs://master:9000/files/ratings.parquet")

ratings.registerTempTable('ratings')

# total users
total = spark.sql("select distinct _c0 from ratings")

# all averages
#avg = spark.sql("select _c0, avg(_c2) as avg_rating from ratings group by _c0 limit 5")

# users with avg > 3

res = spark.sql("select * from (select _c0 from ratings  group by _c0 having avg(_c2) > 3)")

total_users = total.count()
res_users = res.count()
per = 100 * res_users/total_users

print("Percentage = ", per, "%")

end = time.time()

print("Q2 parquet finished after" + str(end - begin) + " seconds")
~                                                                    
