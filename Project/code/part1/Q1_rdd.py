from pyspark.sql import SparkSession
import csv
from io import StringIO
import  time

begin = time.time()
spark = SparkSession.builder.appName("Q1_rdd").getOrCreate()

sc = spark.sparkContext

def reader(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]

rdd = sc.textFile("hdfs://master:9000/files/movies.csv"). \
                map(lambda x : reader(x)). \
                filter(lambda x : (x[3] != "" and x[5] != '0' and x[6] != '0' and int(x[3].split("-")[0]) >= 2000)). \
                map(lambda x : (x[3].split("-")[0], (x[1], (float(x[6])-float(x[5]))*100/float(x[5])))). \
                reduceByKey(lambda x, y : x if x[1] > y[1] else y). \
                sortByKey(ascending=True)

for i in rdd.collect():
        print(i)


end = time.time()
print("Q1 rdd finished in " + str(end - begin) +  " seconds")

