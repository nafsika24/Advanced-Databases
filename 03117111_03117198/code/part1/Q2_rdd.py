from pyspark.sql import SparkSession
import csv
from io import StringIO
import time

spark = SparkSession.builder.appName("Q2_rdd").getOrCreate()

sc = spark.sparkContext

def data_split(f):
        return list(csv.reader(StringIO(f), delimiter=','))[0]

begin = time.time()
total = sc.textFile("hdfs://master:9000/files/ratings.csv"). \
        map(lambda x: data_split(x)[0]).distinct().count()

rdd = sc.textFile("hdfs://master:9000/files/ratings.csv"). \
        map(lambda x: (data_split(x)[0], (float(x.split(",")[2]), 1))). \
        reduceByKey(lambda x, y : (x[0]+y[0], x[1]+y[1])). \
        map(lambda x: (x[0], x[1][0]/x[1][1])). \
        filter(lambda x: x[1]>3).count()

per = 100*rdd/total

print('Percentage = ',per,'%')

end = time.time()

print('Q2 Finished in ' + str(end-begin) + ' seconds')
~                                                          
