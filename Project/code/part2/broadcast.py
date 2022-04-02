from pyspark.sql import SparkSession
from io import StringIO
import csv
from itertools import product
import time
import sys


begin = time.time()

def data_split(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]

def broad_join(tup):
    key = tup[0]
    value = tup[1]
    if key in broad_targetR.value:
        return ((key, (genre, value)) for genre in broad_targetR.value[key])
    else:
        return []


spark = SparkSession.builder.appName("broadcast").getOrCreate()
sc = spark.sparkContext

# R_Record and L_Record variables may change

R_Record = "hdfs://master:9000/files/movie_genres_100.csv"
L_Record = "hdfs://master:9000/files/ratings.csv"

keyr = 0
keyl = 1


targertR = sc.textFile(R_Record). \
    map(lambda line: data_split(line)). \
    map(lambda line: (int(line[0]), line[1])). \
    groupByKey(). \
    mapValues(list).collectAsMap()


broad_targetR = sc.broadcast(targertR)


targetL = sc.textFile(L_Record) \
    .map(lambda line: data_split(line)) \
    .map(lambda line: (int(line[1]), (int(line[0]), float(line[2]), line[3]))).flatMap(broad_join)

for i in targetL.collect():
        print(i)

end = time.time()
print("Broadcast join finished in " +str(end - begin) + " seconds" )

