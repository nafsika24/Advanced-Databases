import csv
import sys
import time
from pyspark.sql import SparkSession
from io import StringIO

spark = SparkSession.builder.appName("repartition").getOrCreate()
sc = spark.sparkContext

begin = time.time()

def repart(map_values):
        targ_R = []
        targ_L = []
        for value in map_values:
                if value[0] == 'R':
                        targ_R.append(value)
                elif value[0] == 'L':
                        targ_L.append(value)
        return [(valueR, valueL) for valueR in targ_R for valueL in targ_L]


def data_split(f):
        return list(csv.reader(StringIO(f), delimiter=','))[0]

# R_Record and L_Record variables may change

R_Record = "hdfs://master:9000/files/movie_genres_100.csv"
L_Record = "hdfs://master:9000/files/ratings.csv"

keyr = 0
keyl = 1


targetR = \
        sc.textFile(R_Record). \
        map(lambda x : (data_split(x))). \
        map(lambda x : (x.pop(keyr), ('R', x)))

targetL = \
        sc.textFile(L_Record). \
        map(lambda x : (data_split(x))). \
        map(lambda x : (x.pop(keyl), ('L', x)))


union_targets = targetR.union(targetL)

res =   union_targets. \
        groupByKey(). \
        flatMapValues(lambda x : repart(x)). \
        map(lambda x : (x[0], x[1][0][1], x[1][1][1]))

for i in res.collect():
        print(i)

end = time.time()
print("Repartition join finished in " +str(end - begin) + " seconds" )



