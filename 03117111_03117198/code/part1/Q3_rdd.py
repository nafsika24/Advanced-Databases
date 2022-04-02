from pyspark.sql import SparkSession
import csv
from io import StringIO
import time

begin = time.time()

spark = SparkSession.builder.appName("Q3_rdd").getOrCreate()

sc = spark.sparkContext

def data_split(f):
        return list(csv.reader(StringIO(f), delimiter=','))[0]


movies_per_genre = sc.textFile("hdfs://master:9000/files/movie_genres.csv"). \
                        map(lambda x: (data_split(x)[1],1)). \
                        reduceByKey(lambda x, y: x + y). \
                        sortByKey()

movies= sc.textFile("hdfs://master:9000/files/movie_genres.csv"). \
                map(lambda x: (data_split(x)[0] , data_split(x)[1]))

avg_movies = sc.textFile("hdfs://master:9000/files/ratings.csv"). \
        map(lambda x: (data_split(x)[1], (float(x.split(",")[2]), 1))). \
        reduceByKey(lambda x, y : (x[0]+y[0], x[1]+y[1])). \
        map(lambda x: (x[0], x[1][0]/x[1][1]))

res = movies.join(avg_movies)

rdd = res. \
        map(lambda x: (x[1][0], (x[1][1],1))). \
        reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])). \
        map(lambda x: (x[0],(x[1][0]/x[1][1], x[1][1]))). \
        sortByKey()

for i in rdd.collect():
        print(i)

end = time.time()
print("Q3 rdd finished in " + str(end - begin) + " " + "seconds" )

