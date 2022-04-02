from pyspark.sql import SparkSession
from io import StringIO
import csv
import time

begin = time.time()

spark = SparkSession.builder.appName("Q4_rdd").getOrCreate()

sc = spark.sparkContext

def data_split(f):
        return list(csv.reader(StringIO(f), delimiter=','))[0]

def timeline(x):
        if x >= 2000 and x <= 2004:
                return('2000-2004')
        elif x >= 2005 and x <= 2009:
                return('2005-2009')
        elif x >= 2010 and x <= 2014:
                return('2010-2014')
        elif x >= 2015 and x <= 2019:
                return('2015-2019')


# remove movies with no description and out of date range
final_movies = \
        sc.textFile("hdfs://master:9000/files/movies.csv"). \
        filter(lambda x : data_split(x)[3][0:4] != '' and  data_split(x)[2] != '' and int(data_split(x)[3][0:4]) <= 2019 and int(data_split(x)[3][0:4]) >= 2000)

res1 = final_movies.map(lambda x : (data_split(x)[0], (timeline(int(data_split(x)[3][0:4])), len(data_split(x)[2].split(" ")))))



genres_drama_expept = \
        sc.textFile("hdfs://master:9000/files/movie_genres.csv"). \
        map(lambda x : (x.split(",")[0],x.split(",")[1])). \
        filter(lambda x : x[1] == 'Drama')

res = genres_drama_expept.join(res1). \
        map(lambda x : (x[1][1][0], ((x[1][1][1], 1)))). \
        reduceByKey(lambda x, y : (x[0]+y[0], x[1]+y[1]) ). \
        sortByKey(). \
        map(lambda x : (x[0], x[1][0] / x[1][1]))


for i in res.collect():
        print(i)


end = time.time()
print("Q5 rdd finished in " + str(end - begin) + " seconds")

