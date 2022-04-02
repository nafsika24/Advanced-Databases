from pyspark.sql import SparkSession
import csv
from io import StringIO
import time

begin = time.time()

spark = SparkSession.builder.appName("Q5_rdd").getOrCreate()

sc = spark.sparkContext

def data_split(f):
        return list(csv.reader(StringIO(f), delimiter=','))[0]

ratings = sc.textFile("hdfs://master:9000/files/ratings.csv"). \
                        map(lambda x: (data_split(x)[1], data_split(x)[0]))

movie_genres = sc.textFile("hdfs://master:9000/files/movie_genres.csv"). \
                map(lambda x: (data_split(x)[0] , data_split(x)[1]))

# result popularity = ( movieid, (movie_name, popularity)) 
popularity = \
        sc.textFile("hdfs://master:9000/files/movies.csv"). \
        map(lambda x : (data_split(x)[0], (data_split(x)[1] , float(data_split(x)[7]))))



movies2 = sc.textFile("hdfs://master:9000/files/ratings.csv"). \
                                map(lambda x : (x.split(",")[1], (x.split(",")[0], float(x.split(",")[2]))))


# result res1 = (userid, (movieid, movie_name,popularity,rating))       
res1 = movies2.join(popularity). \
        map(lambda x : (x[0], (x[1][0][0], x[1][1][0], x[1][0][1], x[1][1][1]))). \
        join(movie_genres). \
        map(lambda x : ((x[1][1], x[1][0][0]), (x[1][0][1], x[1][0][2], x[1][0][3])))

# users favourite movie
fav = res1. \
        reduceByKey(lambda x, y: x if x[1] > y[1] or (x[1] == y[1] and x[2] > y[2]) else y)

# users worst movie
worst = res1. \
        reduceByKey(lambda x, y: x if x[1] < y[1] or (x[1] == y[1] and x[2] > y[2]) else y)

# result of ratings_cat_joined = ((genre,userid),number of ratings))
half = movie_genres.join(ratings). \
        map(lambda x : (x[1] , 1)). \
        reduceByKey(lambda x, y: x+y). \
        map(lambda x : (x[0][0], (x[0][1], x[1]))). \
        reduceByKey(lambda x, y: x if x[1]>y[1] else y). \
                                                                
                                                                
 res12 = \
        fav.join(worst). \
        map(lambda x : (x[0], (x[1][0][0], x[1][0][1], x[1][1][0], x[1][1][1]))). \
        join(half). \
        map(lambda x : (x[0][0], x[0][1], x[1][1], x[1][0][0], x[1][0][1], x[1][0][2], x[1][0][3])). \
        sortBy(lambda x : x[0], ascending = True)

for i in res12.collect():
        print(i)

print(" ====================================================================")

end = time.time()
print("Q5 rdd finished in " + str(end - begin) + " seconds")


                                                               
