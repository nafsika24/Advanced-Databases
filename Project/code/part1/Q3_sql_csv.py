from pyspark.sql import SparkSession
import csv
import time

begin = time.time()

spark = SparkSession.builder.appName("Q3_sql_csv").getOrCreate()
ratings = spark.read.format('csv').options(header='false',inferSchema='true').load("hdfs://master:9000/files/ratings.csv")
ratings.registerTempTable('ratings')

genres = spark.read.format('csv').options(header='false',inferSchema='true').load("hdfs://master:9000/files/movie_genres.csv")
genres.registerTempTable('genres')

res = spark.sql( \
       "select genres_all._c1 as Genre, avg(q1.avgrate) as AverageRating, count(*) as MoviesCount from ( " + \
        "select rates._c1 as MovieID,avg(rates._c2) as avgrate " + \
        "from ratings as rates " + \
        "group by 1 ) as q1 " + \
    "inner join genres as genres_all " + \
    "on genres_all._c0 = q1.MovieID " + \
    "group by genres_all._c1 ")


res.show()

end = time.time()
print("Q3 csv finished in " + str(end - begin) + " seconds" )

