from pyspark.sql import SparkSession
import csv
import time

begin = time.time()


spark = SparkSession.builder.appName("Q5_sql_csv").getOrCreate()
movies = spark.read.format('csv').options(header='false',inferSchema='true').load("hdfs://master:9000/files/movies.csv")
movies.registerTempTable('movies')
ratings = spark.read.format('csv').options(header='false',inferSchema='true').load("hdfs://master:9000/files/ratings.csv")
ratings.registerTempTable('ratings')
movie_genres = spark.read.format('csv').options(header='false',inferSchema='true').load("hdfs://master:9000/files/movie_genres.csv")
movie_genres.registerTempTable('movie_genres')

gen = spark.sql("select g._c1 as Genre, r._c0 as UserId, count(*) as Count, max(r._c2) as MaxRating, min(r._c2) as MinRating " +\
                 "from ratings as r " +\
                 "join movie_genres as g on r._c1 = g._c0 " +\
                 "group by g._c1, r._c0")

gen.registerTempTable("gen")

users = spark.sql("select Genre, UserId, Count, MaxRating, MinRating "+\
                  "from gen "+\
                  "join (select Genre as Genre2, max(Count) as Count2 " +\
                        "from gen "  +\
                        "group by Genre2) " +\
                  "on (Genre = Genre2 and Count = Count2)")

users.registerTempTable("users")

a = spark.sql("select MovieId, Genre, UserId, Reviews, Title, Rating, Popularity "+\
              "from (select m._c0 as MovieId, u.Genre as Genre, u.UserId as UserId, u.Count as Reviews, "+\
                    "m._c1 as Title, r._c2 as Rating, m._c7 as Popularity "+\
                    "from movies as m join ratings as r "+\
                    "on m._c0 = r._c1 join users as u on r._c0 = u.UserId and (r._c2 = u.MaxRating or r._c2 = u.MinRating)) "
              "join movie_genres as g on MovieId = g._c0 and Genre = g._c1")

a.registerTempTable('a')

res = spark.sql("select A.Genre, A.UserId, A.Reviews, A.Title, A.Rating, B.Title, B.Rating "+\
                "from (select a1.Genre, a1.UserId, a1.Reviews, a1.Title, a1.Rating "+\
                      "from a as a1 left outer join a as a2 on a1.Genre = a2.Genre "+\
                      "and (a1.Rating < a2.Rating or (a1.Rating = a2.Rating and a1.Popularity < a2.Popularity)) "+\
                      "where a2.Genre is null) as A "+\
                      "join "+\
                     "(select a1.Genre, a1.Title, a1.Rating "+\
                      "from a as a1 left outer join a as a2 on a1.Genre = a2.Genre "+\
                      "and (a1.Rating > a2.Rating or (a1.Rating = a2.Rating and a1.Popularity < a2.Popularity)) "+\
                      "where a2.Genre is null) as B "+\
                      "on A.Genre = B.Genre "+\
                "order by A.Genre")

res.show()
end = time.time()

print("Q5 sql csv finished in " + str(end - begin) + " seconds")



