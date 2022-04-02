from pyspark.sql import SparkSession
import csv
import time

begin = time.time()
spark = SparkSession.builder.appName("Q4_sql_parquet").getOrCreate()
movies = spark.read.parquet("hdfs://master:9000/files/movies.parquet")
movies.registerTempTable('movies')

genres = spark.read.parquet("hdfs://master:9000/files/movie_genres.parquet")
genres.registerTempTable('genres')

def discr_counter(s):
   return len(s.split(' '))
spark.udf.register("discr_counter", discr_counter)

def fiveyears(s):
  if s > 1999 and s < 2005:
    res = '2000-2004'
  elif s > 2004 and s < 2010:
    res = '2005-2009'
  elif s > 2009 and s < 2015:
    res = '2010-2014'
  elif s > 2014 and s < 2020:
    res = '2015-2019'
  else:
    res = '0'
  return res

spark.udf.register("fiveyears", fiveyears)

movies = spark.sql("select _c0, discr_counter(_c2) as counts, fiveyears(year(_c3)) as period from movies where _c2 is not null and year(_c3)> 2000 and year(_c3) < 2019 ")
movies.createOrReplaceTempView("movies")

drama = spark.sql("select period, avg(counts) " + \
                 "from genres as g "  +\
                "join movies as m on g._c0 = m._c0 " + \
                " group by period")

drama.show()

end = time.time()
print("Q4 sql parquet finished in " + str(end - begin) + " seconds" )


