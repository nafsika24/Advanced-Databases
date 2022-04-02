from pyspark.sql import SparkSession
import time

begin = time.time()
spark = SparkSession.builder.appName("Q1_rdd").getOrCreate()

movies = spark.read.parquet("hdfs://master:9000/files/movies.parquet")

movies.registerTempTable("movies")

res = spark.sql("select Year, movies._c1 as Title, Profit " + \
                "from movies, " + \
                        "(select cast(year(_c3) as int) as Year, max((_c6-_c5)*100/_c5) as Profit " + \
                        "from movies " + \
                        "where cast(year(_c3) as int) >= 2000 and not _c5=0 and not _c6=0 " + \
                        "group by 1) as Temp " + \
                "where year(movies._c3)=Temp.Year and (movies._c6-movies._c5)*100/movies._c5=Temp.Profit " + \
                "order by Year asc")

res.show()

end = time.time()
print("Q1 sql parquet finished in " + str(end - begin) +  " seconds")


