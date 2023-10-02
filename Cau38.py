from pyspark.sql import SparkSession

spark = SparkSession.builder\
                    .appName("11.8")\
                    .getOrCreate()

df1 = spark.read.csv("phim.thongtin.csv", header=True, sep="\t", inferSchema=True)
df2 = spark.read.csv("phim.binhchon.csv", header=True, sep="\t", inferSchema=True)

df1.createOrReplaceTempView("ThongTin")
df2.createOrReplaceTempView("BinhChon")


vote_table = spark.sql("select bc.tconst, bc.averageRating, bc.numVotes, tt.genres, tt.primaryTitle  \
            from ThongTin as tt, BinhChon as bc \
            where tt.tconst = bc.tconst \
          ") 

df = vote_table.createOrReplaceTempView("VoteTable")

result = spark.sql("select genres, primaryTitle, averageRating, numVotes    \
                        from (  \
                            select genres, primaryTitle, averageRating, numVotes, \
                                row_number() over (partition by genres order by averageRating desc) as rank  \
                            from VoteTable  \
                        )   \
                    where rank = 1  \
                    order by numVotes desc\
                    ")

result.show()
print(result.count())
