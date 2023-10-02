from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MovieData").getOrCreate()

# Đường dẫn đến tệp CSV
csv_phim_thongtin_file = "phim.thongtin.csv"
csv_phim_binhchon_file = "phim.binhchon.csv"

# Đọc dữ liệu từ tệp CSV vào DataFrame
df1 = spark.read.csv(csv_phim_thongtin_file, header=True, sep = "\t", inferSchema=True)
df2 = spark.read.csv(csv_phim_binhchon_file, header=True, sep = "\t", inferSchema=True)

df1.createOrReplaceTempView("ThongTin")
df2.createOrReplaceTempView("BinhChon")
spark.sql('select tt.originalTitle, tt.startYear, tt.endYear, bc.averageRating, bc.numVotes from ThongTin as tt, BinhChon as bc where tt.tconst = bc.tconst order by bc.averageRating desc').show(10)

spark.stop()
