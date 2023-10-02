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
spark.sql('select tt.startYear, count(startYear) \
          from ThongTin as tt, BinhChon as bc \
          where tt.tconst = bc.tconst \
          and bc.numVotes > 100000 \
          group by tt.startYear\
          having tt.startYear > 2000 \
          order by tt.startYear asc').show(23)

spark.stop()
