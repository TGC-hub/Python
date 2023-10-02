from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MovieData").getOrCreate()

# Đường dẫn đến tệp CSV
csv_phim_thongtin_file = "phim.thongtin.csv"

# Đọc dữ liệu từ tệp CSV vào DataFrame
df1 = spark.read.csv(csv_phim_thongtin_file, header=True, sep = "\t", inferSchema=True)

#filtered_df = df1.filter(df1["runtimeMinutes"] != "\\N")
df1.createOrReplaceTempView("ThongTin")
spark.sql('select avg(runtimeMinutes) from ThongTin where primaryTitle like "%Star Wars%"').show() # 3955
# dataMinutesOfFilm = spark.sql('select sum(runtimeMinutes) from ThongTin where primaryTitle like "%Star Wars%"').show()

spark.stop()
