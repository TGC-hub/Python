from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MovieData").getOrCreate()

# Đường dẫn đến tệp CSV
csv_phim_thongtin_file = "phim.thongtin.csv"

# Đọc dữ liệu từ tệp CSV vào DataFrame
df1 = spark.read.csv(csv_phim_thongtin_file, header=True, sep = "\t", inferSchema=True)

df1.createOrReplaceTempView("ThongTin")
spark.sql("select genres, count(*) as total from ThongTin group by genres order by count(*) desc").show()

spark.stop()
