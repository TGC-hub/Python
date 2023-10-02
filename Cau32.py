from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MovieData").getOrCreate()

# Đường dẫn đến tệp CSV
csv_phim_thongtin_file = "phim.thongtin.csv"

# Đọc dữ liệu từ tệp CSV vào DataFrame
df1 = spark.read.csv(csv_phim_thongtin_file, header=True, sep = "\t", inferSchema=True)
df1.createOrReplaceTempView("ThongTin")
spark.sql('select * from ThongTin where originalTitle like "%Harry Potter%"').show(20)
spark.stop()
