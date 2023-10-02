from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MovieData").getOrCreate()

# Đường dẫn đến tệp CSV
csv_phim_thongtin_file = "phim.thongtin.csv"
csv_phim_binhchon_file = "phim.binhchon.csv"

# Đọc dữ liệu từ tệp CSV vào DataFrame
df1 = spark.read.csv(csv_phim_thongtin_file, header=True, sep = "\t", inferSchema=True)
df2 = spark.read.csv(csv_phim_binhchon_file, header=True, sep = "\t", inferSchema=True)

#Hiển thị schema của bộ dữ liệu
print("Schema của bộ dữ liệu:")
df1.printSchema()
df2.printSchema()

# Đếm số bản ghi của bộ dữ liệu
num_records_phim_thongtin = df1.count()
num_records_phim_binhchon = df2.count()

print(f"Số bản ghi của bộ dữ liệu của thông tin: {num_records_phim_thongtin}")
print(f"Số bản ghi của bộ dữ liệu của bình chọn: {num_records_phim_binhchon}")
# Đóng SparkSession
spark.stop()
