from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

# Tạo một phiên Spark
spark = SparkSession.builder.appName("MaxRuntimeMovie").getOrCreate()

# Tạo một phiên SparkContext
sc = SparkContext.getOrCreate()
csv_phim_thongtin_file = "phim.thongtin.csv"

# Đọc dữ liệu từ tệp CSV vào DataFrame
movie_data_df = spark.read.csv(csv_phim_thongtin_file, header=True, sep = "\t", inferSchema=True)
movie_data_df = movie_data_df.filter(movie_data_df["startYear"] != "\\N")
movie_data_df = movie_data_df.filter(movie_data_df["runtimeMinutes"] != "\\N")
# Chuyển đổi kiểu dữ liệu của cột "startYear" thành số (integer)
movie_data_df = movie_data_df.withColumn("startYear", movie_data_df["startYear"].cast(IntegerType()))

# Tạo view tạm thời từ DataFrame
movie_data_df.createOrReplaceTempView("movies")

# Sử dụng Spark SQL để tìm phim có thời lượng nhiều nhất trong thập kỷ 2010
max_runtime_movie = spark.sql("""
    SELECT *
    FROM movies
    WHERE startYear >= 2010 AND startYear < 2020
    ORDER BY runtimeMinutes DESC
    LIMIT 1
""")

# Đường dẫn đến file CSV muốn lưu trữ
output_path = "outputDataframe.csv"

# Sử dụng phương thức write để chuyển DataFrame thành file CSV
max_runtime_movie.write.csv(output_path, header=True, mode="overwrite")