from pyspark.sql import SparkSession

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("GradeMovies").getOrCreate()

# Đường dẫn đến tệp CSV
csv_phim_thongtin_file = "phim.thongtin.csv"
csv_phim_binhchon_file = "phim.binhchon.csv"

# Đọc dữ liệu từ tệp CSV vào DataFrame
df1 = spark.read.csv(csv_phim_thongtin_file, header=True, sep = "\t", inferSchema=True)
df2 = spark.read.csv(csv_phim_binhchon_file, header=True, sep = "\t", inferSchema=True)

df1.createOrReplaceTempView("ThongTin")
df2.createOrReplaceTempView("BinhChon")

# Sử dụng Spark SQL để thêm cột mới "Grade" dựa trên điểm bình chọn
query = """
SELECT *,
       CASE
           WHEN `averageRating` >= 8.5 THEN 'A'
           WHEN `averageRating` >= 7 AND `averageRating` < 8.5 THEN 'B'
           WHEN `averageRating` >= 5.5 AND `averageRating` < 7 THEN 'C'
           WHEN `averageRating` >= 4 AND `averageRating` < 5.5 THEN 'D'
           ELSE 'F'
       END AS `Grade`
FROM BinhChon
"""

df_with_grade = spark.sql(query)

# Hiển thị DataFrame với cột mới "Grade"
data_grade = df_with_grade.select("tconst", "averageRating", "numVotes", "Grade")

data_grade.show()

# Đóng SparkSession
spark.stop()
