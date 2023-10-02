from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("MovieData").getOrCreate()

csv_phim_thongtin_file = "phim.thongtin.csv"

df = spark.read.csv(csv_phim_thongtin_file, header=True, sep = "\t", inferSchema=True)

filtered_df = df.filter(df["endYear"] != "\\N")
df1 = filtered_df.withColumn("time", (col("endYear") - col("startYear")))

df1.createOrReplaceTempView("ThongTin")
spark.sql('select * from ThongTin as tta \
        where time = (select max(time) from ThongTin as ttb where tta.genres = ttb.genres group by genres)').show(10)

spark.stop()
