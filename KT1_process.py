from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name,regexp_extract

spark = SparkSession.builder.appName("CSV Processing").getOrCreate()

df = spark.read.option("header", "true").csv("./data/*.csv")

df_with_user_id = df.withColumn(
    "user_id",
    regexp_extract(input_file_name(), r"([^/]+)\.csv$", 1)
    )

df_with_user_id = df_with_user_id.select("user_id", "solving_id", "timestamp", "user_answer", "elapsed_time")

df_with_user_id.coalesce(1).write.option("header", "true").csv("output", mode="overwrite")

spark.stop()
