import pyspark
from delta import *

builder = (
    pyspark.sql.SparkSession.builder.appName("MyApp")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

kt1_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("solving_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("user_answer", StringType(), True),
    StructField("elapsed_time", IntegerType(), True)
])

df_kt1 = spark.createDataFrame([], kt1_schema)
df_kt1.write.format("delta").saveAsTable("KT1")