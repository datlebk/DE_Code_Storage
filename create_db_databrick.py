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

###################################################
kt3_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("action_type", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("item_id", StringType(), True),
    StructField("source", StringType(), True),
    StructField("user_answer", StringType(), True),
    StructField("platform", StringType(), True)
])

df_kt3 = spark.createDataFrame([], kt3_schema)
df_kt3.write.format("delta").saveAsTable("KT3")

###################################################
question_schema = StructType([
    StructField("question_id", StringType(), True),
    StructField("bundle_id", StringType(), True),
    StructField("explanation_id", StringType(), True),
    StructField("correct_answer", StringType(), True),
    StructField("part", StringType(), True),
    StructField("tags", StringType(), True),
    StructField("deployed_at", TimestampType(), True)
])

df_question = spark.createDataFrame([], question_schema)

df_question.write.format("delta").saveAsTable("question")
