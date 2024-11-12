from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("Ejemplo 2") \
    .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.3.4") \
    .getOrCreate()

# READING AND WRITING IN PARQUET


file = "C:/LearningSparkV2-master/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet/"
df = spark.read.format("parquet").load(file)

df.show(10)

(df.write.format("parquet")
 .mode("overwrite")
 .option("compression", "snappy")
 .save("C:/Users/pablo.oller/PycharmProjects/Capitulo4/.venv/df_parquet"))

'''(df.write
 .mode("overwrite")
 .saveAsTable("us_delay_flights_tbl"))
'''

# READING AND WRITING IN JSON

file2 = "C:/LearningSparkV2-master/databricks-datasets/learning-spark-v2/flights/summary-data/json/*"
df2 = spark.read.format("json").load(file2)
df2.show(20)

(df2.write.format("json")
 .mode("overwrite")
 #.option("compression", "snappy")
 .save("C:/Users/pablo.oller/PycharmProjects/Capitulo4/.venv/df_json"))

# READING AND WRITING IN CSV

file3 = "C:/LearningSparkV2-master/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*"
schema = "DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count INT"
df3 = (spark.read.format("csv")
 .option("header", "true")
 .schema(schema)
 .option("mode", "FAILFAST") # Exit if any errors
 .option("nullValue", "") # Replace any null data field with quotes
 .load(file3))

df3.show(10)

df3.write.format("csv").mode("overwrite").save("C:/Users/pablo.oller/PycharmProjects/Capitulo4/.venv/df_csv")

# READING AND WRITING IN AVRO

df4 = (spark.read.format("avro")
 .load("C:/LearningSparkV2-master/databricks-datasets/learning-spark-v2/flights/summary-data/avro/*"))
df4.show(truncate=False)

(df4.write
 .format("avro")
 .mode("overwrite")
 .save("C:/Users/pablo.oller/PycharmProjects/Capitulo4/.venv/df_avro"))

# READING AND WRITING IN ORC

file5 = "C:/LearningSparkV2-master/databricks-datasets/learning-spark-v2/flights/summary-data/orc/*"
df5 = spark.read.format("orc").option("path", file5).load()
df5.show(10, False)

(df5.write.format("orc")
 .mode("overwrite")
 .option("compression", "snappy")
 .save("C:/Users/pablo.oller/PycharmProjects/Capitulo4/.venv/df_orc"))

spark.stop()