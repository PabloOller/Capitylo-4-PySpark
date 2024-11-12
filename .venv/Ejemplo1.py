from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# Crear una sesión de Spark
from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("Ejemplo") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.legacy.createHiveTableByDefault", "false") \
    .config("spark.sql.warehouse.dir", "file:///C:/Users/pablo.oller/PycharmProjects/Capitulo4/spark-warehouse") \
    .config("javax.jdo.option.ConnectionURL", "jdbc:derby:C:/spark-metastore/metastore_db;create=true") \
    .enableHiveSupport() \
    .getOrCreate()






csv_file = "C:/Users/pablo.oller/PycharmProjects/Capitulo4/.venv/data/departuredelays.csv"
# Schema as defined in the preceding example
schema="date STRING, delay INT, distance INT, origin STRING, destination STRING"
flights_df = spark.read.csv(csv_file, schema=schema, header = True)
#flights_df.write.mode("overwrite").saveAsTable("managed_us_delay_flights_tbl")

flights_df.show(10)

#Creacion de una Vista temporal
df = (spark.read.format("csv")
 .option("inferSchema", "true")
 .option("header", "true")
 .load(csv_file))
df.createOrReplaceTempView("us_delay_flights_tbl")

'''
# In Python
df_sfo = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'SFO'")
df_jfk = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'JFK'")
# Create a temporary and global temporary view
df_sfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")
df_jfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")

# Vista de la tabla temporal
spark.read.table("us_origin_airport_JFK_tmp_view")
spark.sql("SELECT * FROM us_origin_airport_JFK_tmp_view")


(flights_df.select("distance", "origin", "destination")
 .where(col("distance") > 1000)
 .orderBy(desc("distance"))).show(10)


#Creacion de una BBDD
spark.sql("CREATE DATABASE IF NOT EXISTS learn_spark_db")
spark.sql("USE learn_spark_db")

#Creacion de sta tabla en la BBDD
spark.sql("""
CREATE TABLE IF NOT EXISTS managed_us_delay_flights_tbl (
    date STRING,
    delay INT,
    distance INT,
    origin STRING,
    destination STRING
)
USING hive
""")
'''
spark.catalog.listDatabases()
spark.catalog.listTables()
spark.catalog.listColumns("us_delay_flights_tbl")




# Detener la sesión de Spark al finalizar
spark.stop()