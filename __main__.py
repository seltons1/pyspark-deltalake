import pyspark
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from delta import *
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.extraClassPath", "/home/elton/Documentos/spark-3.4.3-bin-hadoop3/jars/ojdbc11.jar") \
    .config("spark.sql.session.timeZone", "America/Sao_Paulo") \
    .config("spark.sql.files.maxRecordsPerFile", 100000)

spark = configure_spark_with_delta_pip(builder) \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.debug.maxToStringFields", 1000).getOrCreate()

# Global suppot variables.
RAW_PATH = "raw/"
SILVER_PATH = "silver/"

# Defining schema.
schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True)
])

# Reading file.csv from raw.
df = spark.read.csv(RAW_PATH+'file.csv', header=True, inferSchema=True, schema=schema)

# Full load from file.csv.
df.write.format("delta").mode("overwrite").save(f"""{SILVER_PATH}person""")

# Reading delta table.
df_old = DeltaTable.forPath(spark, f"""{SILVER_PATH}person""")

# Merge new information/updates from the .csv and delta table.
df_old.alias("oldData") \
  .merge(
    df.alias("newData"),
    "oldData.id = newData.id") \
  .whenMatchedUpdate(set = { 
      "id": col("newData.id") ,
      "name": col("newData.name") 
      }) \
  .whenNotMatchedInsert(values = { 
      "id": col("newData.id") ,
      "name": col("newData.name")
      }
      ) \
  .whenNotMatchedBySourceDelete() \
  .execute()

df_old.toDF().show()
