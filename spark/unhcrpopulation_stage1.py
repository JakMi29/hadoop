from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    sum,
    lag
)
from pyspark.sql.window import Window

# =========================================
# Spark session
# =========================================
spark = SparkSession.builder \
    .appName("UNHCR Population") \
    .getOrCreate()

# =========================================
# Load CSV from HDFS
# =========================================
df = spark.read.csv(
    "hdfs:///acled/unhcr_population.csv",
    header=True,
    inferSchema=False
)

# =========================================
# Select needed columns
# cols[0]  -> year
# cols[1]  -> coo_id
# cols[9]  -> refugees
# cols[12] -> idps
# =========================================
population = df.select(
    col(df.columns[0]).alias("year"),
    col(df.columns[1]).alias("coo_id"),
    col(df.columns[9]).alias("refugees"),
    col(df.columns[12]).alias("idps")
)

# =========================================
# Replace "-" and null with 0
# =========================================
population = population.withColumn(
    "refugees",
    when(
        (col("refugees") == "-") |
        (col("refugees") == "") |
        col("refugees").isNull(),
        0
    ).otherwise(col("refugees")).cast("int")
)

population = population.withColumn(
    "idps",
    when(
        (col("idps") == "-") |
        (col("idps") == "") |
        col("idps").isNull(),
        0
    ).otherwise(col("idps")).cast("int")
)

population = population.withColumn(
    "year",
    col("year").cast("int")
)

# =========================================
# Aggregation
# equivalent to reducer aggregation
# =========================================
agg_df = population.groupBy("coo_id", "year").agg(
    sum("refugees").alias("refugees"),
    sum("idps").alias("idps")
)

# =========================================
# Previous year values
# equivalent to prevRefugees / prevIdps
# =========================================
windowSpec = Window.partitionBy("coo_id").orderBy("year")

result = agg_df.withColumn(
    "prevRefugees",
    lag("refugees", 1, 0).over(windowSpec)
).withColumn(
    "prevIdps",
    lag("idps", 1, 0).over(windowSpec)
)

# =========================================
# Final column order
# =========================================
final_df = result.select(
    "year",
    "coo_id",
    "refugees",
    "idps",
    "prevRefugees",
    "prevIdps"
)

# =========================================
# Show result
# =========================================
final_df.show(50, truncate=False)

# =========================================
# Save to HDFS
# =========================================
final_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("hdfs:///pyoutput/unhcr/stage1population")

spark.stop()