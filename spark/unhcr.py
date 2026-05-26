from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    sum,
    lag,
    round
)
from pyspark.sql.window import Window
import time

# =========================================
# Spark session
# =========================================
spark = SparkSession.builder \
    .appName("UNHCR Full Pipeline") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =========================================
# HDFS paths
# =========================================
POPULATION_INPUT = "hdfs:///acled/unhcr_population.csv"
DEMOGRAPHICS_INPUT = "hdfs:///acled/unhcr_demographics.csv"

STAGE1_POP_OUTPUT = "hdfs:///pyoutput/unhcr/stage1population"
STAGE1_DEMO_OUTPUT = "hdfs:///pyoutput/unhcr/stage1demographics"
STAGE2_OUTPUT = "hdfs:///pyoutput/unhcr/stage2"

# =========================================
# STAGE 1 - POPULATION
# =========================================
print("=========================================")
print("START STAGE 1 - POPULATION")
print("=========================================")

start_stage1_pop = time.time()

# Load CSV
population_df = spark.read.csv(
    POPULATION_INPUT,
    header=True,
    inferSchema=False
)

# Select needed columns
population_df = population_df.select(
    col(population_df.columns[0]).alias("year"),
    col(population_df.columns[1]).alias("coo_id"),
    col(population_df.columns[9]).alias("refugees"),
    col(population_df.columns[12]).alias("idps")
)

# Clean data
population_df = population_df.withColumn(
    "refugees",
    when(
        (col("refugees") == "-") |
        (col("refugees") == "") |
        col("refugees").isNull(),
        0
    ).otherwise(col("refugees")).cast("int")
)

population_df = population_df.withColumn(
    "idps",
    when(
        (col("idps") == "-") |
        (col("idps") == "") |
        col("idps").isNull(),
        0
    ).otherwise(col("idps")).cast("int")
)

population_df = population_df.withColumn(
    "year",
    col("year").cast("int")
)

# Aggregation
agg_pop_df = population_df.groupBy("coo_id", "year").agg(
    sum("refugees").alias("refugees"),
    sum("idps").alias("idps")
)

# Window for previous year
windowSpec = Window.partitionBy("coo_id").orderBy("year")

stage1_population = agg_pop_df.withColumn(
    "prevRefugees",
    lag("refugees", 1, 0).over(windowSpec)
).withColumn(
    "prevIdps",
    lag("idps", 1, 0).over(windowSpec)
)

# Final column order
stage1_population = stage1_population.select(
    "year",
    "coo_id",
    "refugees",
    "idps",
    "prevRefugees",
    "prevIdps"
)

# Force execution for timing
stage1_population.cache()
stage1_population.count()

# Save result
stage1_population.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(STAGE1_POP_OUTPUT)

end_stage1_pop = time.time()

print("STAGE 1 POPULATION SUCCESS")
print(f"TIME: {(end_stage1_pop - start_stage1_pop):.2f} seconds")


# =========================================
# STAGE 1 - DEMOGRAPHICS
# =========================================
print("=========================================")
print("START STAGE 1 - DEMOGRAPHICS")
print("=========================================")

start_stage1_demo = time.time()

# Load CSV
demo_df = spark.read.csv(
    DEMOGRAPHICS_INPUT,
    header=True,
    inferSchema=False
)

# Select needed columns
demo_df = demo_df.select(
    col(demo_df.columns[0]).alias("year"),
    col(demo_df.columns[1]).alias("coo_id"),
    col(demo_df.columns[23]).alias("total"),
    col(demo_df.columns[19]).alias("m_18_59")
)

# Clean data
demo_df = demo_df.withColumn(
    "total",
    when(
        (col("total") == "-") |
        (col("total") == "") |
        col("total").isNull(),
        0
    ).otherwise(col("total")).cast("int")
)

demo_df = demo_df.withColumn(
    "m_18_59",
    when(
        (col("m_18_59") == "-") |
        (col("m_18_59") == "") |
        col("m_18_59").isNull(),
        0
    ).otherwise(col("m_18_59")).cast("int")
)

demo_df = demo_df.withColumn(
    "year",
    col("year").cast("int")
)

# Aggregation
stage1_demographics = demo_df.groupBy("coo_id", "year").agg(
    sum("total").alias("total"),
    sum("m_18_59").alias("m_18_59")
)

# Final order
stage1_demographics = stage1_demographics.select(
    "year",
    "coo_id",
    "total",
    "m_18_59"
)

# Force execution for timing
stage1_demographics.cache()
stage1_demographics.count()

# Save result
stage1_demographics.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(STAGE1_DEMO_OUTPUT)

end_stage1_demo = time.time()

print("STAGE 1 DEMOGRAPHICS SUCCESS")
print(f"TIME: {(end_stage1_demo - start_stage1_demo):.2f} seconds")


# =========================================
# STAGE 2 - JOIN + METRICS
# =========================================
print("=========================================")
print("START STAGE 2 - JOIN")
print("=========================================")

start_stage2 = time.time()

# Read saved stage1 outputs
population_stage1 = spark.read.csv(
    STAGE1_POP_OUTPUT,
    header=True,
    inferSchema=True
)

demographics_stage1 = spark.read.csv(
    STAGE1_DEMO_OUTPUT,
    header=True,
    inferSchema=True
)

# Join
joined_df = population_stage1.join(
    demographics_stage1,
    on=["year", "coo_id"],
    how="inner"
)

# Metrics
stage2_df = joined_df.withColumn(
    "fit_for_duty",
    when(
        col("total") == 0,
        0
    ).otherwise(
        round((col("m_18_59") / col("total")) * 100, 2)
    )
).withColumn(
    "percent_diff_refugees",
    when(
        col("prevRefugees") == 0,
        0
    ).otherwise(
        round(
            ((col("refugees") - col("prevRefugees")) / col("prevRefugees")) * 100,
            2
        )
    )
).withColumn(
    "percent_diff_idps",
    when(
        col("prevIdps") == 0,
        0
    ).otherwise(
        round(
            ((col("idps") - col("prevIdps")) / col("prevIdps")) * 100,
            2
        )
    )
)

# Final order
stage2_df = stage2_df.select(
    "year",
    "coo_id",
    "refugees",
    "idps",
    "total",
    "m_18_59",
    "fit_for_duty",
    "percent_diff_refugees",
    "percent_diff_idps"
)

# Force execution for timing
stage2_df.cache()
stage2_df.count()

# Show sample
stage2_df.show(50, truncate=False)

# Save final result
stage2_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(STAGE2_OUTPUT)

end_stage2 = time.time()

print("STAGE 2 SUCCESS")
print(f"TIME: {(end_stage2 - start_stage2):.2f} seconds")


# =========================================
# TOTAL TIME
# =========================================
total_time = (
    (end_stage1_pop - start_stage1_pop)
    + (end_stage1_demo - start_stage1_demo)
    + (end_stage2 - start_stage2)
)

print("=========================================")
print("PIPELINE FINISHED")
print("=========================================")

print(f"STAGE1 POPULATION: {(end_stage1_pop - start_stage1_pop):.2f} sec")
print(f"STAGE1 DEMOGRAPHICS: {(end_stage1_demo - start_stage1_demo):.2f} sec")
print(f"STAGE2 JOIN: {(end_stage2 - start_stage2):.2f} sec")
print(f"TOTAL TIME: {total_time:.2f} sec")

# =========================================
# Stop Spark
# =========================================
spark.stop()