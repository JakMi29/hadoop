from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    sum,
    round
)

# =========================================
# Spark session
# =========================================
spark = SparkSession.builder \
    .appName("UN_Comtrade Pipeline") \
    .getOrCreate()

# =========================================
# INPUT / OUTPUT
# =========================================
INPUT_DIR  = "hdfs:///data/un_comtrade/1997.csv"
OUT_STAGE1 = "hdfs:///uncomtrade/output/stage1"
OUT_STAGE2 = "hdfs:///uncomtrade/output/stage2"
OUT_STAGE3 = "hdfs:///uncomtrade/output/stage3"

# =========================================
# LOAD CSV
# =========================================
df = spark.read.csv(
    INPUT_DIR,
    header=True,
    inferSchema=False
)

# =========================================
# SELECT COLUMNS
# cols[6]  -> reporterCode
# cols[3]  -> refYear
# cols[20] -> cmdCode
# cols[42] -> primaryValue
# =========================================
data = df.select(
    col(df.columns[6]).alias("reporterCode"),
    col(df.columns[3]).alias("refYear"),
    col(df.columns[20]).alias("cmdCode"),
    col(df.columns[42]).alias("primaryValue")
)

# =========================================
# CLEAN DATA
# =========================================
data = data.filter(
    col("reporterCode").isNotNull() &
    col("refYear").isNotNull()
)

data = data.withColumn(
    "primaryValue",
    when(
        col("primaryValue").isNull() |
        (col("primaryValue") == ""),
        0
    ).otherwise(col("primaryValue")).cast("double")
)

# =========================================
# STAGE 1
# aggregation
# equivalent to Stage1Reducer
# =========================================
stage1 = data.groupBy(
    "reporterCode",
    "refYear"
).agg(

    sum("primaryValue").alias("tradeTotal"),

    sum(
        when(col("cmdCode") == "27",
             col("primaryValue"))
        .otherwise(0)
    ).alias("fuelValue"),

    sum(
        when(col("cmdCode") == "12",
             col("primaryValue"))
        .otherwise(0)
    ).alias("grainValue"),

    sum(
        when(col("cmdCode") == "93",
             col("primaryValue"))
        .otherwise(0)
    ).alias("gunValue")
)

# =========================================
# SAVE STAGE1
# =========================================
stage1.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(OUT_STAGE1)

# =========================================
# STAGE 2
# compute shares/percentages
# equivalent to p3.0 -> p3.1
# =========================================
stage2 = stage1.withColumn(
    "fuelShare",
    round(col("fuelValue") / col("tradeTotal") * 100, 2)
).withColumn(
    "grainShare",
    round(col("grainValue") / col("tradeTotal") * 100, 2)
).withColumn(
    "gunShare",
    round(col("gunValue") / col("tradeTotal") * 100, 2)
)

# =========================================
# SAVE STAGE2
# =========================================
stage2.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(OUT_STAGE2)

# =========================================
# STAGE 3
# sorting
# equivalent to Stage3 reducer sort
# =========================================
stage3 = stage2.orderBy(
    col("fuelShare").desc()
)

# =========================================
# SAVE STAGE3
# =========================================
stage3.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(OUT_STAGE3)

# =========================================
# SHOW RESULTS
# =========================================
stage3.show(50, truncate=False)

spark.stop()