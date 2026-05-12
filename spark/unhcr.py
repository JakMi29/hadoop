"""
Stage1 PySpark implementation
Equivalent to Stage1Mapper and Stage1Reducer from the Java MapReduce code.

Reads uncomtrade CSV data and:
1. Extracts reporterCode, refYear, cmdCode, and primaryValue
2. Groups by (reporterCode, refYear)
3. Aggregates to calculate:
   - tradeTotal: sum of all values
   - fuelValue: sum where cmdCode == "27"
   - grainValue: sum where cmdCode == "12"
   - gunValue: sum where cmdCode == "93"
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when, coalesce
from pyspark.sql.types import DoubleType
import sys


def main():
    spark = SparkSession.builder \
        .appName("UncomtradeStage1") \
        .getOrCreate()

    input_path = "hdfs:///master:9000/acled/unhcr_population.csv"
    output_path = "hdfs:///master:9000/pyspark/unhcr/unhcr_population.csv"
    
    # Read the CSV file
    df = spark.read.csv(input_path, header=True, inferSchema=False)

    # Column indices (0-indexed):
    # col 3: refYear, col 6: reporterCode, col 20: cmdCode, col 42: primaryValue
    # When using header=True, columns are named _1, _2, _3, etc. for numeric indices
    # But with proper headers, we can use column names directly
    
    # Get column names and select by index
    cols = df.columns
    
    # Extract columns by position
    # Note: if the CSV has headers, we need to handle them properly
    df = df.select(
        col(cols[6]).alias("reporterCode"),
        col(cols[3]).alias("refYear"),
        col(cols[20]).alias("cmdCode"),
        col(cols[42]).alias("primaryValue")
    )

    # Filter out rows with empty reporterCode or refYear
    df = df.filter(
        (col("reporterCode").isNotNull()) & 
        (col("reporterCode") != "") &
        (col("refYear").isNotNull()) & 
        (col("refYear") != "")
    )

    # Convert primaryValue to double, default to 0 on conversion failure
    df = df.withColumn(
        "primaryValue",
        coalesce(col("primaryValue").cast(DoubleType()), 0.0)
    )

    # Group by (reporterCode, refYear) and aggregate
    result = df.groupBy("reporterCode", "refYear").agg(
        sum("primaryValue").alias("tradeTotal"),
        sum(when(col("cmdCode") == "27", col("primaryValue")).otherwise(0.0)).alias("fuelValue"),
        sum(when(col("cmdCode") == "12", col("primaryValue")).otherwise(0.0)).alias("grainValue"),
        sum(when(col("cmdCode") == "93", col("primaryValue")).otherwise(0.0)).alias("gunValue")
    )

    # Write output as CSV (single partition for simplicity)
    result.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", "false") \
        .csv(output_path)

    print(f"Stage1 processing complete. Output written to {output_path}")


if __name__ == "__main__":
    main()
