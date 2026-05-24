import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, count, sum as _sum, when, concat_ws


def run_acled_jobs():
    if len(sys.argv) < 3:
        print("Użycie: spark-submit acled_job.py <input path> <output path>")
        sys.exit(-1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    temp1_path = "temp_1"
    temp2_path = "temp_2"

    # Inicjalizacja sesji Spark
    spark = SparkSession.builder \
        .appName("ACLED_Spark_Migration") \
        .getOrCreate()

    overall_start = time.time()

    start_j1 = time.time()

    raw_df = spark.read.option("header", "true").csv(input_path)

    step1_df = raw_df.withColumn("year", split(col("event_date"), "-")[0]) \
        .withColumn("fatalities", col("fatalities").cast("long")) \
        .filter(col("year").isNotNull() & col("iso").isNotNull())

    agg_df = step1_df.groupBy("iso", "year").agg(
        count("*").alias("event_count"),
        _sum("fatalities").alias("total_fatalities")
    )

    agg_df.write.mode("overwrite").option("header", "true").csv(temp1_path)

    end_j1 = time.time()
    print(f">>> JOB 1 zakończony w: {round(end_j1 - start_j1, 2)}s")

    start_j2 = time.time()

    schema_temp1 = "iso STRING, year STRING, event_count LONG, total_fatalities LONG"
    step2_input = spark.read.option("header", "true").schema(schema_temp1).csv(temp1_path)

    step2_df = step2_input.withColumn(
        "intensity",
        col("total_fatalities") / col("event_count")
    ).select("iso", "year", "total_fatalities", "intensity")

    step2_df.write.mode("overwrite").option("header", "true").csv(temp2_path)

    end_j2 = time.time()
    print(f">>> JOB 2 zakończony w: {round(end_j2 - start_j2, 2)}s")

    start_j3 = time.time()

    schema_temp2 = "iso STRING, year STRING, total_fatalities LONG, intensity DOUBLE"
    step3_input = spark.read.option("header", "true").schema(schema_temp2).csv(temp2_path)

    quantiles = step3_input.stat.approxQuantile("intensity", [0.25, 0.50, 0.75], 0.0)

    if not quantiles:
        print("Błąd: Brak danych do obliczenia kwantyli!")
        sys.exit(1)

    q1, q2, q3 = quantiles[0], quantiles[1], quantiles[2]

    final_df = step3_input.withColumn(
        "quantile",
        when(col("intensity") <= q1, "LOW")
        .when(col("intensity") <= q2, "MID_LOW")
        .when(col("intensity") <= q3, "MID_HIGH")
        .otherwise("HIGH")
    )

    formatted_output = final_df.select(
        concat_ws(",", col("iso"), col("year")).alias("key"),
        concat_ws(",", col("quantile"), col("total_fatalities")).alias("value")
    )

    formatted_output.write.mode("overwrite").option("sep", "\t").csv(output_path)

    end_j3 = time.time()
    print(f">>> JOB 3 zakończony w: {round(end_j3 - start_j3, 2)}s")

    print(f">>> CAŁKOWITY CZAS: {round(end_j3 - overall_start, 2)}s")

    spark.stop()


if __name__ == "__main__":
    run_acled_jobs()