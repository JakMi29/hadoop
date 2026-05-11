from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, trim, regexp_replace, sum as _sum, count
import time


def run_acled_step1():
    spark = SparkSession.builder \
        .appName("ACLED_Spark_Migration_Step1") \
        .getOrCreate()

    start_time = time.time()

    try:
        raw_df = spark.read.option("header", "true").csv("hdfs://master:9000/acled_data.csv")

        processed_df = raw_df.select(
            split(col("event_date"), "-")[0].alias("year"),
            col("iso"),
            col("fatalities").cast("long")
        )

        result_df = processed_df.groupBy("iso", "year").agg(
            count("*").alias("event_count"),
            _sum("fatalities").alias("total_fatalities")
        )

        print(">>> WYNIK PRZETWARZANIA ACLED STEP 1 <<<")
        result_df.show(50)


    except Exception as e:
        print(f"BŁĄD: {e}")
    finally:
        end_time = time.time()
        print(f">>> CZAS WYKONANIA SPARK: {round(end_time - start_time, 2)}s")
        spark.stop()


if __name__ == "__main__":
    run_acled_step1()