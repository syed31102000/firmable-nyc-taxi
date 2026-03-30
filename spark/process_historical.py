from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, date_trunc, year, month,
    count, sum as spark_sum, avg, when
)

def main():
    spark = (
        SparkSession.builder
        .appName("nyc-taxi-historical")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    input_path = "data/raw/*.parquet"
    zone_path = "dbt/seeds/taxi_zone_lookup.csv"
    output_path = "data/processed/agg_daily_revenue"

    trips = spark.read.parquet(input_path)
    zones = spark.read.option("header", True).csv(zone_path)

    cleaned = (
        trips
        .withColumn("pickup_datetime", to_timestamp(col("tpep_pickup_datetime")))
        .withColumn("dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))
        .withColumn(
            "trip_duration_minutes",
            (col("dropoff_datetime").cast("long") - col("pickup_datetime").cast("long")) / 60.0
        )
        .filter(col("trip_distance") > 0)
        .filter(col("fare_amount") > 0)
        .filter(col("passenger_count") > 0)
        .filter(col("trip_duration_minutes").between(1, 180))
        .withColumn("trip_date", date_trunc("day", col("pickup_datetime")))
        .withColumn("year", year(col("pickup_datetime")))
        .withColumn("month", month(col("pickup_datetime")))
    )

    daily = (
        cleaned
        .repartition("year", "month")
        .groupBy("trip_date", "year", "month")
        .agg(
            count("*").alias("total_trips"),
            spark_sum("fare_amount").alias("total_fare"),
            avg("fare_amount").alias("avg_fare"),
            spark_sum("tip_amount").alias("total_tips"),
            when(spark_sum("fare_amount") == 0, 0.0)
            .otherwise((spark_sum("tip_amount") / spark_sum("fare_amount")) * 100.0)
            .alias("tip_rate_percent")
        )
    )

    (
        daily
        .write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(output_path)
    )

    print("Historical processing completed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()