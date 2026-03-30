from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("NYC Taxi Load")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )
    return spark


def read_processed(spark):
    """
    Читаем из processed слоя а не из raw.
    Данные уже чистые — не нужно фильтровать заново.
    Spark автоматически читает только year=2024/month=1
    если мы укажем фильтр — partition pruning в действии.
    """
    df = spark.read.parquet(
        "hdfs://namenode:9000/data/processed/nyc_taxi"
    )
    return df

def aggregate_by_location(df):
    """
    Агрегация по районам pickup.
    PULocationID — ID района где подобрали пассажира.
    Аналитик увидит какой район самый прибыльный.
    """
    return df.groupBy("PULocationID").agg(
        F.count("*").alias("trip_count"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_min"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue")
    ).orderBy(F.desc("trip_count"))

def aggregate_by_hour(df):
    """
    Агрегация по часам дня.
    Показывает в какое время больше всего поездок.
    Полезно для планирования числа машин.
    """
    return df.withColumn(
        "hour", F.hour("tpep_pickup_datetime")
    ).groupBy("hour").agg(
        F.count("*").alias("trip_count"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare")
    ).orderBy("hour")

def save(df, path):
    df.write.mode("overwrite").parquet(
        f"hdfs://namenode:9000/data/processed/{path}"
    )
    print(f"=== SAVED: /data/processed/{path} ===")


if __name__ == "__main__":
    spark = create_spark_session()
    df = read_processed(spark)

    print("=== TOP 10 LOCATIONS BY TRIP COUNT ===")
    agg_location = aggregate_by_location(df)
    agg_location.show(10)
    save(agg_location, "agg_by_location")

    print("=== TRIPS BY HOUR OF DAY ===")
    agg_hour = aggregate_by_hour(df)
    agg_hour.show(24)
    save(agg_hour, "agg_by_hour")

    spark.stop()