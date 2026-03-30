from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("NYC Taxi Transform")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )
    return spark


def read_raw(spark):
    """
    Читаем сырые данные из HDFS.
    Тот же файл что загрузили в extract фазе.
    """
    df = spark.read.parquet(
        "hdfs://namenode:9000/data/raw/yellow_tripdata_2024-01.parquet"
    )
    return df


def clean(df):
    """
    Убираем невалидные строки.
    Каждый filter — это бизнес-правило:
    почему эта строка не имеет смысла?
    """
    df_clean = df.filter(
        # Хотя бы один пассажир
        (F.col("passenger_count") >= 1) &
        (F.col("passenger_count") <= 6) &
        # Поездка должна иметь дистанцию
        (F.col("trip_distance") > 0) &
        # Стоимость не может быть отрицательной
        (F.col("fare_amount") > 0) &
        # Время посадки должно быть раньше высадки
        (F.col("tpep_pickup_datetime") < F.col("tpep_dropoff_datetime")) &
        (F.year("tpep_pickup_datetime") == 2024) &
        (F.month("tpep_pickup_datetime") == 1)
    )
    return df_clean


def enrich(df):
    """
    Добавляем новые колонки которых не было в сырых данных.
    Это называется feature engineering.
    """
    df_enriched = df.withColumn(
        # Длительность поездки в минутах
        "trip_duration_minutes",
        (
            F.unix_timestamp("tpep_dropoff_datetime") -
            F.unix_timestamp("tpep_pickup_datetime")
        ) / 60
    ).withColumn(
        # Год — нужен для партиционирования
        "year",
        F.year("tpep_pickup_datetime")
    ).withColumn(
        # Месяц — нужен для партиционирования
        "month",
        F.month("tpep_pickup_datetime")
    ).withColumn(
        # Исправляем тип: long → integer (экономим память)
        "passenger_count",
        F.col("passenger_count").cast(IntegerType())
    )
    return df_enriched


def select_columns(df):
    """
    Оставляем только нужные колонки.
    В реальных проектах это важно —
    не тащим лишние данные в processed слой.
    """
    return df.select(
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "tip_amount",
        "total_amount",
        "payment_type",
        "PULocationID",
        "DOLocationID",
        "trip_duration_minutes",
        "year",
        "month"
    )


def save(df):
    """
    Сохраняем в HDFS в формате Parquet.
    partitionBy — создаёт папочную структуру year=/month=.
    mode("overwrite") — перезаписывает если уже есть.
    """
    df.write.mode("overwrite").partitionBy("year", "month").parquet(
        "hdfs://namenode:9000/data/processed/nyc_taxi"
    )
    print("=== SAVED TO HDFS /data/processed/nyc_taxi ===")


def log_stats(df_raw, df_clean):
    """
    Показываем сколько строк отфильтровали и почему.
    В продакшне это уходит в monitoring систему.
    """
    raw_count = df_raw.count()
    clean_count = df_clean.count()
    dropped = raw_count - clean_count
    pct = (dropped / raw_count) * 100

    print(f"=== DATA QUALITY REPORT ===")
    print(f"Raw rows:     {raw_count:,}")
    print(f"Clean rows:   {clean_count:,}")
    print(f"Dropped rows: {dropped:,} ({pct:.1f}%)")


if __name__ == "__main__":
    spark = create_spark_session()

    df_raw = read_raw(spark)
    df_clean = clean(df_raw)
    df_enriched = enrich(df_clean)
    df_final = select_columns(df_enriched)

    log_stats(df_raw, df_clean)
    save(df_final)

    spark.stop()
