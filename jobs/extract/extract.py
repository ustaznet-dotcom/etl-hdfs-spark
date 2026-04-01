from pyspark.sql import SparkSession

def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("Spark SQL Example")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )
    return spark

def extract(spark):
    df = spark.read.parquet(
        "hdfs://namenode:9000/data/raw/yellow_tripdata_2024-01.parquet"
    )
    return df


def inspect(spark):
    
    print("=== SCHEMA ===")
    df.printSchema()

    print("=== SAMPLE (5 rows) ===")
    df.show(5, truncate=False)

    print("=== ROW COUNT ===")
    print(f"Total rows: {df.count():,}")


if __name__ == "__main__":
    spark = create_spark_session()
    df = extract(spark)
    inspect(df)
    spark.stop()