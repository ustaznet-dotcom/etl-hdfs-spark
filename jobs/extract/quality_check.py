from pyspark.sql import SparkSession
from pyspark.sql import functions as F


class DataQualityError(Exception):
    """
    Специальный тип ошибки для проблем с данными.
    Когда Airflow видит это исключение — таск падает
    с понятным сообщением, а не просто RuntimeError.
    """
    pass


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("NYC Taxi Quality Check")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )
    return spark


def run_checks(df, filename):
    """
    Каждая проверка — это бизнес-правило.
    Если правило нарушено — останавливаем pipeline.
    """
    results = []

    # Проверка 1: файл не пустой
    row_count = df.count()
    check1 = row_count > 0
    results.append({
        "check": "file_not_empty",
        "passed": check1,
        "value": row_count,
        "threshold": "> 0"
    })
    if not check1:
        raise DataQualityError(
            f"CRITICAL: File {filename} is empty (0 rows)"
        )

    # Проверка 2: достаточно строк
    # Январь 2024 должен иметь минимум 1M поездок
    check2 = row_count >= 1_000_000
    results.append({
        "check": "minimum_row_count",
        "passed": check2,
        "value": row_count,
        "threshold": ">= 1,000,000"
    })
    if not check2:
        raise DataQualityError(
            f"CRITICAL: Too few rows ({row_count:,}). "
            f"Expected >= 1,000,000. Possible data loss."
        )

    # Проверка 3: колонки на месте
    required_cols = [
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance", "fare_amount", "total_amount"
    ]
    missing_cols = [c for c in required_cols if c not in df.columns]
    check3 = len(missing_cols) == 0
    results.append({
        "check": "required_columns_present",
        "passed": check3,
        "value": f"missing: {missing_cols}",
        "threshold": "no missing columns"
    })
    if not check3:
        raise DataQualityError(
            f"CRITICAL: Missing columns: {missing_cols}"
        )

    # Проверка 4: не слишком много null в ключевых колонках
    null_fare = df.filter(F.col("fare_amount").isNull()).count()
    null_pct = (null_fare / row_count) * 100
    check4 = null_pct < 10.0
    results.append({
        "check": "fare_amount_null_rate",
        "passed": check4,
        "value": f"{null_pct:.1f}%",
        "threshold": "< 10%"
    })
    if not check4:
        raise DataQualityError(
            f"CRITICAL: fare_amount has {null_pct:.1f}% nulls. "
            f"Expected < 10%. File may be corrupted."
        )

    # Проверка 5: даты в разумном диапазоне
    # Файл должен содержать данные не старше 2 лет
    old_rows = df.filter(
        F.year("tpep_pickup_datetime") < 2022
    ).count()
    old_pct = (old_rows / row_count) * 100
    check5 = old_pct < 1.0
    results.append({
        "check": "date_range_valid",
        "passed": check5,
        "value": f"{old_pct:.2f}% rows before 2022",
        "threshold": "< 1%"
    })
    if not check5:
        raise DataQualityError(
            f"CRITICAL: {old_pct:.2f}% of rows have dates before 2022. "
            f"Wrong file loaded?"
        )

    return results


def print_report(results):
    print("\n=== DATA QUALITY REPORT ===")
    print(f"{'Check':<30} {'Status':<10} {'Value':<25} {'Threshold'}")
    print("-" * 80)
    for r in results:
        status = "PASS" if r["passed"] else "FAIL"
        print(
            f"{r['check']:<30} {status:<10} "
            f"{str(r['value']):<25} {r['threshold']}"
        )
    print("=" * 80)
    all_passed = all(r["passed"] for r in results)
    print(f"Overall: {'ALL CHECKS PASSED' if all_passed else 'FAILED'}\n")


if __name__ == "__main__":
    spark = create_spark_session()

    df = spark.read.parquet(
        "hdfs://namenode:9000/data/raw/yellow_tripdata_2024-01.parquet"
    )

    results = run_checks(df, "yellow_tripdata_2024-01.parquet")
    print_report(results)

    spark.stop()
    print("Quality checks passed. Pipeline can proceed.")
