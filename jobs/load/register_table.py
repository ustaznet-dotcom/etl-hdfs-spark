import requests
import json


def register_trino_table():
    """
    Регистрируем таблицу в Trino через REST API.
    Это идемпотентная операция — можно запускать много раз,
    таблица создаётся только если её нет.
    """
    queries = [
        """CREATE SCHEMA IF NOT EXISTS hive.nyc_taxi
           WITH (location = 'hdfs://namenode:9000/data/processed')""",

        """CREATE TABLE IF NOT EXISTS hive.nyc_taxi.trips (
            VendorID INTEGER,
            tpep_pickup_datetime TIMESTAMP,
            tpep_dropoff_datetime TIMESTAMP,
            passenger_count INTEGER,
            trip_distance DOUBLE,
            fare_amount DOUBLE,
            tip_amount DOUBLE,
            total_amount DOUBLE,
            payment_type BIGINT,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            trip_duration_minutes DOUBLE,
            year INTEGER,
            month INTEGER
        )
        WITH (
            external_location = 'hdfs://namenode:9000/data/processed/nyc_taxi',
            format = 'PARQUET',
            partitioned_by = ARRAY['year', 'month']
        )""",

        """CALL hive.system.sync_partition_metadata(
            'nyc_taxi', 'trips', 'FULL'
        )"""
    ]

    headers = {
        "X-Trino-User": "airflow",
        "Content-Type": "application/json"
    }

    for query in queries:
        response = requests.post(
            "http://trino:8080/v1/statement",
            headers=headers,
            data=query
        )

        if response.status_code != 200:
            raise Exception(f"Trino query failed: {response.text}")

        result = response.json()
        query_id = result.get("id")
        print(f"Query {query_id}: OK")

    print("=== TABLE REGISTERED IN TRINO ===")


if __name__ == "__main__":
    register_trino_table()
