.PHONY: up down status logs run test init-hdfs load-data clean

up:
	docker-compose up -d
	@echo "Airflow:  http://localhost:8080"
	@echo "Spark:    http://localhost:8081"
	@echo "HDFS:     http://localhost:9870"
	@echo "Superset: http://localhost:8088"

down:
	docker-compose down

status:
	docker-compose ps

run:
	docker exec airflow-scheduler airflow dags trigger nyc_taxi_etl

test:
	docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/extract/quality_check.py

init-hdfs:
	docker exec namenode bash -c "hdfs dfs -mkdir -p /data/raw && hdfs dfs -mkdir -p /data/processed"

load-data:
	docker cp data/raw/yellow_tripdata_2024-01.parquet namenode:/tmp/
	docker exec namenode bash -c "hdfs dfs -put -f /tmp/yellow_tripdata_2024-01.parquet /data/raw/"

clean:
	docker-compose down -v