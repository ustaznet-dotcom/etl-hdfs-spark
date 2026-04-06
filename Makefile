.PHONY: up down restart status logs run test clean

# Поднять весь кластер
up:
docker-compose up -d
@echo "Waiting for services to start..."
@sleep 30
@echo "Cluster is ready!"
@echo "Airflow UI:  http://localhost:8080 (admin/admin)"
@echo "Spark UI:    http://localhost:8081"
@echo "HDFS UI:     http://localhost:9870"
@echo "Superset UI: http://localhost:8088 (admin/admin)"

# Остановить кластер
down:
docker-compose down

# Перезапустить кластер
restart: down up

# Статус всех сервисов
status:
docker-compose ps

# Логи конкретного сервиса (make logs s=namenode)
logs:
docker-compose logs -f $(s)

# Запустить pipeline вручную
run:
docker exec airflow-scheduler airflow dags trigger nyc_taxi_etl
@echo "Pipeline triggered! Check http://localhost:8080"

# Запустить quality check отдельно
test:
docker exec spark-master \
/opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
/opt/spark/jobs/extract/quality_check.py

# Создать директории в HDFS
init-hdfs:
docker exec namenode bash -c "\
hdfs dfs -mkdir -p /data/raw && \
hdfs dfs -mkdir -p /data/processed && \
hdfs dfs -mkdir -p /data/logs && \
echo 'HDFS directories created'"

# Загрузить датасет в HDFS
load-data:
docker cp data/raw/yellow_tripdata_2024-01.parquet namenode:/tmp/
docker exec namenode bash -c "\
hdfs dfs -put -f /tmp/yellow_tripdata_2024-01.parquet /data/raw/"
@echo "Dataset loaded to HDFS"

# Полная очистка (удаляет все данные!)
clean:
docker-compose down -v
@echo "All containers and volumes removed"
