# ETL Pipeline: HDFS + Spark + Airflow

End-to-end ETL pipeline обрабатывающий NYC Taxi данные (~150M строк)
с использованием Apache Spark, HDFS и Apache Airflow.

## Архитектура
```
[Raw CSV] → HDFS /raw → Spark Transform → HDFS /processed (Parquet) → Airflow DAG
```

## Стек

| Компонент | Версия | Роль |
|-----------|--------|------|
| Apache Spark | 3.5 | Распределённая обработка |
| HDFS (Hadoop) | 3.3 | Распределённое хранилище |
| Apache Airflow | 2.8 | Оркестрация |
| Docker Compose | - | Локальный кластер |
| Python | 3.11 | PySpark jobs |

## Быстрый старт
```bash
git clone https://github.com/ТВО_ИМЯ/etl-hdfs-spark.git
cd etl-hdfs-spark
cp .env.example .env
docker-compose up -d
```

## Структура проекта
```
├── dags/        # Airflow DAG-файлы
├── jobs/        # PySpark скрипты (Extract / Transform / Load)
├── docker/      # Конфиги Hadoop и Spark
├── tests/       # Unit-тесты
└── docs/        # Архитектурные схемы
```

## Статус

🚧 В разработке