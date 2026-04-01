# NYC Taxi ETL Pipeline

End-to-end ETL pipeline обрабатывающий 3M+ записей NYC Yellow Taxi
с использованием Apache Spark, HDFS и Apache Airflow — всё в Docker.

## Бизнес-задача

Taxi-компании нужно понимать:
- Какие районы приносят максимальную выручку?
- В какое время суток спрос максимальный?
- Сколько данных теряется из-за проблем качества?

Этот pipeline отвечает на эти вопросы автоматически каждый день.

## Результаты

### Топ районы по выручке (январь 2024)

| Район        | Поездок | Средний fare | Общая выручка |
|--------------|---------|--------------|---------------|
| JFK Airport  | 136,949 | $62.87       | $11,077,565   |
| LaGuardia    | 86,560  | $42.36       | $5,746,299    |
| Midtown (161)| 134,965 | $15.45       | $3,234,940    |

### Спрос по времени суток

| Время   | Поездок | Средний fare | Вывод                        |
|---------|---------|--------------|------------------------------|
| 04:00   | 12,782  | $23.02       | Минимум — аэропорт поездки   |
| 05:00   | 15,627  | $27.57       | Ранние рейсы, дорогие        |
| 18:00   | 195,911 | $16.95       | Пик дня — короткие поездки   |
| 17:00   | 191,251 | $18.07       | Вечерний час пик             |

### Качество данных

- Сырых строк: **2,964,624**
- После очистки: **2,723,707**
- Отфильтровано: **240,917 (8.1%)** — нулевые дистанции, отрицательные суммы, невалидные даты

## Архитектура
```
NYC Taxi Parquet
      |
   [Extract]
      |
  HDFS /raw
      |
  [Transform] — очистка, типы, партиционирование
      |
  HDFS /processed/nyc_taxi/year=2024/month=1/
      |
    [Load] — агрегации по районам и часам
      |
  Business Insights

  Всё оркестрируется Airflow DAG (ежедневно в 00:00)
```

## Стек

| Компонент      | Версия | Роль                        |
|----------------|--------|-----------------------------|
| Apache Spark   | 3.5.3  | Распределённая обработка    |
| HDFS (Hadoop)  | 3.2.1  | Распределённое хранилище    |
| Apache Airflow | 2.8.1  | Оркестрация DAG             |
| PostgreSQL     | 15     | Метаданные Airflow          |
| Docker Compose | -      | Локальный кластер (7 сервисов) |
| Python         | 3.8    | PySpark jobs                |

## Быстрый старт
```bash
git clone https://github.com/ТВО_ИМЯ/etl-hdfs-spark.git
cd etl-hdfs-spark
cp .env.example .env
docker-compose up -d
```

Airflow UI: http://localhost:8080 (admin/admin)
Spark UI:   http://localhost:8081
HDFS UI:    http://localhost:9870

## Структура проекта
```
├── dags/
│   └── nyc_taxi_etl.py       # Airflow DAG
├── jobs/
│   ├── extract/extract.py    # Загрузка в HDFS
│   ├── transform/transform.py # Очистка и обогащение
│   └── load/load.py          # Агрегации
├── docker/
│   └── hadoop/               # Конфиги HDFS
└── docker-compose.yml        # Кластер из 7 сервисов
```
The AT command has been deprecated. Please use schtasks.exe instead.

The request is not supported.
