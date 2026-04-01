from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import sys
sys.path.insert(0, '/opt/airflow/jobs')

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="nyc_taxi_etl",
    default_args=default_args,
    description="ETL pipeline: NYC Taxi data",
    schedule_interval="0 0 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    quality_check = BashOperator(
        task_id="quality_check",
        bash_command=(
            "docker exec spark-master "
            "/opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "/opt/spark/jobs/extract/quality_check.py"
        ),
    )

    extract = BashOperator(
        task_id="extract",
        bash_command=(
            "docker exec spark-master "
            "/opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "/opt/spark/jobs/extract/extract.py"
        ),
    )

    transform = BashOperator(
        task_id="transform",
        bash_command=(
            "docker exec spark-master "
            "/opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "/opt/spark/jobs/transform/transform.py"
        ),
    )

    load = BashOperator(
        task_id="load",
        bash_command=(
            "docker exec spark-master "
            "/opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "/opt/spark/jobs/load/load.py"
        ),
    )

    register_table = BashOperator(
        task_id="register_table",
        bash_command=(
            "docker exec airflow-scheduler "
            "python /opt/airflow/jobs/load/register_table.py"
        ),
    )

    quality_check >> extract >> transform >> load >> register_table

