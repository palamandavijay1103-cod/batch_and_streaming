from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "vijaykanth",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="nyc_taxi_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    description="NYC Taxi ETL Pipeline",
) as dag:

    bronze_ingestion = BashOperator(
        task_id="bronze_ingestion",
        bash_command="python /opt/airflow/spark_jobs/bronze/bronze_ingestion.py"
    )

    silver_transformation = BashOperator(
        task_id="silver_transformation",
        bash_command="python /opt/airflow/spark_jobs/silver/silver_partitioned_etl.py"
    )

    gold_aggregation = BashOperator(
        task_id="gold_aggregation",
        bash_command="python /opt/airflow/spark_jobs/gold/gold_daily_metrics.py"
    )

    bronze_ingestion >> silver_transformation >> gold_aggregation