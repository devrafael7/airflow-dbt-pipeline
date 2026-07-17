from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="ecommerce_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    ingest_data = BashOperator(
    task_id="ingest_dummyjson_to_snowflake",
    bash_command=(
        "python /opt/airflow/ingestion/"
        "ingest_dummyjson.py"
    ),
)

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt run --profiles-dir /home/airflow/.dbt"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt test --profiles-dir /home/airflow/.dbt"
        ),
    )

    ingest_data >> dbt_run >> dbt_test