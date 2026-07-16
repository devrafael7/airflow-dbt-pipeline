from airflow import DAG #importa DAG do airflow para utilizar
from airflow.operators.bash import BashOperator #importa BashOperator para executar comandos do terminal
from datetime import datetime #importa data e hora 

with DAG( #inicializa DAG 
    dag_id="dbt_run_dag", #passa id da DAG 
    start_date=datetime(2026, 1, 1), #inicializa no primeiro dia e mês de 2026
    schedule=None, #sem schedule para rodar
    catchup=False, 
) as dag: #como uma DAG execute:
    dbt_run = BashOperator( # BashOperator com id da task e o comando escrito
        task_id="dbt_run", 
        bash_command="cd /opt/airflow/dbt && dbt run", #entre na pasta airflow/dbt do container e rode "dbt run"
    )