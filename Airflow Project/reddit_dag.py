from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import Etl_reddit 

default_args = {
    'owner': 'Abdo',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'email': ['abdelrhmanosama42@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
dag= DAG(
    dag_id='reddit_dag',
    schedule_interval='@hourly',
    default_args=default_args,
    description="Apache air flow task"
    
)

extracting=PythonOperator(
    task_id='extract',
    python_callable=Etl_reddit.extract_data,
    dag=dag
)
transforming=PythonOperator(
    task_id='transform',
    python_callable=Etl_reddit.transform_data,
    dag=dag
)
loading=PythonOperator(
    task_id='load',
    python_callable=Etl_reddit.load_data,
    dag=dag
)




extracting
