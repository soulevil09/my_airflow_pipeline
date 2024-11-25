from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

def etl_task():
    from etl_job import dados_cidade
    return dados_cidade(11700100)

def email_task(**kwargs):
    from send_email import enviar_email
    file_path = kwargs['ti'].xcom_pull(task_ids='run_etl')
    enviar_email(file_path, 'MS_b9ZGaq@trial-k68zxl2n56elj905.mlsender.net', 'igsfortnite@gmail.com', 'ZrUQoletNq6z5OWp', 'Relatório')

with DAG('dados_cidade_pipeline',
         default_args={'start_date':datetime(2024, 11, 11)},
         schedule_interval='@daily',
         catchup=False) as dag:
    
    etl = PythonOperator(
        task_id='run_etl',
        python_callable=etl_task
    )

    email = PythonOperator(
        task_id='send_email',
        python_callable=email_task,
        provide_context=True
    )

    etl >> email