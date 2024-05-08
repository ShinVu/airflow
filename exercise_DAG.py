from datetime import timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator

from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'ETL_Server_Access_Log_Processing',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
)

t0 = BashOperator(
    task_id = "dowload",
    bash_command='wget  -O /home/airflow/project/web-server-access-log.txt "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt" '
)
t1 = BashOperator(
    task_id = "extract",
    bash_command='cut -d "#" -f 1,4 /home/airflow/project/web-server-access-log.txt | tr "#" "," > /home/airflow/project/extracted_data.txt',
    dag = dag
)

t2 = BashOperator(
    task_id = "transform",
    bash_command='tr [:upper:] [:lower:] < /home/airflow/project/extracted_data.txt > /home/airflow/project/transformed_data.csv',
    dag = dag
)

t3 = BashOperator(
    task_id="load",
    bash_command='tar -czf /home/airflow/project/compressed_data.tar.gz  /home/airflow/project/transformed_data.csv /home/airflow/project/extracted_data.txt',
    dag = dag
)

t0 >> t1
t1 >> t2
t2 >> t3

