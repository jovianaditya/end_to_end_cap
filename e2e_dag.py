from datetime import datetime, timedelta
from airflow import DAG
# from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Define the DAG
default_args = {
    'owner': 'jovian',
    'start_date': datetime(2023, 5, 24),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)}

dag = DAG(
    'e2e_fix',
    default_args=default_args,
    description='dag e2e',
    schedule_interval='@daily')

# Define the tasks
create_table_hive = BashOperator(
    task_id = 'create_table_hive',
    bash_command= 'python3 /home/jovianaditya/cap/e2e_hive.py',
    dag=dag)

spark_transform = BashOperator(
    task_id='spark_transform',
    bash_command= 'python3 /home/jovianaditya/cap/e2e_spark.py',
    dag=dag)

load_into_hive = BashOperator(
    task_id = 'load_into_hive',
    bash_command = 'python3 /home/jovianaditya/cap/e2e_load.py',
    dag=dag)

# Set task dependencies
create_table_hive >> spark_transform >> load_into_hive