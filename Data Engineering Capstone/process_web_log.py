from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import datetime as dt

#Task 1: Define the DAG arguments
default_args = {
  'owner': 'Zack Long',
  'start_date': dt.datetime(2024,8,20),
  'email': ['zack.long@email.com'],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 1,
  'retry_delay': dt.timedelta(minutes=5),
}

#Task 2: Define the DAG
dag=DAG(
  'process_web_log',
  description='Capstone Project - Analyze webserve log file ETL',
  default_args=default_args,
  schedule_interval=dt.timedelta(days=1),
)
#Task 3: Create a Task to extract data
extract_data = BashOperator(
  task_id='extract_data',
  bash_command='cut -f1 -d" " $AIRFLOW_HOME/dags/capstone/accesslog.txt > $AIRFLOW_HOME/dags/capstone/extracted_data.txt',
  dag=dag,
)
#Task 4: Transform the data
transform_data = BashOperator(
  task_id='transform_data',
  bash_command='grep -vw "198.46.149.143" $AIRFLOW_HOME/dags/capstone/extracted_data.txt > $AIRFLOW_HOME/dags/capstone/transformed_data.txt',
  dag=dag,
)
#Task 5: Create a task to load the data
load_data = BashOperator(
  task_id='load_data',
  bash_command='tar -zcvf $AIRFLOW_HOME/dags/capstone/weblog.tar $AIRFLOW_HOME/dags/capstone/transformed_data.txt',
  dag=dag,
)
#Task 6: Define the task pipeline
extract_data >> transform_data >> load_data

