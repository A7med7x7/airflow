from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# Exercise 1: Create imports, DAG argument, and definition

# Task 1.1: Define DAG arguments (2 pts)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Task 1.2: Define the DAG (2 pts)
dag = DAG(
    'data_processing_pipeline',
    default_args=default_args,
    description='A simple data processing DAG',
    schedule_interval=timedelta(days=1),
)

# Exercise 2: Create the tasks using BashOperator

# Task 2.1: Create a task to unzip data (2 pts)
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='unzip /path/to/data.zip -d /path/to/unzipped_data/',
    dag=dag,
)

# Task 2.2: Create a task to extract data from CSV file (2 pts)
extract_csv = BashOperator(
    task_id='extract_csv',
    bash_command='cut -d"," -f1,2,3 /path/to/unzipped_data/data.csv > /path/to/csv_data.csv',
    dag=dag,
)

# Task 2.3: Create a task to extract data from TSV file (2 pts)
extract_tsv = BashOperator(
    task_id='extract_tsv',
    bash_command='cut -f1,2,3 /path/to/unzipped_data/data.tsv > /path/to/tsv_data.tsv',
    dag=dag,
)

# Task 2.4: Create a task to extract data from a fixed-width file (2 pts)
extract_fixed_width = BashOperator(
    task_id='extract_fixed_width',
    bash_command='cut -c1-10,20-30 /path/to/unzipped_data/data.fw > /path/to/fixed_width_data.txt',
    dag=dag,
)

# Task 2.5: Create a task to consolidate data extracted from previous tasks (2 pts)
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste /path/to/csv_data.csv /path/to/tsv_data.tsv /path/to/fixed_width_data.txt > /path/to/consolidated_data.csv',
    dag=dag,
)

# Task 2.6: Transform the data (2 pts)
transform_data = BashOperator(
    task_id='transform_data',
    bash_command='awk -F"," \'{print $1,$2,$3}\' /path/to/consolidated_data.csv > /path/to/transformed_data.csv',
    dag=dag,
)

# Task 2.7: Define the task pipeline (1 pt)
unzip_data >> [extract_csv, extract_tsv, extract_fixed_width] >> consolidate_data >> transform_data