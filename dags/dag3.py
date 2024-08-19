from datetime import timedelta
from airflow import DAG
import pendulum

# Define DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.today('UTC').add(days=-0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'your_dag_id',
    default_args=default_args,
    description='Your DAG description',
    schedule=timedelta(days=1),
)

from airflow.operators.bash import BashOperator

# Create a task to unzip data
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='unzip /path/to/your/data.zip',
    dag=dag,
)

from airflow.operators.python import PythonOperator
import pandas as pd

def extract_data_from_csv(**kwargs):

    df = pd.read_csv('/path fpr the file')

    kwargs['ti'].xcom_push(key='extracted_data', value=df.to_json())


extract_data_from_csv = PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extract_data_from_csv,
    dag=dag,
)


def extract_data_from_tsv(**kwargs):
    # Read the TSV file into a pandas DataFrame
    df = pd.read_csv('/path/of some data.tsv', sep='\t')
    kwargs['ti'].xcom_push(key='extracted_data', value=df.to_json())


extract_data_from_tsv = PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extract_data_from_tsv,
    dag=dag,
)



def extract_data_from_fixed_width(**kwargs):

    col_specs = [(0, 10), (10, 20), (20, 30)]
    col_names = ['column1', 'column2', 'column3']
    df = pd.read_fwf('/path/tosome/data.txt', colspecs=col_specs, names=col_names)


    kwargs['ti'].xcom_push(key='extracted_data', value=df.to_json())

# Create a task to extract data from a fixed width file
extract_data_from_fixed_width = PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extract_data_from_fixed_width,
    dag=dag,
)

def consolidate_data(**kwargs):

    extracted_data_csv = pd.read_json(kwargs['ti'].xcom_pull(task_ids='extract_data_from_csv', key='extracted_data'))
    extracted_data_tsv = pd.read_json(kwargs['ti'].xcom_pull(task_ids='extract_data_from_tsv', key='extracted_data'))
    extracted_data_fixed_width = pd.read_json(kwargs['ti'].xcom_pull(task_ids='extract_data_from_fixed_width', key='extracted_data'))

    df = pd.concat([extracted_data_csv, extracted_data_tsv, extracted_data_fixed_width])
    kwargs['ti'].xcom_push(key='consolidated_data', value=df.to_json())


consolidate_data = PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate_data,
    dag=dag,
)


def transform_data(**kwargs):

    df = pd.read_json(kwargs['ti'].xcom_pull(task_ids='consolidate_data', key='consolidated_data'))
    kwargs['ti'].xcom_push(key='transformed_data', value=df.to_json())

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)


unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data