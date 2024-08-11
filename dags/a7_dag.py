from airflow import DAG
from datetime import datetime , timedelta
from airflow.operators.bash_operator import BashOperator 

default_args = {
    'owner' : 'admin7',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=2)
}

with DAG(
    dag_id = "a7_dag_v3",
    default_args = default_args,
    description = 'this dag is responsibile of doing jobs',
    start_date = datetime(2024,8,12),
    schedule_interval = '@daily'
    
) as dag:
    
    task1 = BashOperator(
        task_id = 'first_task',
        bash_command = 'echo hello world'
    )
    task2 = BashOperator(
        task_id = 'second_task',
        bash_command = 'echo Im the second task and I will run after the second one.'
    )
    task3 = BashOperator(
        task_id = 'third_task', 
        bash_command = "echo I'm the third task"
        
    )


task1.set_downstream(task2)
task1.set_downstream(task3)

#or replace it with 
#task1 >> task2 
#task2 >> task3
#task1 >> [task2, task3]
