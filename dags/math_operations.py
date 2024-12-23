from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

#Define functions for each tasks

def start_number(**context):
    context['ti'].xcom_push(key='current_value',value=10)
    print("Starting number is 10")

def add_five(**context):
    current_value=context['ti'].xcom_pull(key='current_value',task_id='start_task')#
    new_value = current_value +5 
    context['ti'].xcom_push(key='current_value',value=new_value)
    print(f'Add 5: {current_value} + 5 = {new_value}')

def multiply_by_two(**context):
    current_value=context['ti'].xcom_pull(key='current_value',task_id='add_five_task')
    new_value = current_value * 2
    context['ti'].xcom_push(key='current_value',value=new_value)
    print(f'Multiply by 2: {current_value} * 2 = {new_value}')

def subtract_three(**context):
    current_value = context['ti'].xcom_pull(key='current_value',task_id='multiply_by_two_task')
    new_value = current_value -3 
    context['ti'].xcom_push(key='current_value',value=new_value)
    print(f'Subtract 3: {current_value} - 3 = {new_value}')

def square_number(**context):
    current_value = context['ti'].xcom_pull(key='current_value',task_id='subtract_three_task')
    new_value = current_value ** 2
    print(f'Square: {current_value} ^ 2 = {new_value}')

with DAG (
    dag_id = 'math_sequence_dag',
    start_date = datetime(2024,1,1),
    schedule_interval = '@once',
    catchup = False
) as dag:
    
    start_task = PythonOperator(task_id='start_task',python_callable=start_number,provide_context=True)
    add_five_task = PythonOperator(task_id='add_five_task',python_callable=add_five,provide_context=True)
    multiply_by_two_task = PythonOperator(task_id='multiply_by_two_task',python_callable=multiply_by_two,provide_context=True)
    subtract_three_task = PythonOperator(task_id='subtract_three_task',python_callable=subtract_three,provide_context=True)
    square_number_task = PythonOperator(task_id='square_number_task',python_callable=square_number,provide_context=True)

    start_task >> add_five_task >> multiply_by_two_task >> subtract_three_task >> square_number_task