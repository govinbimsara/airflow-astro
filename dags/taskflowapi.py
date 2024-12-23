from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG (
    dag_id='math_sequence_dag_with_taskflow',
    start_date = datetime(2024,1,1),
    schedule_interval = '@once',
    catchup = False
) as dag:

    @task
    def start_number():
        intial_value = 10
        print(f'Starting number: {intial_value}')
        return intial_value
    
    @task
    def add_five(number):
        new_value = number + 5
        print(f'Add 5: {number} = {new_value}')
        return new_value

    @task
    def multiply_two(number):
        new_value = number * 2
        print(f'Multiply 2: {number} = {new_value}')
        return new_value
    
    @task
    def subtract_three(number):
        new_value = number - 3
        print(f'Subtract 3: {number} = {new_value}')
        return new_value

    @task
    def square_number(number):
        new_value = number ** 2 
        print(f'Square : {number} = {new_value}')
        return new_value

    start_value = start_number()
    added_value = add_five(start_value)
    multiplied_value = multiply_two(added_value)
    subtracted_value = subtract_three(multiplied_value)
    squared_value = square_number(subtracted_value)