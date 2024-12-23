from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

#Define task 1
def preprocess_data():
    print("Preprocessing data...")

#Define task 2
def train_model():
    print("Training the model...")

#Define task 2
def evaluate_model():
    print("Evaluating the model...")

with DAG(
    'ml_pipeline',
    start_date=datetime(2024,1,1),
    schedule_interval = '@daily'
) as dag:
    
    #Define the tasks
    preprocess = PythonOperator(task_id="preprocess_task",python_callable=preprocess_data)
    train = PythonOperator(task_id="train_task",python_callable=train_model)
    evaluation = PythonOperator(task_id="evaluation_task",python_callable=evaluate_model)

    ##Set dependencies

    preprocess >> train >> evaluation