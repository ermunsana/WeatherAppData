from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task, dag
from engineer.extract import get_cities
from engineer.transform import transform
from engineer.load import start_db, load_data

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 11, 3),
}

@dag(
    default_args=default_args,
    description='An ETL pipeline DAG',
    schedule='@daily',
    catchup=False,
)
def etl_dag():
    
    @task
    def start_database():
        start_db()

    @task
    def extract_data():
        return get_cities()

    @task
    def transform_data(raw_data):
        return transform(raw_data)

    @task
    def load_data_task(transformed_data):
        load_data(transformed_data)


    start_db_task = start_database()
    raw_data = extract_data()
    transformed_data = transform_data(raw_data)
    load_data_task(transformed_data)

    start_db_task >> raw_data >> transformed_data >> load_data_task



etl_dag = etl_dag()
