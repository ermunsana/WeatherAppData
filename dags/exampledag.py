from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
from engineer.extract import get_cities
from engineer.transform import transform
from engineer.load import start_db, load_data


@dag(
    start_date=datetime(year=2025, month=11, day=1, hour=9, minute=0),
    schedule="@daily",
    catchup=True,
    max_active_runs=1
)
def weather_etl():
    @task()
    def extract_data():
        raw = get_cities()
        return raw

    @task()
    def transform_data(raw):
        df = transform(raw)
        return df

    @task()
    def load_data(df):
        start_db()
        load_data(df)
        print(df)


    raw_dataset = extract_data()
    transformed_dataset = transform_data(raw_dataset)
    load_data(transformed_dataset)



weather_etl()
