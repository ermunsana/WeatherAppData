from prefect import flow, task
from engineer.extract import get_cities
from engineer.transform import clean_weather_data
from engineer.load import start_db, insert_data


@task(retries=2, retry_delay_seconds=5)
def fetch_task():
    raw = get_cities()
    return raw


@task
def clean_task(raw):
    df = clean_weather_data(raw)
    return df


@task
def store_task(df):
    start_db()
    insert_data(df)
    return "Data stored successfully."


@flow(name="stage1_data_engineering_pipeline")
def stage1_pipeline():
    print("Starting Stage 1: Data Engineering pipeline...")
    raw = fetch_task()
    df = clean_task(raw)
    store_task(df)
    print("Stage 1 completed. Data appended to SQLite.")


if __name__ == "__main__":
    stage1_pipeline()
