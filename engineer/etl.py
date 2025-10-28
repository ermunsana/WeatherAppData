from extract import get_cities
from transform import transform
from load import start_db, load_data

def etl():
    start_db()

    raw_data = get_cities()
    df = transform(raw_data)

    load_data(df)
    print("Done")

if __name__ == "__main__":
    etl()
