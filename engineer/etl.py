from fetch import get_cities
from clean import clean_weather_data
from store import start_db, insert_data

def etl():
    start_db()

    raw_data = get_cities()
    df = clean_weather_data(raw_data)

    insert_data(df)
    print("Fin")

if __name__ == "__main__":
    etl()
