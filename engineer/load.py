import sqlite3
import os
import pandas as pd
from extract import get_cities
from transform import transform


db = "../db/weather_database.db"
os.makedirs(os.path.dirname(db), exist_ok=True) 

def start_db():
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS weather_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        country TEXT,
        city TEXT,
        localtime TEXT,
        temp_c REAL,
        humidity INTEGER,
        cloud INTEGER,
        aqi INTEGER
    )
    """)
    conn.commit()
    conn.close()


def load_data(df):
    conn = sqlite3.connect(db)

    df = df[["city", "temp_c", "aqi"]]

    df.to_sql("weather_data", conn, if_exists="replace", index=False)
    conn.close()

if __name__ == "__main__":
    start_db()
    raw_data = get_cities()
    df = transform(raw_data)
    load_data(df)

