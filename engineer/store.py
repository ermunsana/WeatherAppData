import sqlite3
import os
import pandas as pd
from fetch import fetch_all_cities
from clean import clean_weather_data


db = "../db/weather_data.db"
os.makedirs(os.path.dirname(db), exist_ok=True) 

def init_db():
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS weather_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city TEXT,
        temp_c REAL,
        aqi INTEGER
    )
    """)
    conn.commit()
    conn.close()


def insert_data(df):
    conn = sqlite3.connect(db)

    # Force DataFrame columns to match table schema
    df = df[["city", "temp_c", "aqi"]]

    # Append data safely
    df.to_sql("weather_data", conn, if_exists="replace", index=False)
    conn.close()

if __name__ == "__main__":
    init_db()
    raw_data = fetch_all_cities()
    df = clean_weather_data(raw_data)
    insert_data(df)

