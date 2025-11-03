import sqlite3
import os
import pandas as pd
from extract import get_cities
from transform import transform


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
db_dir = os.path.join(BASE_DIR, "..", "db")
os.makedirs(db_dir, exist_ok=True)

db = os.path.join(db_dir, "weather_database.db")

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

    df.to_sql("weather_data", conn, if_exists="append", index=False)
    conn.close()

