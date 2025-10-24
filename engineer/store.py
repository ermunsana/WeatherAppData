import sqlite3
import os
from fetch import fetch_all_cities
from clean import clean_weather_data


db = "../db/weather_data.db"
os.makedirs(os.path.dirname(db), exist_ok=True) 

def start_db():
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS weather_data (
        id INTEGER PRIMARY KEY,
        city TEXT,
        temp_c REAL,
        aqi INTEGER
    )
    """)
    conn.commit()
    conn.close()

conn = sqlite3.connect(db)
cur = conn.cursor()
cur.execute("DROP TABLE IF EXISTS weather_data")
conn.commit()
conn.close()

def insert_data(df):
    conn = sqlite3.connect(db)
    df.to_sql('weather_data', conn, if_exists='replace', index=False)
    conn.close()

if __name__ == "__main__":
    start_db()
    raw_data = fetch_all_cities()
    df = clean_weather_data(raw_data)
    insert_data(df)

