
import sqlite3
import pandas as pd

db = "../db/weather_data.db"

def cargar_data():
    conn = sqlite3.connect(db)
    df = pd.read_sql_query("SELECT * FROM weather_data", conn)
    conn.close()
    return df

if __name__ == "__main__":
    df = cargar_data()
    print(df.to_string())
