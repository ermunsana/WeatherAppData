import schedule
import time
from datetime import datetime
from extract import get_cities
from transform import transform
from load import start_db, load_data

def etl():
    try:
        start_db()
        raw_data = get_cities()
        df = transform(raw_data)
        load_data(df)
        print(f"[{datetime.now()}] Done")
    except Exception as e:
        print(f"[{datetime.now()}] Failed: {e}")

schedule.every().day.at("06:00").do(etl)

while True:
    schedule.run_pending()
    time.sleep(60) 
