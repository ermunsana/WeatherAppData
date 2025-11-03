import os
import pandas as pd
import json
from datetime import datetime
import glob


def transform(raw_data):
    results = []
    for raw in raw_data:
        if raw is None:  # skip failed API calls
            continue
        clean_data = {
            "country": raw["country"],
            "city": raw["city"],
            "localtime": raw["localtime"],
            "temp_c": raw["temp_c"],
            "humidity": raw["humidity"],
            "cloud": raw["cloud"],
            "aqi": raw["air_quality"]
        }
        results.append(clean_data)
    df = pd.DataFrame(results)
    df = df.dropna().reset_index(drop=True)
    return df

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
raw_dir = os.path.join(BASE_DIR, "data", "raw")
clean_dir = os.path.join(BASE_DIR, "data", "clean")
os.makedirs(clean_dir, exist_ok=True)


list_of_files = glob.glob(os.path.join(raw_dir, "*.json"))
if not list_of_files:
    raise FileNotFoundError(f"No JSON files found in {raw_dir}")

latest_file = max(list_of_files, key=os.path.getctime)  

with open(latest_file, "r", encoding="utf-8") as f:
    raw_data = json.load(f)


df = transform(raw_data)


save_json = df.to_dict(orient="records")
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
file_path = os.path.join(clean_dir, f"clean_data_{timestamp}.json")

with open(file_path, "w", encoding="utf-8") as f:
    json.dump(save_json, f, indent=2)


