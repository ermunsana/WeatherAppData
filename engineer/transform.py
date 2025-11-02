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



list_of_files = glob.glob("data/raw/*.json")
latest_file = max(list_of_files, key=lambda x: x) 

with open(latest_file, "r", encoding="utf-8") as f:
    raw_data = json.load(f)


df = transform(raw_data)


save_json = df.to_dict(orient="records")
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
file_path = f"data/clean/clean_data_{timestamp}.json"


with open(file_path, "w", encoding="utf-8") as f:
    json.dump(save_json, f, indent=2)


