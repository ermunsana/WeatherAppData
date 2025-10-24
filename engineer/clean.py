import pandas as pd

def clean_weather_data(raw_data):
    rows = []
    for raw in raw_data:
        clean_record = {
            "city": raw.get("city"),
            "temp_c": raw.get("temp_c"),
            "aqi": raw.get("air_quality")
        }
        rows.append(clean_record)
    df = pd.DataFrame(rows)
    df.dropna(inplace=True)
    return df
