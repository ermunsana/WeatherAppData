import requests
import os
from dotenv import load_dotenv
from cities import cities

load_dotenv()
weahther_api_key = os.getenv('weatherapi')


def get_weather(city):
    baseurl = "http://api.weatherapi.com/v1"
    url = f"{baseurl}/current.json?key={weahther_api_key}&q={city}&aqi=yes"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return {
            "city": data["location"]["name"],
            "timestamp": data["location"]["localtime"],
            "temp_c": data["current"]["temp_c"],
            "air_quality": data["current"]['air_quality']['us-epa-index']
        }
    else:
        print("Error. Bad response from weather API.")
        return None




def fetch_all_cities():
    records = []
    for city in cities:
        record = get_weather(city)
        if record:
            records.append(record)
    return records













