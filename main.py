import requests
import os
from dotenv import load_dotenv
import sqlite3

load_dotenv()
weahther_api_key = os.getenv('weatherapi')

con = sqlite3.connect('weather_data.db')
cur = con.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS weather (city, temperature, aqi)")


city = input("Enter city name: ")

def get_weather(city):
    baseurl = "http://api.weatherapi.com/v1"
    url = f"{baseurl}/current.json?key={weahther_api_key}&q={city}&aqi=yes"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        temp = data['current']['temp_c']
        aqi = data['current']['air_quality']['us-epa-index']
        cur.execute("INSERT INTO weather (city, temperature, aqi) VALUES (?, ?, ?)", (city, temp, aqi))
        con.commit()
        print(f"City: {city}, Temperature: {temp}°C, AQI: {aqi}")
    else:
        print("Error. Bad response from weather API.")


get_weather(city)

# Select
# for row in cur.execute("SELECT * FROM weather"):
#    print(row)
    







