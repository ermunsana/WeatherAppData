import requests
from dotenv import load_dotenv
import os
import json
from datetime import datetime

load_dotenv()

api = os.getenv("api")

cities = [
    "Madrid", "Barcelona", "Valencia", "Seville", "Zaragoza", "Malaga", "Murcia", "Palma",
    "Las Palmas", "Bilbao", "Alicante", "Cordoba", "Valladolid", "Vigo", "Gijon", "Hospitalet de Llobregat",
    "A Coruna", "Vitoria-Gasteiz", "Granada", "Elche", "Oviedo", "Badalona", "Cartagena", "Terrassa",
    "Jerez de la Frontera", "Sabadell", "Mostoles", "Santa Cruz de Tenerife", "Pamplona", "Almeria", "San Sebastian",
    "Burgos", "Salamanca", "Albacete", "Getafe", "Logrono", "Huelva", "Badajoz", "Santander", "Leon", "Tarragona",
    "Cadiz", "Lleida", "Jaen", "Ourense", "Toledo", "Guadalajara", "Marbella", "Leganes", "Castellon de la Plana",
    "Algeciras", "Fuenlabrada", "Torrevieja", "Mataro", "Reus", "Benidorm", "Ferrol", "Lugo", "Aviles", "Cuenca",
    "Pontevedra", "Gandia", "Ceuta", "Melilla", "Lisbon", "Porto", "Coimbra", "Braga", "Aveiro", "Faro", "Paris",
    "Lyon", "Marseille", "Toulouse", "Nice", "Bordeaux", "Berlin", "Hamburg", "Munich", "Cologne", "Frankfurt",
    "Vienna", "Salzburg", "Brussels", "Antwerp", "Amsterdam", "Rotterdam", "The Hague", "Utrecht", "Copenhagen",
    "Aarhus", "Oslo", "Bergen", "Stockholm", "Gothenburg", "Helsinki", "Tallinn", "Riga", "Vilnius", "Warsaw",
    "Krakow", "Prague", "Budapest", "Dubrovnik"
]


def extract(city):
    try: 
        url = f"http://api.weatherapi.com/v1/current.json?key={api}&q={city}&aqi=yes"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return {
            "country": data["location"]["country"],
            "city": data["location"]["name"],
            "localtime": data["location"]["localtime"],
            "temp_c": data["current"]["temp_c"],
            "humidity": data["current"]["humidity"],
            "cloud": data["current"]["cloud"],
            "air_quality": data["current"]['air_quality']['us-epa-index']
            }
    except:
        return None
    

def get_cities():
    results = []
    for city in cities:
        city_data = extract(city)
        results.append(city_data)
    return results


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

data_dir = os.path.join(BASE_DIR, "data", "raw")
os.makedirs(data_dir, exist_ok=True)  

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
file_path = os.path.join(data_dir, f"raw_data_{timestamp}.json")

with open(file_path, "w", encoding="utf-8") as f:
    json.dump(get_cities(), f, indent=2)

