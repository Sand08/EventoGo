import json, os, time
from kafka import KafkaConsumer
import requests

DATA_FILE = "./data/german_cities_weather.json"
BOOTSTRAP_SERVERS = "localhost:9092"

# Clear file before new session
open(DATA_FILE, "w").close()

consumer = KafkaConsumer(
    "city_data",
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="weather-fetcher"
)

def fetch_weather(lat, lon):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": "temperature_2m,cloudcover,weathercode"
    }
    res = requests.get(url, params=params).json()
    return res.get("current", {})

while True:
    for msg in consumer:
        data = msg.value
        city = data["city"]
        lat, lon = data["lat"], data["lon"]
        weather = fetch_weather(lat, lon)

        with open(DATA_FILE, "a") as f:
            f.write(json.dumps({
                "city": city,
                "lat": lat,
                "lon": lon,
                "weather": weather
            }) + "\n")

        print(f"[weather_data] {city}: {weather}")
    
    time.sleep(300)  # wait 5 mins
    