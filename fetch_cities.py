# fetch_cities.py
import json
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from geopy.distance import geodesic

GERMAN_CITIES_CSV = "german_cities.csv"

consumer = KafkaConsumer(
    "city_requests",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="city-fetcher"
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Load cities from file
df = pd.read_csv(GERMAN_CITIES_CSV)  # columns: city, lat, lon

print("[fetch_cities] Listening for requests...")
for msg in consumer:
    request = msg.value
    city = request["city"]
    range_km = request["range_km"]

    # Get user city coordinates
    user_row = df[df["city"].str.lower() == city.lower()]
    if user_row.empty:
        print(f"[fetch_cities] City '{city}' not found in dataset.")
        continue
    user_coords = (user_row.iloc[0]["lat"], user_row.iloc[0]["lon"])

    # Filter cities in range
    results = []
    for _, row in df.iterrows():
        target_coords = (row["lat"], row["lon"])
        distance = geodesic(user_coords, target_coords).km
        if distance <= range_km and row["city"].lower() != city.lower():
            results.append({
                "city": row["city"],
                "lat": row["lat"],
                "lon": row["lon"]
            })

    print(f"[fetch_cities] Found {len(results)} cities near {city} within {range_km} km")

    for city_info in results:
        producer.send("city_data", value=city_info)
        print(f"[city_data] Sent: {city_info}")

producer.flush()