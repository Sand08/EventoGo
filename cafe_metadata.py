#!/usr/bin/env python3
import json
import time
from kafka import KafkaProducer

# ── Step A: Replace this with the actual Overpass‐extracted JSON.
# Here’s a small sample to illustrate. You would populate this list with
# the full set of café features (id, name, lat, lon, orientation, geometry).
cafes = [
    {
        "cafe_id": "osm_node_1001",
        "name": "Café Rossi",
        "lat": 49.4100,
        "lon": 8.6910,
        "orientation_deg": 45,
        "geometry": [
            [8.6910, 49.4100],
            [8.6911, 49.4100],
            [8.6911, 49.4101],
            [8.6910, 49.4101]
        ]
    },
    {
        "cafe_id": "osm_way_1002",
        "name": "Mildner’s",
        "lat": 49.4095,
        "lon": 8.6920,
        "orientation_deg": 135,
        "geometry": [
            [8.6920, 49.4095],
            [8.6921, 49.4095],
            [8.6921, 49.4096],
            [8.6920, 49.4096]
        ]
    }
    # … add all Heidelberg cafés here …
]

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def produce_cafe_metadata():
    for cafe in cafes:
        message = json.dumps(cafe,ensure_ascii=False)  # ✅ Properly serialize to JSON
        producer.send("cafe_metadata", value=message)
        print(f"[cafe_metadata] Sent: {cafe['cafe_id']} – {cafe['name']}")
        time.sleep(0.1)

if __name__ == "__main__":
    produce_cafe_metadata()
    # Wait for all messages to be delivered before exit
    producer.flush()
    producer.close()