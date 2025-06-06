from kafka import KafkaConsumer, KafkaProducer
import json
import ast
import time
from datetime import datetime, timezone
from pysolar.solar import get_altitude, get_azimuth

consumer = KafkaConsumer(
    'cafe_metadata',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='sun-position-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def process_cafe_message(cafe):
    now_utc = datetime.now(timezone.utc)
    lat = cafe['lat']
    lon = cafe['lon']
    alt = get_altitude(lat, lon, now_utc)
    az = get_azimuth(lat, lon, now_utc)

    sun_data = {
        "timestamp": now_utc.isoformat() + "Z",
        "cafe_id": cafe['cafe_id'],
        "name": cafe.get('name'),
        "latitude": lat,
        "longitude": lon,
        "altitude_deg": round(alt, 2),
        "azimuth_deg": round(az, 2)
    }
    msg = json.dumps(sun_data)
    producer.send("sun_position_updates", value=msg)
    print(f"[sun_position_updates] {cafe['name']} – Sent sun info")

for message in consumer:
    cafe = ast.literal_eval(message.value)
    process_cafe_message(cafe)
    time.sleep(0.1)  # optional throttle