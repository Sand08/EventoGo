# streamlit_app.py

import streamlit as st
import pandas as pd
import folium
import json
import time
import os
from streamlit_folium import st_folium
from kafka import KafkaProducer
import requests

# ------------- SETTINGS -------------
KAFKA_TOPIC = "city_requests"
DATA_FILE = "./data/german_cities_weather.json"
BOOTSTRAP_SERVERS = "localhost:9092"
WAIT_SECONDS = 30
TICKETMASTER_API_KEY = "bN9mOHps6ZdE1nIioUAogmd4GnmtxrFs"  # Replace with your actual API key

# -------------- FUNCTIONS --------------
def fetch_events(city_name):
    url = "https://app.ticketmaster.com/discovery/v2/events.json"
    params = {
        "apikey": TICKETMASTER_API_KEY,
        "city": city_name,
        "size": 5,
        "sort": "date,asc"
    }
    response = requests.get(url, params=params).json()
    events = []
    for evt in response.get("_embedded", {}).get("events", []):
        name = evt["name"]
        url = evt["url"]
        date = evt["dates"]["start"].get("localDate", "N/A")
        venue = evt["_embedded"]["venues"][0].get("name", "Unknown Venue")
        events.append({
            "name": name,
            "url": url,
            "date": date,
            "venue": venue
        })
    return events

# ------------- STREAMLIT UI -------------
st.set_page_config(layout="wide")
st.title("🌤️ German City Weather + Events Explorer")

city = st.text_input("Enter your base city (e.g. Heidelberg):")
radius = st.slider("Select range (km)", 10, 300, 100)

weather_pref = st.radio(
    "Preferred Weather",
    ["Any", "Sunny", "Cloudy", "Rainy"],
    horizontal=True
)

include_events = st.checkbox("🎪 Include Events (beta)", value=False)

if "show_map" not in st.session_state:
    st.session_state["show_map"] = False

if st.button("Show Weather Map"):
    if not city:
        st.warning("Please enter a valid city.")
        st.stop()

    st.session_state["show_map"] = False

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    payload = {"city": city, "range_km": radius}
    producer.send(KAFKA_TOPIC, value=payload)
    producer.flush()
    producer.close()
    st.success(f"📤 Request sent for {city} within {radius} km")

    st.info("⏳ Waiting for weather data...")
    start_time = time.time()
    while not os.path.exists(DATA_FILE):
        if time.time() - start_time > WAIT_SECONDS:
            st.error("❌ Timed out waiting for weather data.")
            st.stop()
        time.sleep(2)

    st.session_state["show_map"] = True

if st.session_state["show_map"]:
    with open(DATA_FILE) as f:
        records = [json.loads(line) for line in f.readlines()]
    df = pd.DataFrame(records)

    if weather_pref != "Any":
        condition_codes = {
            "Sunny": [0, 1],
            "Cloudy": [2, 3],
            "Rainy": [51, 53, 61, 63, 65, 80, 81, 82]
        }
        codes = condition_codes.get(weather_pref, [])
        df = df[df["weather"].apply(lambda w: w.get("weathercode") in codes)]
        st.info(f"✅ Filtered cities by {weather_pref} preference.")

    if df.empty:
        st.warning("No matching cities found with that weather preference.")
    else:
        m = folium.Map(location=[df["lat"].mean(), df["lon"].mean()], zoom_start=7)
        for _, row in df.iterrows():
            popup = f"""
            <b>{row['city']}</b><br>
            🌡 {row['weather'].get('temperature_2m', 'N/A')}°C<br>
            ☁️ Cloudcover: {row['weather'].get('cloudcover', 'N/A')}%
            """
            folium.Marker([row["lat"], row["lon"]], popup=popup).add_to(m)

        st_folium(m, height=600, width=1000)

        if include_events:
            st.markdown("## 🎟 Upcoming Events")
            for _, row in df.iterrows():
                st.markdown(f"### 📍 {row['city']}")
                events = fetch_events(row["city"])
                if not events:
                    st.write("No events found.")
                    continue
                for event in events:
                    st.markdown(f"- **[{event['name']}]({event['url']})** – 🗓 {event['date']} at 🏟 {event['venue']}")
