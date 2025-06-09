import pandas as pd
from pymongo import MongoClient
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_style("whitegrid")

def connect_to_mongo(uri="mongodb://localhost:27017", db_name="eventcast"):
    """Connect to MongoDB and return database object."""
    client = MongoClient(uri)
    db = client[db_name]
    return db

def fetch_collections(db):
    """Fetch data from relevant MongoDB collections and return DataFrames."""
    weather_data = list(db['weather'].find({}))
    city_data = list(db['cities'].find({}))
    events_data = list(db['event'].find({}))

    weather_df = pd.DataFrame(weather_data)
    city_df = pd.DataFrame(city_data)
    events_df = pd.DataFrame(events_data)

    for df in [weather_df, city_df, events_df]:
        if '_id' in df.columns:
            df.drop(columns=['_id'], inplace=True)
    return weather_df, city_df, events_df

def prepare_analysis_data(weather_df, city_df, events_df):
    """Merge city and weather data, aggregate interested events, and return analysis DataFrame."""
    merged_df = pd.merge(city_df, weather_df, on='city', how='inner')
    interested_events_df = events_df[events_df.get('interested', False) == True]

    # Check if city field exists in events
    if 'city' not in interested_events_df.columns:
        raise KeyError("Field 'city' not found in events collection. Please check your schema.")

    event_counts = interested_events_df.groupby('city').size().reset_index(name='interested_event_count')
    analysis_df = pd.merge(merged_df, event_counts, on='city', how='left')
    analysis_df['interested_event_count'] = analysis_df['interested_event_count'].fillna(0).astype(int)

    return analysis_df

def plot_city_temperatures(analysis_df):
    """Return a matplotlib figure for city temperatures bar plot."""
    fig, ax = plt.subplots(figsize=(12,6))
    sns.barplot(data=analysis_df.sort_values('temperature', ascending=False), x='city', y='temperature', ax=ax)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right')
    ax.set_title('City Temperatures')
    plt.tight_layout()
    return fig

def plot_interested_events_per_city(analysis_df):
    """Return a matplotlib figure for interested events count per city."""
    fig, ax = plt.subplots(figsize=(12,6))
    sns.barplot(data=analysis_df.sort_values('interested_event_count', ascending=False), x='city', y='interested_event_count', ax=ax)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right')
    ax.set_title('Number of Interested Events per City')
    plt.tight_layout()
    return fig

def plot_temp_vs_events(analysis_df):
    """Return a matplotlib figure for scatter plot temperature vs interested events."""
    fig, ax = plt.subplots(figsize=(8,6))
    sns.scatterplot(data=analysis_df, x='temperature', y='interested_event_count', hue='city', s=100, ax=ax)
    ax.set_title('Temperature vs Interested Events')
    plt.tight_layout()
    return fig
