#!/usr/bin/env python3
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, expr, broadcast
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# ── Step A: Define schemas
cafe_schema = StructType([
    StructField("cafe_id", StringType()),
    StructField("name", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("orientation_deg", DoubleType()),
    StructField("geometry", StringType(), nullable=True)
])

sun_schema = StructType([
    StructField("timestamp", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("altitude_deg", DoubleType()),
    StructField("azimuth_deg", DoubleType())
])

weather_schema = StructType([
    StructField("timestamp", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("temperature_C", DoubleType()),
    StructField("cloud_cover_pct", DoubleType()),
    StructField("visibility_m", DoubleType()),
    StructField("weather_main", StringType()),
    StructField("weather_desc", StringType())
])

# ── Step B: Spark session
spark = (
    SparkSession.builder
    .appName("SunlightStatusProcessor")
    .master("local[*]")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── Step C: Load cafe metadata (batch read, static)
cafes_raw = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "cafe_metadata")
    .option("startingOffsets", "earliest")
    .load()
)

cafes_df = (
    cafes_raw.selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), cafe_schema).alias("data"))
    .select("data.*")
    .dropDuplicates(["cafe_id"])
)
cafes_df = broadcast(cafes_df)

# ── Step D: Streaming sun position
sun_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "sun_position_updates")
    .option("startingOffsets", "latest")
    .load()
)

sun_df = (
    sun_stream.selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), sun_schema).alias("data"))
    .select(
        col("data.timestamp").alias("sun_ts"),
        col("data.azimuth_deg"),
        col("data.altitude_deg")
    )
)

# ── Step E: Streaming weather
weather_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "weather_updates")
    .option("startingOffsets", "latest")
    .load()
)

weather_df = (
    weather_stream.selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), weather_schema).alias("data"))
    .select(
        col("data.timestamp").alias("weather_ts"),
        col("data.cloud_cover_pct")
    )
)

# ── Step F: UDF to decide sun/shade
def sun_or_shade(orient_deg, azimuth_deg, cloud_pct):
    if cloud_pct is None:
        return "UNKNOWN"
    if cloud_pct > 70:
        return "☁️ shade"
    diff = abs(orient_deg - azimuth_deg) % 360
    if diff > 180:
        diff = 360 - diff
    return "☀️ full sun" if diff < 45 else "🌥️ partial"

sunshade_udf = udf(sun_or_shade, StringType())

# ── Step G: Cross join sun + weather (latest)
latest_sun = sun_df \
    .withWatermark("sun_ts", "1 minute") \
    .groupBy() \
    .agg(
        expr("last(azimuth_deg)").alias("azimuth_deg"),
        expr("last(altitude_deg)").alias("altitude_deg"),
        expr("last(sun_ts)").alias("sun_ts")
    )

latest_weather = weather_df \
    .withWatermark("weather_ts", "2 minutes") \
    .groupBy() \
    .agg(
        expr("last(cloud_cover_pct)").alias("cloud_pct"),
        expr("last(weather_ts)").alias("weather_ts")
    )

context = latest_sun.crossJoin(latest_weather)
cafes_with_ctx = cafes_df.crossJoin(context)

# ── Step H: Calculate sunlight status
cafes_status = cafes_with_ctx.withColumn(
    "sun_status",
    sunshade_udf(col("orientation_deg"), col("azimuth_deg"), col("cloud_pct"))
).select(
    col("cafe_id"),
    col("name"),
    col("lat"),
    col("lon"),
    col("sun_status"),
    col("sun_ts").alias("timestamp"),
    col("cloud_pct")
)

# ── Step I: Send to Kafka
query = (
    cafes_status
    .selectExpr("CAST(cafe_id AS STRING) AS key", "to_json(struct(*)) AS value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "cafe_sunlight_status")
    .option("checkpointLocation", "/tmp/spark-checkpoints/sunlight_status")
    .outputMode("update")
    .start()
)

print("✅ Streaming job started. Press Ctrl+C to stop.")
query.awaitTermination()
