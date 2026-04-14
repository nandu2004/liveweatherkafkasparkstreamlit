import json
import logging
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Allow imports from project root
sys.path.insert(0, ".")
from spark.transformation import transform_record

# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ----------------------------
# Paths
# ----------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "data", "weather.db")
CHECKPOINT_DIR = os.path.join(PROJECT_ROOT, "data", "spark-checkpoints")

# ----------------------------
# Kafka Schema
# ----------------------------
KAFKA_SCHEMA = StructType([
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature_c", DoubleType(), True),
    StructField("humidity_pct", DoubleType(), True),
    StructField("apparent_temperature_c", DoubleType(), True),
    StructField("precipitation_mm", DoubleType(), True),
    StructField("wind_speed_kmh", DoubleType(), True),
    StructField("wind_gusts_kmh", DoubleType(), True),
    StructField("weather_code", IntegerType(), True),
    StructField("pressure_hpa", DoubleType(), True),
])

# ----------------------------
# DB Initialization
# ----------------------------
def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")

    # Current weather (latest snapshot)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS current_weather (
            city TEXT PRIMARY KEY,
            country TEXT,
            latitude REAL,
            longitude REAL,
            timestamp TEXT,
            temperature_c REAL,
            temperature_f REAL,
            humidity_pct REAL,
            apparent_temperature_c REAL,
            apparent_temperature_f REAL,
            precipitation_mm REAL,
            wind_speed_kmh REAL,
            wind_speed_mph REAL,
            wind_gusts_kmh REAL,
            wind_gusts_mph REAL,
            weather_code INTEGER,
            weather_description TEXT,
            pressure_hpa REAL,
            alert_level TEXT
        )
    """)

    # Full history
    conn.execute("""
        CREATE TABLE IF NOT EXISTS weather_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT,
            country TEXT,
            latitude REAL,
            longitude REAL,
            timestamp TEXT,
            temperature_c REAL,
            temperature_f REAL,
            humidity_pct REAL,
            apparent_temperature_c REAL,
            apparent_temperature_f REAL,
            precipitation_mm REAL,
            wind_speed_kmh REAL,
            wind_speed_mph REAL,
            wind_gusts_kmh REAL,
            wind_gusts_mph REAL,
            weather_code INTEGER,
            weather_description TEXT,
            pressure_hpa REAL,
            alert_level TEXT
        )
    """)

    # Alerts table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS weather_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT,
            timestamp TEXT,
            alert_level TEXT,
            alert_message TEXT
        )
    """)

    # Indexes
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_history_city_ts
        ON weather_history(city, timestamp)
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_alerts_city_ts
        ON weather_alerts(city, timestamp)
    """)

    conn.commit()
    conn.close()

    logger.info(f"SQLite database initialized at {DB_PATH}")


# ----------------------------
# Cleanup old data
# ----------------------------
def cleanup_old_data(conn: sqlite3.Connection):
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()

    conn.execute(
        "DELETE FROM weather_history WHERE timestamp < ?", (cutoff,)
    )
    conn.execute(
        "DELETE FROM weather_alerts WHERE timestamp < ?", (cutoff,)
    )


# ----------------------------
# Write batch
# ----------------------------
def write_batch_to_sqlite(batch_df: DataFrame, batch_id: int):
    if batch_df.isEmpty():
        return

    rows = batch_df.collect()
    logger.info(f"Processing batch {batch_id} with {len(rows)} rows")

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")

    try:
        for row in rows:
            record = row.asDict()
            transformed = transform_record(record)

            # ----------------------
            # UPSERT current_weather
            # ----------------------
            conn.execute("""
                INSERT OR REPLACE INTO current_weather (
                    city, country, latitude, longitude, timestamp,
                    temperature_c, temperature_f, humidity_pct,
                    apparent_temperature_c, apparent_temperature_f,
                    precipitation_mm, wind_speed_kmh, wind_speed_mph,
                    wind_gusts_kmh, wind_gusts_mph,
                    weather_code, weather_description,
                    pressure_hpa, alert_level
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                transformed["city"],
                transformed["country"],
                transformed["latitude"],
                transformed["longitude"],
                transformed["timestamp"],
                transformed["temperature_c"],
                transformed["temperature_f"],
                transformed["humidity_pct"],
                transformed["apparent_temperature_c"],
                transformed["apparent_temperature_f"],
                transformed["precipitation_mm"],
                transformed["wind_speed_kmh"],
                transformed["wind_speed_mph"],
                transformed["wind_gusts_kmh"],
                transformed["wind_gusts_mph"],
                transformed["weather_code"],
                transformed["weather_description"],
                transformed["pressure_hpa"],
                transformed["alert_level"],
            ))

            # ----------------------
            # Insert history
            # ----------------------
            conn.execute("""
                INSERT INTO weather_history (
                    city, country, latitude, longitude, timestamp,
                    temperature_c, temperature_f, humidity_pct,
                    apparent_temperature_c, apparent_temperature_f,
                    precipitation_mm, wind_speed_kmh, wind_speed_mph,
                    wind_gusts_kmh, wind_gusts_mph,
                    weather_code, weather_description,
                    pressure_hpa, alert_level
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                transformed["city"],
                transformed["country"],
                transformed["latitude"],
                transformed["longitude"],
                transformed["timestamp"],
                transformed["temperature_c"],
                transformed["temperature_f"],
                transformed["humidity_pct"],
                transformed["apparent_temperature_c"],
                transformed["apparent_temperature_f"],
                transformed["precipitation_mm"],
                transformed["wind_speed_kmh"],
                transformed["wind_speed_mph"],
                transformed["wind_gusts_kmh"],
                transformed["wind_gusts_mph"],
                transformed["weather_code"],
                transformed["weather_description"],
                transformed["pressure_hpa"],
                transformed["alert_level"],
            ))

            # ----------------------
            # Insert alerts
            # ----------------------
            if transformed["alert_level"] != "normal" and transformed.get("alert_message"):
                conn.execute("""
                    INSERT INTO weather_alerts (
                        city, timestamp, alert_level, alert_message
                    ) VALUES (?, ?, ?, ?)
                """, (
                    transformed["city"],
                    transformed["timestamp"],
                    transformed["alert_level"],
                    transformed["alert_message"],
                ))

        cleanup_old_data(conn)

        conn.commit()
        logger.info(f"Batch {batch_id} written successfully")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error writing batch {batch_id}: {e}")
        raise

    finally:
        conn.close()


# ----------------------------
# Main Spark Job
# ----------------------------
def main():
    init_db()

    spark = (
        SparkSession.builder
        .appName("WeatherStreaming")
        .master("local[*]")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    logger.info("Reading from Kafka topic 'raw-weather'...")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "raw-weather")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), KAFKA_SCHEMA).alias("data"))
        .select("data.*")
    )

    query = (
        parsed_df.writeStream
        .foreachBatch(write_batch_to_sqlite)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="30 seconds")
        .start()
    )

    logger.info("Streaming query started...")
    query.awaitTermination()


# ----------------------------
# Entry
# ----------------------------
if __name__ == "__main__":
    main()