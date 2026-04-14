import json
import logging
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from producer.config import (
    CITIES,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    OPEN_METEO_BASE_URL,
    OPEN_METEO_PARAMS,
    POLL_INTERVAL_SECONDS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)


# ✅ Create Kafka producer with retry
def create_producer(max_retries=30, retry_delay=5):
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
            )
            logger.info("Connected to Kafka")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"Kafka not ready (attempt {attempt}), retrying...")
            time.sleep(retry_delay)

    raise RuntimeError("Could not connect to Kafka")


# ✅ Fetch weather
def fetch_weather():
    latitudes = ",".join(str(c["latitude"]) for c in CITIES)
    longitudes = ",".join(str(c["longitude"]) for c in CITIES)

    params = {
        "latitude": latitudes,
        "longitude": longitudes,
        "current": ",".join(OPEN_METEO_PARAMS),
        "timezone": "auto",
    }

    response = requests.get(OPEN_METEO_BASE_URL, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()

    if not isinstance(data, list):
        data = [data]

    results = []
    fetch_time = datetime.now(timezone.utc).isoformat()

    for i, city in enumerate(CITIES):
        current = data[i].get("current", {})

        results.append({
            "city": city["name"],
            "country": city["country"],
            "latitude": city["latitude"],
            "longitude": city["longitude"],
            "timestamp": fetch_time,
            "temperature_c": current.get("temperature_2m"),
            "humidity_pct": current.get("relative_humidity_2m"),
            "apparent_temperature_c": current.get("apparent_temperature"),
            "precipitation_mm": current.get("precipitation"),
            "wind_speed_kmh": current.get("wind_speed_10m"),
            "wind_gusts_kmh": current.get("wind_gusts_10m"),
            "weather_code": current.get("weather_code"),
            "pressure_hpa": current.get("surface_pressure"),
        })

    return results


# ✅ Main loop
def run():
    producer = create_producer()

    logger.info(f"Starting producer (interval={POLL_INTERVAL_SECONDS}s)")

    while True:
        try:
            records = fetch_weather()

            for record in records:
                producer.send(
                    KAFKA_TOPIC,
                    key=record["city"],
                    value=record,
                )

            producer.flush()
            logger.info(f"Sent {len(records)} records")

        except Exception as e:
            logger.error(f"Error: {e}")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run()