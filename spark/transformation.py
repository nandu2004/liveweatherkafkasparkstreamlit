# Weather data transformations

# WMO Weather interpretation codes
WMO_CODES = {
    0: "Clear sky",
    1: "Mostly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Depositing rime fog",
    51: "Light drizzle",
    53: "Moderate drizzle",
    55: "Dense drizzle",
    56: "Freezing drizzle",
    57: "Dense freezing drizzle",
    61: "Slight rain",
    63: "Moderate rain",
    65: "Heavy rain",
    66: "Freezing rain",
    67: "Heavy freezing rain",
    71: "Slight snowfall",
    73: "Moderate snowfall",
    75: "Heavy snowfall",
    77: "Snow grains",
    80: "Rain showers",
    81: "Moderate rain showers",
    82: "Violent rain showers",
    85: "Snow showers",
    86: "Heavy snow showers",
    95: "Thunderstorm",
    96: "Thunderstorm with hail",
    99: "Severe thunderstorm",
}


# ----------------------------
# Unit conversions
# ----------------------------

def celsius_to_fahrenheit(temp_c):
    if temp_c is None:
        return None
    return round(temp_c * 9 / 5 + 32, 1)


def kmh_to_mph(speed_kmh):
    if speed_kmh is None:
        return None
    return round(speed_kmh * 0.621371, 1)


# ----------------------------
# Weather description
# ----------------------------

def get_weather_description(code):
    if code is None:
        return "Unknown"
    return WMO_CODES.get(code, "Unknown")


# ----------------------------
# Alert classification
# ----------------------------

def classify_alert(temp_c, wind_speed_kmh, precipitation_mm, weather_code):
    
    # Severe
    if temp_c is not None and (temp_c > 45 or temp_c < -30):
        return "severe"
    if wind_speed_kmh is not None and wind_speed_kmh > 100:
        return "severe"
    if weather_code in [96, 99]:
        return "severe"

    # Warning
    if temp_c is not None and (temp_c > 40 or temp_c < -20):
        return "warning"
    if wind_speed_kmh is not None and wind_speed_kmh > 70:
        return "warning"
    if precipitation_mm is not None and precipitation_mm > 10:
        return "warning"
    if weather_code == 95:
        return "warning"

    # Advisory
    if temp_c is not None and (temp_c > 35 or temp_c < -10):
        return "advisory"
    if wind_speed_kmh is not None and wind_speed_kmh > 50:
        return "advisory"
    if precipitation_mm is not None and precipitation_mm > 5:
        return "advisory"

    return "normal"


# ----------------------------
# Alert message
# ----------------------------

def get_alert_message(city, alert_level, temp_c, wind_speed_kmh, precipitation_mm, weather_desc):

    parts = [f"{city}: {alert_level.upper()} - {weather_desc}"]

    if temp_c is not None:
        if temp_c > 35:
            parts.append(f"Extreme heat ({temp_c:.1f}°C)")
        elif temp_c < -10:
            parts.append(f"Extreme cold ({temp_c:.1f}°C)")

    if wind_speed_kmh is not None and wind_speed_kmh > 50:
        parts.append(f"High winds ({wind_speed_kmh:.1f} km/h)")

    if precipitation_mm is not None and precipitation_mm > 5:
        parts.append(f"Heavy precipitation ({precipitation_mm:.1f} mm)")

    return " | ".join(parts)


# ----------------------------
# Main transformation
# ----------------------------

def transform_record(record: dict) -> dict:

    temp_c = record.get("temperature_c")
    wind_kmh = record.get("wind_speed_kmh")
    precip_mm = record.get("precipitation_mm")
    code = record.get("weather_code")

    weather_desc = get_weather_description(code)
    alert_level = classify_alert(temp_c, wind_kmh, precip_mm, code)

    return {
        **record,
        "temperature_f": celsius_to_fahrenheit(temp_c),
        "apparent_temperature_f": celsius_to_fahrenheit(
            record.get("apparent_temperature_c")
        ),
        "wind_speed_mph": kmh_to_mph(wind_kmh),
        "wind_gusts_mph": kmh_to_mph(record.get("wind_gusts_kmh")),
        "weather_description": weather_desc,
        "alert_level": alert_level,
        "alert_message": get_alert_message(
            record.get("city", "Unknown"),
            alert_level,
            temp_c,
            wind_kmh,
            precip_mm,
            weather_desc,
        ),
    }