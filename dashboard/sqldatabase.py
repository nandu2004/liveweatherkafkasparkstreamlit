import os
import sqlite3
import pandas as pd


# -------------------------------
# PATH SETUP
# -------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "data", "weather.db")


# -------------------------------
# CONNECTION (READ-ONLY)
# -------------------------------
def _get_connection() -> sqlite3.Connection:
    """Get a read-only SQLite connection (safe with Spark writing)."""
    if not os.path.exists(DB_PATH):
        return None

    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.execute("PRAGMA busy_timeout=3000")
    return conn


# -------------------------------
# CURRENT WEATHER
# -------------------------------
def get_current_weather() -> pd.DataFrame:
    """Get latest weather for all cities."""
    conn = _get_connection()
    if conn is None:
        return pd.DataFrame()

    try:
        df = pd.read_sql_query(
            "SELECT * FROM current_weather ORDER BY city",
            conn
        )
        return df
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


# -------------------------------
# WEATHER HISTORY
# -------------------------------
def get_weather_history(cities: list[str] = None, hours: int = 6) -> pd.DataFrame:
    """Get weather history with optional city filter and time range."""
    conn = _get_connection()
    if conn is None:
        return pd.DataFrame()

    try:
        query = """
            SELECT * FROM weather_history
            WHERE timestamp >= datetime('now', ? || ' hours')
        """

        params = [f"-{hours}"]

        if cities:
            placeholders = ",".join("?" for _ in cities)
            query += f" AND city IN ({placeholders})"
            params.extend(cities)

        query += " ORDER BY timestamp ASC"

        df = pd.read_sql_query(query, conn, params=params)
        return df

    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


# -------------------------------
# ACTIVE ALERTS
# -------------------------------
def get_active_alerts() -> pd.DataFrame:
    """Get recent weather alerts (last 6 hours)."""
    conn = _get_connection()
    if conn is None:
        return pd.DataFrame()

    try:
        df = pd.read_sql_query(
            """
            SELECT * FROM weather_alerts
            WHERE timestamp >= datetime('now', '-6 hours')
            ORDER BY timestamp DESC
            """,
            conn
        )
        return df

    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


# -------------------------------
# AGGREGATE STATS
# -------------------------------
def get_aggregate_stats() -> dict:
    """Get overall statistics for dashboard."""
    conn = _get_connection()
    if conn is None:
        return {}

    try:
        row = conn.execute(
            """
            SELECT
                COUNT(*) as city_count,
                ROUND(AVG(temperature_c), 1) as avg_temp_c,
                ROUND(AVG(temperature_f), 1) as avg_temp_f,
                ROUND(MAX(wind_speed_kmh), 1) as max_wind_kmh,
                ROUND(MAX(wind_speed_mph), 1) as max_wind_mph,
                SUM(CASE WHEN alert_level != 'normal' THEN 1 ELSE 0 END) as active_alerts
            FROM current_weather
            """
        ).fetchone()

        history_count = conn.execute(
            "SELECT COUNT(*) FROM weather_history"
        ).fetchone()[0]

        return {
            "city_count": row[0],
            "avg_temp_c": row[1],
            "avg_temp_f": row[2],
            "max_wind_kmh": row[3],
            "max_wind_mph": row[4],
            "active_alerts": row[5] or 0,
            "data_points": history_count,
        }

    except Exception:
        return {}
    finally:
        conn.close()