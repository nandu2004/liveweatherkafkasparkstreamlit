import sys
import os

# Fix import path (so dashboard modules work)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
from streamlit_autorefresh import st_autorefresh

# DB functions
from dashboard.sqldatabase import (
    get_active_alerts,
    get_aggregate_stats,
    get_current_weather,
    get_weather_history,
)

# UI components
from dashboard.components import (
    render_alerts,
    render_comparison_bar,
    render_current_table,
    render_map,
    render_metric_cards,
    render_temperature_chart,
    render_wind_chart,
)

# -------------------------------
# PAGE CONFIG
# -------------------------------
st.set_page_config(
    page_title="Weather Streaming Dashboard",
    page_icon="🌦️",
    layout="wide",
)

# -------------------------------
# SIDEBAR
# -------------------------------
st.sidebar.title("Settings")

refresh_interval = st.sidebar.slider(
    "Refresh interval (seconds)",
    min_value=5,
    max_value=60,
    value=10,
)

# auto-refresh
st_autorefresh(interval=refresh_interval * 1000, key="auto_refresh")

use_fahrenheit = st.sidebar.toggle("Show Fahrenheit", value=False)

time_range = st.sidebar.selectbox(
    "History time range",
    options=[1, 3, 6, 12, 24],
    index=2,
    format_func=lambda x: f"{x} hours"
)

# -------------------------------
# LOAD DATA
# -------------------------------
current_df = get_current_weather()

available_cities = sorted(current_df["city"].tolist()) if not current_df.empty else []

selected_cities = st.sidebar.multiselect(
    "Filter cities",
    options=available_cities,
    default=available_cities
)

# -------------------------------
# HEADER
# -------------------------------
st.title("Real-Time Weather Streaming Dashboard")
st.caption("Open-Meteo API → Kafka → Spark Streaming → SQLite → Streamlit")

# -------------------------------
# FILTER DATA
# -------------------------------
if not current_df.empty and selected_cities:
    filtered_current = current_df[current_df["city"].isin(selected_cities)]
else:
    filtered_current = current_df

# -------------------------------
# KPI METRICS
# -------------------------------
stats = get_aggregate_stats()
render_metric_cards(stats, use_fahrenheit=use_fahrenheit)

st.divider()

# -------------------------------
# CURRENT WEATHER TABLE
# -------------------------------
st.subheader("Current Weather")
render_current_table(filtered_current, use_fahrenheit=use_fahrenheit)

st.divider()

# -------------------------------
# HISTORY DATA
# -------------------------------
history_df = get_weather_history(
    cities=selected_cities if selected_cities else None,
    hours=time_range
)

# -------------------------------
# CHARTS
# -------------------------------
col1, col2 = st.columns(2)

with col1:
    render_temperature_chart(history_df, use_fahrenheit=use_fahrenheit)

with col2:
    render_wind_chart(history_df, use_fahrenheit=use_fahrenheit)

col3, col4 = st.columns(2)

with col3:
    render_comparison_bar(filtered_current, use_fahrenheit=use_fahrenheit)

with col4:
    render_map(filtered_current)

st.divider()

# -------------------------------
# ALERTS
# -------------------------------
st.subheader("Weather Alerts")

alerts_df = get_active_alerts()

if selected_cities and not alerts_df.empty:
    alerts_df = alerts_df[alerts_df["city"].isin(selected_cities)]

render_alerts(alerts_df)