# Real-Time Weather Streaming Dashboard
A simple real-time data pipeline that streams weather data, processes it, stores it, and visualizes it in a live dashboard.

## Overview
This project connects multiple components into a working pipeline:

Weather API → Kafka → Spark Streaming → SQLite → Streamlit

* Producer fetches weather data and sends it to Kafka
* Spark consumes and processes the stream
* Processed data is written to SQLite
* Streamlit reads from the database and renders a live dashboard

## Project Structure
```
dashboard/
  components.py
  sqldatabase.py
  streamlitdash.py

producer/
  config.py
  weatherproducer.py

spark/
  sparkconsumer.py
  transformation.py

data/
  weather.db
```


## Setup

### 1. Install dependencies
```
pip install -r requirement.txt
```



### 2. Start Kafka and Zookeeper
```
docker-compose up
```

---

### 3. Run Producer
```
python -m producer.weatherproducer
```

### 4. Run Spark Consumer
```
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
spark/sparkconsumer.py
```



### 5. Run Dashboard

```
streamlit run dashboard/streamlitdash.py
```



## Output

### Dashboard
<img width="1900" height="813" alt="Screenshot 2026-04-15 120340" src="https://github.com/user-attachments/assets/b1356409-6470-4a97-963b-e937a4cdfcd2" />
<img width="1886" height="812" alt="Screenshot 2026-04-15 120414" src="https://github.com/user-attachments/assets/fe0bcca0-afbb-4fd2-88aa-0a2dae50424c" />


### Streaming Logs (Producer + Spark)


<img width="983" height="616" alt="Screenshot 2026-04-15 120652" src="https://github.com/user-attachments/assets/4c5e8ff4-dcad-422f-9eda-5c3e8b9a8c17" />

<img width="1028" height="647" alt="Screenshot 2026-04-15 120710" src="https://github.com/user-attachments/assets/0df54efc-7a32-4084-a7c5-744a72de074c" />

## Features

* Real-time streaming pipeline
* Multi-city weather tracking
* Historical data visualization
* Alerts system (basic)
* Interactive dashboard (filters, time range)


## Issues Faced

### 1. Spark setup issues
* Spark failed initially due to:

  * Java version mismatch
  * Missing `winutils.exe`
  * Python path not detected

* These were not code issues, but environment issues.

**What this clarified:**
* Spark is highly dependent on system setup
* Understanding how Spark connects Python, Java, and Hadoop matters more than just writing code

### 2 Kafka + Spark integration confusion
* Figuring out how Spark reads from Kafka took time
* Understanding batch processing vs real-time streaming was not intuitive at first

**What this clarified:**
* Streaming is not continuous in the way it seems
* It operates in micro-batches



## What this project helped with
* Understanding how real-time pipelines are actually wired
* Seeing how data flows across systems, not just within one script
* Learning how much of data engineering is setup and debugging, not just logic
* Getting comfortable with Spark beyond surface-level usage

