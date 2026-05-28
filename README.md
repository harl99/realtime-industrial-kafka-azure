# Real-Time Industrial Kafka Pipeline

Portfolio-grade Data Engineering project that simulates an industrial real-time streaming pipeline using Kafka, Docker, Python, Medallion Architecture, and Streamlit.

## Business Problem

Industrial machines generate continuous sensor telemetry such as temperature, vibration, pressure, and energy consumption. Operations teams need a pipeline that can ingest events in real time, process abnormal conditions, organize raw and enriched data, and generate KPIs for monitoring equipment behavior.

## Architecture

Industrial Sensor Simulator
        ↓
Kafka Producer
        ↓
Kafka Broker running on Docker
        ↓
Kafka Topic: industrial_sensor_events
        ↓
Kafka Consumer Group
        ↓
Bronze Layer: raw events
        ↓
Silver Layer: enriched events
        ↓
Gold Layer: KPI dataset
        ↓
Streamlit Dashboard

## Tech Stack

- Python
- Apache Kafka
- Docker Compose
- Pandas
- Streamlit
- Plotly
- JSON Lines
- Medallion Architecture
- Azure-ready design

## How to Run

1. Start Kafka

docker compose up -d

2. Activate environment

source .venv/bin/activate

3. Run producer

python app/producer.py

4. Run lakehouse consumer

python app/consumer_lakehouse.py

5. Build Gold KPIs

python app/build_gold_layer.py

6. Run dashboard

streamlit run dashboard/app.py

Open:

http://localhost:8501

## Data Layers

Bronze:
Raw Kafka events stored as JSONL.

Silver:
Enriched events with alert priority, temperature band, vibration band, and energy band.

Gold:
Business-ready KPIs stored as CSV.

## KPIs Generated

- Total events
- Critical events
- Warning events
- Normal events
- Average temperature
- Average vibration
- Average energy consumption
- Top machine by event count
- Alert rate percentage

## Azure Migration Plan

This project was designed to be migrated to Azure.

Potential Azure architecture:

Kafka Producer
        ↓
Azure Event Hubs with Kafka-compatible endpoint
        ↓
Azure Data Lake Storage Gen2
        ↓
Azure Databricks / Synapse / Python transformations
        ↓
Power BI or Streamlit dashboard

## Interview Talking Points

I built a Kafka-based industrial streaming pipeline using Docker and Python. The system simulates real-time machine telemetry, publishes events into Kafka, consumes them with a consumer group, and stores the data using a Medallion-style architecture with Bronze, Silver, and Gold layers.

The Gold layer generates operational KPIs, and a Streamlit dashboard visualizes machine status, alerts, and sensor behavior.

This project helped me practice real Data Engineering concepts such as event streaming, producers, consumers, offsets, consumer groups, data lake design, batch KPI generation, and cloud-ready architecture for Azure.
