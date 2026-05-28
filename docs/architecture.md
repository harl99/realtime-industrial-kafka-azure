# Architecture

## Current Local Architecture

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
Bronze Layer: raw JSONL events
        ↓
Silver Layer: enriched JSONL events
        ↓
Gold Layer: KPI CSV dataset
        ↓
Streamlit Dashboard

## Azure Migration Plan

Kafka Producer
        ↓
Azure Event Hubs with Kafka-compatible endpoint
        ↓
Azure Data Lake Storage Gen2
        ↓
Azure Databricks / Azure Functions / Python Consumer
        ↓
Silver and Gold transformations
        ↓
Power BI / Streamlit / Synapse Analytics

## Data Layers

- Bronze: raw events exactly as received from Kafka.
- Silver: enriched events with alert priority and sensor bands.
- Gold: business-ready KPIs for monitoring and analytics.

## Key Concepts Demonstrated

- Event streaming
- Kafka producer and consumer
- Kafka topics and offsets
- Consumer groups
- JSONL event storage
- Medallion architecture
- KPI generation
- Dashboard analytics
- Azure-ready cloud migration design
