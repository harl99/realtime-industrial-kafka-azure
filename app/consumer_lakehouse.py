import json
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaConsumer
from settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

BRONZE_PATH = Path("data_lake/bronze/industrial_sensor_events/events_raw.jsonl")
SILVER_PATH = Path("data_lake/silver/industrial_sensor_events/events_enriched.jsonl")

BRONZE_PATH.parent.mkdir(parents=True, exist_ok=True)
SILVER_PATH.parent.mkdir(parents=True, exist_ok=True)

def enrich_event(event):
    event["processed_at"] = datetime.now(timezone.utc).isoformat()

    if event["status"] == "critical":
        event["alert_required"] = True
        event["priority"] = "high"
    elif event["status"] == "warning":
        event["alert_required"] = True
        event["priority"] = "medium"
    else:
        event["alert_required"] = False
        event["priority"] = "low"

    event["temperature_band"] = "high" if event["temperature_c"] >= 90 else "medium" if event["temperature_c"] >= 75 else "normal"
    event["vibration_band"] = "high" if event["vibration_mm_s"] >= 9 else "medium" if event["vibration_mm_s"] >= 6 else "normal"
    event["energy_band"] = "high" if event["energy_kwh"] >= 60 else "medium" if event["energy_kwh"] >= 35 else "normal"

    return event

def append_jsonl(path, record):
    with path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(record) + "\n")

def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="industrial-lakehouse-consumer-group",
        value_deserializer=lambda value: json.loads(value.decode("utf-8"))
    )

    print(f"Consuming events from topic: {KAFKA_TOPIC}")

    for message in consumer:
        raw_event = message.value
        enriched_event = enrich_event(raw_event.copy())

        append_jsonl(BRONZE_PATH, raw_event)
        append_jsonl(SILVER_PATH, enriched_event)

        print(
            f"offset={message.offset} "
            f"machine={enriched_event['machine_id']} "
            f"status={enriched_event['status']} "
            f"priority={enriched_event['priority']}"
        )

if __name__ == "__main__":
    main()
