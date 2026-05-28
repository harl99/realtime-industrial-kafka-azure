import json
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaConsumer

from settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC


OUTPUT_PATH = Path("data/processed/sensor_events_processed.jsonl")
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)


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

    return event


def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="industrial-monitoring-consumer-group",
        value_deserializer=lambda value: json.loads(value.decode("utf-8"))
    )

    print(f"Consuming events from topic: {KAFKA_TOPIC}")

    for message in consumer:
        event = message.value
        enriched_event = enrich_event(event)

        with OUTPUT_PATH.open("a", encoding="utf-8") as file:
            file.write(json.dumps(enriched_event) + "\n")

        print(
            f"offset={message.offset} "
            f"machine={enriched_event['machine_id']} "
            f"status={enriched_event['status']} "
            f"priority={enriched_event['priority']}"
        )


if __name__ == "__main__":
    main()
