import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

from settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC


def generate_sensor_event():
    machine_id = random.choice(["M-001", "M-002", "M-003", "M-004"])

    temperature = round(random.uniform(55, 105), 2)
    vibration = round(random.uniform(0.5, 12.0), 2)
    pressure = round(random.uniform(20, 90), 2)
    energy_kwh = round(random.uniform(10, 80), 2)

    if temperature > 95 or vibration > 9:
        status = "critical"
    elif temperature > 85 or vibration > 6:
        status = "warning"
    else:
        status = "normal"

    return {
        "event_id": f"{machine_id}-{int(time.time() * 1000)}",
        "machine_id": machine_id,
        "temperature_c": temperature,
        "vibration_mm_s": vibration,
        "pressure_bar": pressure,
        "energy_kwh": energy_kwh,
        "status": status,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8")
    )

    print(f"Producing events to topic: {KAFKA_TOPIC}")

    while True:
        event = generate_sensor_event()

        producer.send(
            KAFKA_TOPIC,
            key=event["machine_id"],
            value=event
        )

        producer.flush()
        print(event)

        time.sleep(1)


if __name__ == "__main__":
    main()
