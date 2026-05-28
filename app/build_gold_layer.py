import pandas as pd
from pathlib import Path

SILVER_PATH = Path(
    "data_lake/silver/industrial_sensor_events/events_enriched.jsonl"
)

GOLD_OUTPUT = Path(
    "data_lake/gold/kpis/industrial_kpis.csv"
)

GOLD_OUTPUT.parent.mkdir(parents=True, exist_ok=True)

df = pd.read_json(SILVER_PATH, lines=True)

total_events = len(df)

critical_events = len(df[df["status"] == "critical"])
warning_events = len(df[df["status"] == "warning"])
normal_events = len(df[df["status"] == "normal"])

avg_temperature = round(df["temperature_c"].mean(), 2)
avg_vibration = round(df["vibration_mm_s"].mean(), 2)
avg_energy = round(df["energy_kwh"].mean(), 2)

top_machine = (
    df["machine_id"]
    .value_counts()
    .idxmax()
)

alert_rate = round(
    ((critical_events + warning_events) / total_events) * 100,
    2
)

kpis = pd.DataFrame([
    {
        "total_events": total_events,
        "critical_events": critical_events,
        "warning_events": warning_events,
        "normal_events": normal_events,
        "avg_temperature_c": avg_temperature,
        "avg_vibration_mm_s": avg_vibration,
        "avg_energy_kwh": avg_energy,
        "top_machine_by_events": top_machine,
        "alert_rate_percent": alert_rate
    }
])

kpis.to_csv(GOLD_OUTPUT, index=False)

print("\n===== INDUSTRIAL STREAMING KPIs =====\n")
print(kpis.to_string(index=False))

print(f"\nSaved KPIs to: {GOLD_OUTPUT}")
