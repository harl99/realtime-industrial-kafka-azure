from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st


SILVER_PATH = Path("data_lake/silver/industrial_sensor_events/events_enriched.jsonl")
GOLD_PATH = Path("data_lake/gold/kpis/industrial_kpis.csv")

st.set_page_config(page_title="Industrial Kafka Monitoring", layout="wide")

st.title("Industrial Kafka Real-Time Monitoring Dashboard")
st.caption("Kafka → Bronze → Silver → Gold → Analytics")

if not SILVER_PATH.exists():
    st.error("Silver data not found. Run the Kafka consumer first.")
    st.stop()

df = pd.read_json(SILVER_PATH, lines=True)

if GOLD_PATH.exists():
    kpis = pd.read_csv(GOLD_PATH)
    row = kpis.iloc[0]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Events", int(row["total_events"]))
    c2.metric("Critical Events", int(row["critical_events"]))
    c3.metric("Warning Events", int(row["warning_events"]))
    c4.metric("Alert Rate", f'{row["alert_rate_percent"]}%')
else:
    st.warning("Gold KPIs not found. Run app/build_gold_layer.py first.")

st.subheader("Event Status Distribution")
status_counts = df["status"].value_counts().reset_index()
status_counts.columns = ["status", "count"]
st.plotly_chart(px.bar(status_counts, x="status", y="count"), use_container_width=True)

st.subheader("Events by Machine")
machine_counts = df["machine_id"].value_counts().reset_index()
machine_counts.columns = ["machine_id", "count"]
st.plotly_chart(px.bar(machine_counts, x="machine_id", y="count"), use_container_width=True)

st.subheader("Temperature vs Vibration")
fig_scatter = px.scatter(
    df,
    x="temperature_c",
    y="vibration_mm_s",
    color="status",
    hover_data=["machine_id", "priority", "energy_kwh"],
)
st.plotly_chart(fig_scatter, use_container_width=True)

st.subheader("Recent Events")
st.dataframe(df.tail(50), use_container_width=True)
