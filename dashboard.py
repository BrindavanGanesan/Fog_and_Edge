import streamlit as st, pandas as pd, threading, time, asyncio
from iot_listener import telemetry_queue, main
import plotly.graph_objects as go

# ✅ Page Configuration
st.set_page_config(page_title="NASA Asteroid & Weather Dashboard", layout="wide", page_icon="🛰️")

# ✅ Dark Mode CSS
st.markdown("""
    <style>
    body { background-color: #0E1117; color: #FAFAFA; }
    .stDataFrame, .stMarkdown, .stMetric { background-color: #1E1E1E; }
    h1, h2, h3 { color: #00BFFF; }
    </style>
""", unsafe_allow_html=True)

# ✅ Dashboard Header
st.markdown("<h1 style='text-align: center;'>🛰️ Real-Time NASA Asteroid & Weather Monitoring</h1>", unsafe_allow_html=True)
st.markdown("---")

data_buffer = []
placeholder = st.empty()

# ✅ Start Async IoT Listener
def start_async_listener():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())

threading.Thread(target=start_async_listener, daemon=True).start()

# ✅ Dashboard Loop
while True:
    if not telemetry_queue.empty():
        msg = telemetry_queue.get()

        # 🔹 Handle list of asteroids
        if isinstance(msg, list):
            for asteroid in msg:
                asteroid["timestamp"] = asteroid.get("timestamp", time.time())
                data_buffer.append(asteroid)

        # 🔹 Handle single telemetry dict (weather or asteroid)
        elif isinstance(msg, dict):
            msg["timestamp"] = msg.get("timestamp", time.time())
            data_buffer.append(msg)

        # 🔹 Keep last 500 messages
        data_buffer = data_buffer[-500:]
        df = pd.DataFrame(data_buffer)

        # ✅ Separate asteroid and weather data safely
        if "object_name" in df.columns:
            asteroid_df = df[df["object_name"].notna()].copy()
        else:
            asteroid_df = pd.DataFrame()

        if "temperature" in df.columns:
            weather_df = df[df["temperature"].notna()].copy()
        else:
            weather_df = pd.DataFrame()

        with placeholder.container():
            cols = st.columns(2)

            # ✅ Asteroid Section
            with cols[0]:
                st.markdown("## ☄️ Asteroid Telemetry")
                if not asteroid_df.empty:
                    asteroid_df.sort_values("timestamp", ascending=False, inplace=True)

                    latest = asteroid_df.iloc[0]
                    obj = latest.get("object_name", "Unknown")
                    dist = latest.get("distance_km", 0)
                    vel = latest.get("velocity_kmh", 0)
                    alert = latest.get("alert", "SAFE")

                    k1, k2, k3 = st.columns(3)
                    k1.metric("🚀 Latest Asteroid", obj)
                    k2.metric("📏 Distance (km)", f"{dist:,.2f}")
                    k3.metric("🚨 Alert", alert)

                    # ✅ Distance Gauge
                    gauge = go.Figure(go.Indicator(
                        mode="gauge+number",
                        value=dist,
                        title={'text': "Closest Approach"},
                        gauge={
                            'axis': {'range': [0, 10000000]},
                            'bar': {'color': "red" if dist < 1000000 else "limegreen"},
                            'steps': [
                                {'range': [0, 1000000], 'color': "red"},
                                {'range': [1000000, 3000000], 'color': "yellow"},
                                {'range': [3000000, 10000000], 'color': "green"}
                            ],
                        }
                    ))
                    st.plotly_chart(gauge, use_container_width=True, key=f"gauge_{time.time()}")

                    # ✅ Velocity Trend
                    st.markdown("### 📈 Velocity Trend (last 50)")
                    if "velocity_kmh" in asteroid_df.columns:
                        vel_df = asteroid_df.set_index("timestamp")[["velocity_kmh"]].tail(50)
                        st.line_chart(vel_df)

                    # ✅ Top 5 Closest Asteroids
                    st.markdown("### 📋 Top 5 Closest Asteroids")
                    top5 = asteroid_df.sort_values("distance_km").head(5)
                    st.dataframe(top5[["timestamp", "object_name", "distance_km", "velocity_kmh", "alert"]],
                                 use_container_width=True)

                    # ✅ Alerts
                    if dist < 1000000:
                        st.error(f"🔥 CRITICAL: {obj} within 1,000,000 km!")
                    elif dist < 3000000:
                        st.warning(f"⚠️ WARNING: {obj} within 3,000,000 km!")
                else:
                    st.info("Waiting for asteroid telemetry...")

            # ✅ Weather Section
            with cols[1]:
                st.markdown("## 🌡️ Weather Telemetry")
                if not weather_df.empty:
                    st.success("🌦️ Detected Weather Telemetry")
                    weather_df.sort_values("timestamp", ascending=False, inplace=True)

                    latest = weather_df.iloc[0]
                    temp = latest.get("temperature", 0)
                    hum = latest.get("humidity", 0)
                    alert = latest.get("alert", "NORMAL")

                    k1, k2, k3 = st.columns(3)
                    k1.metric("🌡️ Temperature (°C)", f"{temp:.2f}")
                    k2.metric("💧 Humidity (%)", f"{hum:.2f}")
                    k3.metric("🚨 Alert", alert)

                    # ✅ Temperature Trend
                    st.markdown("### 📈 Temperature Trend (last 50)")
                    temp_df = weather_df.set_index("timestamp")[["temperature"]].tail(50)
                    st.line_chart(temp_df)

                    # ✅ Humidity Trend
                    st.markdown("### 📈 Humidity Trend (last 50)")
                    hum_df = weather_df.set_index("timestamp")[["humidity"]].tail(50)
                    st.line_chart(hum_df)
                else:
                    st.info("Waiting for weather telemetry...")

    time.sleep(5)
