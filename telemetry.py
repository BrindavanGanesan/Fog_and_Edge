import time, json, csv, requests, sys, random
from datetime import datetime
from azure.iot.device import IoTHubDeviceClient, Message

# 🔑 Azure IoT Hub Device Connection String
CONNECTION_STRING = "HostName=BrindavanDevices.azure-devices.net;DeviceId=device001;SharedAccessKey=gzLViL0cRaZmYjOQ7dGmRWoyAiWW9+IsnAIoTPWLN1I="

# 🌌 NASA API Configuration
NASA_API_KEY = "NxJ7RKJ2CZgW2DWUGmCESXBugqWYG6qH7IGDzDM0"
NASA_URL = f"https://api.nasa.gov/neo/rest/v1/feed?start_date={datetime.now().strftime('%Y-%m-%d')}&api_key={NASA_API_KEY}"

# 📂 CSV Logging
csv_file = open("telemetry_log.csv", "a", newline="")
csv_writer = csv.writer(csv_file)
if csv_file.tell() == 0:
    csv_writer.writerow(["timestamp", "object_name", "distance_km", "velocity_kmh", "alert", "temperature", "humidity"])

# ✅ IoT Hub Client
def create_client():
    print("🔌 Connecting to Azure IoT Hub...")
    client = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING)
    client.connect()
    print("✅ Connected to IoT Hub.\n")
    return client

# ✅ Fetch Top 5 Asteroids
def get_top_asteroids():
    try:
        r = requests.get(NASA_URL, timeout=10)
        data = r.json()
        today = datetime.now().strftime('%Y-%m-%d')
        neo_list = data["near_earth_objects"].get(today, [])

        sorted_asteroids = sorted(neo_list, key=lambda x: float(x["close_approach_data"][0]["miss_distance"]["kilometers"]))
        top5 = sorted_asteroids[:5]

        telemetry_list = []
        for neo in top5:
            name = neo["name"]
            approach = neo["close_approach_data"][0]
            distance_km = float(approach["miss_distance"]["kilometers"])
            velocity_kmh = float(approach["relative_velocity"]["kilometers_per_hour"])
            alert = "DANGER" if distance_km < 1000000 else "SAFE"

            telemetry_list.append({
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "object_name": name,
                "distance_km": round(distance_km, 2),
                "velocity_kmh": round(velocity_kmh, 2),
                "alert": alert
            })

        return telemetry_list

    except Exception as e:
        print("⚠️ Error fetching NASA data:", e)
        return []

# ✅ Simulate Weather Telemetry (replace with actual sensor or OpenWeather API)
def get_weather():
    temperature = round(random.uniform(15, 30), 2)  # Simulate temp (°C)
    humidity = round(random.uniform(40, 90), 2)     # Simulate humidity (%)
    alert = "NORMAL" if temperature < 35 else "ALERT"
    return {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "temperature": temperature,
        "humidity": humidity,
        "alert": alert
    }

# ✅ Main Loop
client = create_client()
try:
    while True:
        try:
            asteroids = get_top_asteroids()
            weather = get_weather()

            if asteroids:
                # 🔹 Send asteroid batch
                client.send_message(Message(json.dumps(asteroids)))
                print(f"📤 Sent {len(asteroids)} asteroid objects")

            # 🔹 Send weather separately
            client.send_message(Message(json.dumps(weather)))
            print(f"🌦️ Sent weather data: {weather}")

            time.sleep(300)  # Update every 5 min

        except Exception as e:
            print("⚠️ Disconnected or error sending:", e)
            try:
                client.shutdown()
            except:
                pass
            time.sleep(5)
            client = create_client()

except KeyboardInterrupt:
    print("🛑 Stopping telemetry...")
    try:
        client.shutdown()
    except:
        pass
    csv_file.close()
    sys.exit(0)
