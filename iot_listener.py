from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub import TransportType
import asyncio
import queue
import ast
import traceback
from datetime import datetime

# Event Hub Connection Details
CONNECTION_STR = "Endpoint=sb://ihsuprodlnres010dednamespace.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=gzLViL0cRaZmYjOQ7dGmRWoyAiWW9+IsnAIoTPWLN1I=;EntityPath=iothub-ehub-brindavand-56805614-86dc16d158"
EVENTHUB_NAME = "iothub-ehub-brindavand-56805614-86dc16d158"

# Shared Queue for Dashboard
telemetry_queue = queue.Queue()

def normalize_weather(data):
    """Convert possible IoT weather keys to a standard format."""
    if "temp" in data:
        data["temperature"] = data.pop("temp")
    if "hum" in data:
        data["humidity"] = data.pop("hum")
    return data

async def on_event(partition_context, event):
    try:
        raw = event.body_as_str()
        data = ast.literal_eval(raw)

        print(f"✅ Message received from partition {partition_context.partition_id}")
        print(f"📦 Raw Telemetry: {data}")

        # Handle single telemetry dict
        if isinstance(data, dict):
            data["timestamp"] = datetime.now()

            if any(k in data for k in ["temp", "temperature", "hum", "humidity"]):
                data = normalize_weather(data)
                print("🌦️ Detected Weather Telemetry")

            telemetry_queue.put(data)

        # Handle list of asteroid telemetry
        elif isinstance(data, list):
            for obj in data:
                obj["timestamp"] = datetime.now()
            print("☄️ Detected Asteroid Batch")
            telemetry_queue.put(data)

        await partition_context.update_checkpoint(event)

    except Exception as e:
        print("⚠️ Error processing telemetry event:")
        traceback.print_exc()

async def main():
    print("\n🔍 Starting Event Hub Listener...\n")
    client = EventHubConsumerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        consumer_group="$Default",
        eventhub_name=EVENTHUB_NAME,
        transport_type=TransportType.AmqpOverWebsocket
    )

    async with client:
        await client.receive(
            on_event=on_event,
            starting_position="-1"
        )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🛑 Listener stopped by user.")
