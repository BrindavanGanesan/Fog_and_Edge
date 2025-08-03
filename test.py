from azure.iot.device import IoTHubDeviceClient

CONNECTION_STRING = "HostName=BrindavanDevices.azure-devices.net;DeviceId=device001;SharedAccessKey=gzLViL0cRaZmYjOQ7dGmRWoyAiWW9+IsnAIoTPWLN1I="
EVENTHUB_NAME = "iothub-ehub-brindavand-56805614-86dc16d158"

client = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING)
client.connect()
print("🎉 Connection successful!")
client.disconnect()
