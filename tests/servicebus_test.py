import os
import json
from dotenv import load_dotenv
from azure.servicebus import ServiceBusClient, ServiceBusMessage

load_dotenv()

conn = os.environ["SERVICE_BUS_CONNECTION_STRING"]
QUEUE_NAME = "teams-marker-queue"
TEST_MESSAGE = {"meeting_id": "test-meeting-id", "marker_label": "Test Marker"}

with ServiceBusClient.from_connection_string(conn) as client:
    with client.get_queue_sender(queue_name=QUEUE_NAME) as sender:
        message = ServiceBusMessage(json.dumps(TEST_MESSAGE))
        sender.send_messages(message)
print("Test message sent to Service Bus queue.")