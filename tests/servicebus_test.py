import os
import json
from dotenv import load_dotenv
from azure.servicebus import ServiceBusClient, ServiceBusMessage

load_dotenv()

conn = os.environ["SERVICE_BUS_CONNECTION_STRING"]
QUEUE_NAME = "teams-marker-queue"
MEETING_ID = "MSpjMWMzZjMzMC0zZWNlLTQxOTQtODI4OC1jOGNjNGVlNzRiZWUqMCoqMTk6bWVldGluZ19OalEwTTJNMVl6UXRZbUk1WkMwMFlXVXpMVGhtTVdVdFptSmxOVFZoWXpoall6a3lAdGhyZWFkLnYy"
ORGANIZER_ID = "c1c3f330-3ece-4194-8288-c8cc4ee74bee"
TEST_MESSAGE = {"online_meeting_id": MEETING_ID, "organizer_id": ORGANIZER_ID}

with ServiceBusClient.from_connection_string(conn) as client:
    with client.get_queue_sender(queue_name=QUEUE_NAME) as sender:
        message = ServiceBusMessage(json.dumps(TEST_MESSAGE))
        sender.send_messages(message)
print("Test message sent to Service Bus queue.")