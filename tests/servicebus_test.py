import os
import json
from dotenv import load_dotenv
from azure.servicebus import ServiceBusClient, ServiceBusMessage

load_dotenv()

conn = os.environ["SERVICE_BUS_CONNECTION_STRING"]
QUEUE_NAME = "teams-marker-queue"
MEETING_ID = "MSpjMWMzZjMzMC0zZWNlLTQxOTQtODI4OC1jOGNjNGVlNzRiZWUqMCoqMTk6bWVldGluZ19OalEwTTJNMVl6UXRZbUk1WkMwMFlXVXpMVGhtTVdVdFptSmxOVFZoWXpoall6a3lAdGhyZWFkLnYy"
ORGANIZER_ID = "c1c3f330-3ece-4194-8288-c8cc4ee74bee"
JOIN_URL = "https://teams.microsoft.com/l/meetup-join/19%3ameeting_MDhhMTkyNDQtZTdiNS00OTVmLTgwMjktYTMwNTMyYWRmMmE5%40thread.v2/0?context=%7b%22Tid%22%3a%22dba8614e-5825-4990-87e0-e392a37f09a4%22%2c%22Oid%22%3a%22c1c3f330-3ece-4194-8288-c8cc4ee74bee%22%7d"
#TEST_MESSAGE = {"join_url": JOIN_URL, "organizer_id": ORGANIZER_ID}
TEST_MESSAGE = {
  "id": "test-evt-001",
  "eventType": "Microsoft.Graph.LifecycleNotification",
  "subject": "/subscriptions/<subId>",
  "eventTime": "2025-10-24T01:00:00Z",
  "dataVersion": "1.0",
  "metadataVersion": "1",
  "data": {
    "subscriptionId": "<REAL_SUBSCRIPTION_ID_OR_FAKE_FOR_NEGATIVE_TEST>",
    "clientState": "<YOUR_GRAPH_SUBS_CLIENT_STATE>",
    "lifecycleEvent": "reauthorizationRequired"
  },
  "topic": "/subscriptions/<azureSubId>/resourceGroups/<rg>/providers/Microsoft.EventGrid/partnerTopics/<topicName>"
}

with ServiceBusClient.from_connection_string(conn) as client:
    with client.get_queue_sender(queue_name=QUEUE_NAME) as sender:
        message = ServiceBusMessage(json.dumps(TEST_MESSAGE))
        sender.send_messages(message)
print("Test message sent to Service Bus queue.")