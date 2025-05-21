from functions_framework import cloud_event
from google.events.cloud.firestore import DocumentEventData
import base64

@cloud_event
def multi_trigger_function(cloud_event):
    print("=== EVENT RECEIVED ===")
    event_id = cloud_event["id"]
    event_type = cloud_event["type"]
    print(f"ID: {event_id}")
    print(f"Type: {event_type}")
    print(f"Source: {cloud_event['source']}")

    print("TODO Data:")
    data = cloud_event.data
    if event_type == "google.cloud.storage.object.v1.finalized":
        bucket = data["bucket"]
        name = data["name"]
        metageneration = data["metageneration"]
        timeCreated = data["timeCreated"]
        updated = data["updated"]
        print(f"Bucket: {bucket}")
        print(f"File: {name}")
        print(f"Metageneration: {metageneration}")
        print(f"Created: {timeCreated}")
        print(f"Updated: {updated}")
        return
    if event_type == "google.cloud.firestore.document.v1.written":
        firestore_payload = DocumentEventData()
        firestore_payload._pb.ParseFromString(cloud_event.data)
        print("Old value:")
        print(firestore_payload.old_value)
        print("New value:")
        print(firestore_payload.value)        
        return
    if cloud_event['type'].startswith('google.cloud.pubsub.topic.v1.messagePublished'):
        message_data = cloud_event.data.get('message', {}).get('data')
        decoded = base64.b64decode(message_data).decode('utf-8')
        print(f"Decoded Pub/Sub message: {decoded}")
        return
