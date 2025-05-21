import functions_framework
from google.events.cloud.firestore import DocumentEventData
import base64
from cloudevents.http import from_http
import flask

def multi_trigger_function_internal(cloud_event):
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

@functions_framework.http
def multi_trigger_function(request: flask.Request):
    print("=== Incoming headers ===")
    for header, value in request.headers.items():
        print(f"{header}: {value}")
    if "Ce-Type" in request.headers:
        cloud_event = from_http(request.headers, request.get_data())
        multi_trigger_function_internal(cloud_event)
        return "Handled as CloudEvent", 200

    # Otherwise, treat as regular HTTP
    print(f"Payload: {request.get_json()}")
    return "Handled as HTTP", 200

@functions_framework.cloud_event
def OLD_multi_trigger_function(cloud_event):
    # HACK: Access the Flask request via Flask's thread-local
    req = flask.request  # flask.request is global in context of a request
    print("=== Incoming headers ===")
    for header, value in req.headers.items():
        print(f"{header}: {value}")
    multi_trigger_function_internal(cloud_event)

