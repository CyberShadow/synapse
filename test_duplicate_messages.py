#!/usr/bin/env python3
"""Test that sending duplicate messages creates different event IDs."""

import json
import time
import urllib.request

# Test configuration
server_url = "http://localhost:8008"
access_token = "YOUR_ACCESS_TOKEN"  # Replace with actual token
room_id = "!YOUR_ROOM_ID:localhost"  # Replace with actual room ID

def send_message(text):
    """Send a message and return the event ID."""
    url = f"{server_url}/_matrix/client/r0/rooms/{room_id}/send/m.room.message/{int(time.time() * 1000)}"
    
    data = json.dumps({
        "msgtype": "m.text",
        "body": text
    }).encode('utf-8')
    
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        },
        method='PUT'
    )
    
    with urllib.request.urlopen(req) as response:
        result = json.loads(response.read().decode('utf-8'))
        return result['event_id']

# Send the same message twice
message = "This is a duplicate message"

print("Sending first message...")
event_id_1 = send_message(message)
print(f"First event ID: {event_id_1}")

# Small delay to ensure different timestamp
time.sleep(0.1)

print("\nSending identical message...")
event_id_2 = send_message(message)
print(f"Second event ID: {event_id_2}")

print(f"\nEvent IDs are different: {event_id_1 != event_id_2}")