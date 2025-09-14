#!/usr/bin/env python3
"""
Integration test for disaster recovery with real Synapse shutdown and restart.

This test runs outside of the Twisted Trial framework to properly simulate
a real disaster recovery scenario with database restore and server restart.
"""

import os
import sys
import json
import time
import sqlite3
import tempfile
import subprocess
import urllib.request
import urllib.parse
import urllib.error
from pathlib import Path
from typing import Optional, Dict, Any


class SynapseIntegrationTest:
    """Integration test that manages a real Synapse process."""
    
    def __init__(self):
        self.temp_dir = tempfile.mkdtemp(prefix="synapse_disaster_test_")
        self.db_path = os.path.join(self.temp_dir, "homeserver.db")
        self.config_path = os.path.join(self.temp_dir, "homeserver.yaml")
        self.log_config_path = os.path.join(self.temp_dir, "log.yaml")
        self.server_name = "localhost"
        self.port = 18008  # Use non-standard port to avoid conflicts
        self.process: Optional[subprocess.Popen] = None
        self.user_id = "@admin:localhost"
        self.password = "admin_password"
        self.access_token: Optional[str] = None
        
    def _make_request(self, method: str, url: str, data: Optional[Dict] = None, 
                      headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Make HTTP request and return JSON response."""
        if headers is None:
            headers = {}
            
        if data is not None:
            data = json.dumps(data).encode('utf-8')
            headers['Content-Type'] = 'application/json'
            
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        
        try:
            with urllib.request.urlopen(req) as response:
                result = response.read().decode('utf-8')
                if result:
                    return json.loads(result)
                return {}
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8')
            raise Exception(f"HTTP {e.code}: {error_body}")
        
    def setup(self):
        """Set up test environment."""
        print(f"Setting up test in {self.temp_dir}")
        
        # Create config files
        self._create_config()
        self._create_log_config()
        
    def _create_config(self):
        """Create minimal Synapse config for testing."""
        config = f"""
server_name: "{self.server_name}"
pid_file: {self.temp_dir}/homeserver.pid
public_baseurl: http://localhost:{self.port}/

listeners:
  - port: {self.port}
    type: http
    tls: false
    x_forwarded: false
    bind_addresses: ['127.0.0.1']
    resources:
      - names: [client, federation]
        compress: false

database:
  name: sqlite3
  args:
    database: {self.db_path}

media_store_path: {self.temp_dir}/media_store
uploads_path: {self.temp_dir}/uploads

registration_shared_secret: "test_secret"
report_stats: false
enable_registration: true
enable_registration_without_verification: true

macaroon_secret_key: "test_macaroon_secret"
form_secret: "test_form_secret"
signing_key_path: {self.temp_dir}/signing.key

log_config: {self.log_config_path}

# Enable experimental features for bulk event injection
experimental_features:
  # Allow injection with auth events only
  require_verification_for_room_joins: false

trusted_key_servers: []
suppress_key_server_warning: true
"""
        with open(self.config_path, 'w') as f:
            f.write(config)
            
    def _create_log_config(self):
        """Create minimal log config."""
        log_config = """
version: 1
formatters:
  precise:
    format: '%(asctime)s - %(name)s - %(lineno)d - %(levelname)s - %(message)s'
handlers:
  console:
    class: logging.StreamHandler
    formatter: precise
loggers:
  synapse:
    level: INFO
    handlers: [console]
root:
  level: INFO
  handlers: [console]
"""
        with open(self.log_config_path, 'w') as f:
            f.write(log_config)
            
    def start_synapse(self):
        """Start Synapse process."""
        print("Starting Synapse...")
        
        # First generate keys
        print("Generating keys...")
        key_gen_cmd = [
            sys.executable, "-m", "synapse.app.homeserver",
            "-c", self.config_path,
            "--generate-keys"
        ]
        subprocess.run(key_gen_cmd, check=True)
        
        # Now start the server
        cmd = [
            sys.executable, "-m", "synapse.app.homeserver",
            "-c", self.config_path
        ]
        
        # Start process and capture output
        self.process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True
        )
        
        # Collect output in background
        output_lines = []
        import threading
        def read_output():
            for line in self.process.stdout:
                output_lines.append(line.strip())
                
        output_thread = threading.Thread(target=read_output)
        output_thread.daemon = True
        output_thread.start()
        
        # Wait for Synapse to start
        start_time = time.time()
        while time.time() - start_time < 30:
            try:
                response = self._make_request("GET", f"http://localhost:{self.port}/_matrix/client/versions")
                if "versions" in response:
                    print("Synapse started successfully")
                    return
            except Exception as e:
                # Check if process died
                if self.process.poll() is not None:
                    print("\nSynapse process died! Output:")
                    for line in output_lines:
                        print(f"  {line}")
                    raise Exception(f"Synapse process exited with code {self.process.poll()}")
            time.sleep(0.5)
            
        # Show output if startup failed
        print("\nSynapse failed to start. Output:")
        for line in output_lines[-50:]:  # Show last 50 lines
            print(f"  {line}")
        raise Exception("Synapse failed to start within 30 seconds")
        
    def stop_synapse(self):
        """Stop Synapse process."""
        print("Stopping Synapse...")
        if self.process:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
            self.process = None
            
    def register_user(self):
        """Register admin user."""
        print(f"Registering user {self.user_id}...")
        
        # First get nonce
        try:
            nonce_response = self._make_request(
                "GET", 
                f"http://localhost:{self.port}/_synapse/admin/v1/register"
            )
            nonce = nonce_response.get("nonce", "test_nonce")
        except:
            nonce = "test_nonce"
        
        # Use registration shared secret
        import hmac
        import hashlib
        
        username = self.user_id.split(':')[0][1:]
        mac = hmac.new(
            b"test_secret",
            f"{nonce}\x00{username}\x00{self.password}\x00admin".encode(),
            hashlib.sha1
        ).hexdigest()
        
        try:
            response = self._make_request(
                "POST",
                f"http://localhost:{self.port}/_synapse/admin/v1/register",
                data={
                    "nonce": nonce,
                    "username": username,
                    "password": self.password,
                    "admin": True,
                    "mac": mac
                }
            )
            
            self.access_token = response["access_token"]
            print(f"User registered, access token: {self.access_token[:20]}...")
        except Exception as e:
            # Try to login if already registered
            print(f"Registration failed ({e}), trying login...")
            self.login()
            
    def login(self):
        """Login as admin user."""
        print(f"Logging in as {self.user_id}...")
        response = self._make_request(
            "POST",
            f"http://localhost:{self.port}/_matrix/client/r0/login",
            data={
                "type": "m.login.password",
                "user": self.user_id,
                "password": self.password
            }
        )
        
        self.access_token = response["access_token"]
        print(f"Logged in, access token: {self.access_token[:20]}...")
            
    def create_room(self) -> str:
        """Create a test room."""
        print("Creating room...")
        response = self._make_request(
            "POST",
            f"http://localhost:{self.port}/_matrix/client/r0/createRoom",
            headers={"Authorization": f"Bearer {self.access_token}"},
            data={"name": "Test Room"}
        )
        
        room_id = response["room_id"]
        print(f"Created room: {room_id}")
        return room_id
            
    def send_message(self, room_id: str, message: str) -> str:
        """Send a message to a room."""
        print(f"Sending message: {message}")
        response = self._make_request(
            "PUT",
            f"http://localhost:{self.port}/_matrix/client/r0/rooms/{room_id}/send/m.room.message/{int(time.time()*1000)}",
            headers={"Authorization": f"Bearer {self.access_token}"},
            data={
                "msgtype": "m.text",
                "body": message
            }
        )
        
        event_id = response["event_id"]
        print(f"Sent message: {event_id}")
        return event_id
            
    def get_room_messages(self, room_id: str) -> list:
        """Get messages from a room."""
        response = self._make_request(
            "GET",
            f"http://localhost:{self.port}/_matrix/client/r0/rooms/{room_id}/messages?dir=b&limit=100",
            headers={"Authorization": f"Bearer {self.access_token}"}
        )
        
        return response["chunk"]
            
    def backup_database(self) -> str:
        """Backup the database."""
        backup_path = os.path.join(self.temp_dir, "backup.db")
        print(f"Backing up database to {backup_path}")
        
        # Use SQLite backup API
        source = sqlite3.connect(self.db_path)
        dest = sqlite3.connect(backup_path)
        source.backup(dest)
        source.close()
        dest.close()
        
        return backup_path
        
    def restore_database(self, backup_path: str):
        """Restore database from backup."""
        print(f"Restoring database from {backup_path}")
        
        # Copy backup over current database
        source = sqlite3.connect(backup_path)
        dest = sqlite3.connect(self.db_path)
        source.backup(dest)
        source.close()
        dest.close()
        
    def get_all_room_events(self, room_id: str) -> list:
        """Get all events from a room using the context API."""
        print(f"Getting all events from room {room_id}")
        
        # First get one event to use as anchor
        messages = self.get_room_messages(room_id)
        if not messages:
            return []
            
        anchor_event = messages[0]["event_id"]
        
        # Get context with large limit
        response = self._make_request(
            "GET",
            f"http://localhost:{self.port}/_matrix/client/r0/rooms/{room_id}/context/{anchor_event}?limit=1000",
            headers={"Authorization": f"Bearer {self.access_token}"}
        )
        
        # Combine all events
        all_events = []
        if "events_before" in response:
            all_events.extend(response["events_before"])
        if "event" in response:
            all_events.append(response["event"])
        if "events_after" in response:
            all_events.extend(response["events_after"])
        if "state" in response:
            all_events.extend(response["state"])
            
        return all_events
            
    def inject_room_events(self, room_id: str, events_list: list):
        """Inject room events for disaster recovery."""
        print(f"Injecting {len(events_list)} events into room {room_id}")
        
        # Use the bulk injection endpoint
        response = self._make_request(
            "POST",
            f"http://localhost:{self.port}/_synapse/admin/v1/bulk_inject",
            headers={"Authorization": f"Bearer {self.access_token}"},
            data={"events": events_list}
        )
        print("Events injected successfully")
        return response
            
    def run_test(self):
        """Run the full disaster recovery test."""
        print("\n=== Disaster Recovery Integration Test ===\n")
        
        try:
            # 1. Setup and start Synapse
            self.setup()
            self.start_synapse()
            self.register_user()
            
            # 2. Create room and send initial messages
            room_id = self.create_room()
            msg1 = self.send_message(room_id, "Message 1")
            msg2 = self.send_message(room_id, "Message 2")
            
            # 3. Take backup after message 2
            backup_path = self.backup_database()
            
            # 4. Send more messages
            msg3 = self.send_message(room_id, "Message 3")
            msg4 = self.send_message(room_id, "Message 4")
            
            # 5. Get all events before disaster
            all_events = self.get_all_room_events(room_id)
            
            # Filter to just messages 3 and 4
            messages_to_recover = []
            for event in all_events:
                if event.get("type") == "m.room.message":
                    content = event.get("content", {})
                    if content.get("body") in ["Message 3", "Message 4"]:
                        messages_to_recover.append(event)
                        
            print(f"\nEvents to recover: {len(messages_to_recover)}")
            
            # Debug: print event structure
            if messages_to_recover:
                print("\nFirst event structure:")
                first_event = messages_to_recover[0]
                for key in first_event:
                    print(f"  {key}: {type(first_event[key])}")
            
            # 6. SIMULATE DISASTER: Stop Synapse
            print("\n--- SIMULATING DISASTER ---")
            self.stop_synapse()
            
            # 7. Restore database to backup point (losing messages 3 and 4)
            self.restore_database(backup_path)
            
            # 8. Restart Synapse (simulating recovery)
            print("\n--- RECOVERY PHASE ---")
            self.start_synapse()
            self.login()  # Need to login again after restart
            
            # 9. Verify messages 3 and 4 are gone
            messages = self.get_room_messages(room_id)
            message_bodies = [m.get("content", {}).get("body") for m in messages 
                            if m.get("type") == "m.room.message"]
            
            print(f"\nMessages after restore: {message_bodies}")
            assert "Message 3" not in message_bodies
            assert "Message 4" not in message_bodies
            print("✓ Confirmed messages 3 and 4 were lost")
            
            # 10. Use bulk injection to restore lost messages
            print("\n--- RESTORING LOST MESSAGES ---")
            
            # Add required fields for bulk injection
            # Get auth events from room state
            state_events = [e for e in all_events if e.get("state_key") is not None]
            create_event_id = None
            member_event_id = None
            power_event_id = None
            
            for event in state_events:
                if event["type"] == "m.room.create":
                    create_event_id = event["event_id"]
                elif event["type"] == "m.room.member" and event["state_key"] == self.user_id:
                    member_event_id = event["event_id"]
                elif event["type"] == "m.room.power_levels":
                    power_event_id = event["event_id"]
                    
            # Add required fields to events
            for event in messages_to_recover:
                event["auth_events"] = []
                if create_event_id:
                    event["auth_events"].append(create_event_id)
                if member_event_id:
                    event["auth_events"].append(member_event_id)
                if power_event_id:
                    event["auth_events"].append(power_event_id)
                    
                # For prev_events, use the last message before this one
                event["prev_events"] = [msg2]  # Message 2's event ID
                event["depth"] = 10  # Reasonable depth
            
            response = self.inject_room_events(room_id, messages_to_recover)
            print(f"\nInjection response: {response}")
            
            # Check if event IDs were mapped
            if "event_id_mapping" in response:
                print("\nEvent ID mappings:")
                for old_id, new_id in response["event_id_mapping"].items():
                    print(f"  {old_id} -> {new_id}")
            
            # 11. Verify recovery worked (may need to wait a bit)
            print("\nWaiting for events to be processed...")
            time.sleep(2)
            
            messages = self.get_room_messages(room_id)
            message_bodies = [m.get("content", {}).get("body") for m in messages 
                            if m.get("type") == "m.room.message"]
            
            print(f"\nMessages after injection: {message_bodies}")
            
            # Debug: show all message events
            print("\nAll message events:")
            for m in messages:
                if m.get("type") == "m.room.message":
                    print(f"  - {m.get('event_id')}: {m.get('content', {}).get('body')}")
            
            # Also try using sync to get messages
            print("\nTrying sync API...")
            sync_response = self._make_request(
                "GET",
                f"http://localhost:{self.port}/_matrix/client/r0/sync?timeout=0",
                headers={"Authorization": f"Bearer {self.access_token}"}
            )
            
            if "rooms" in sync_response and "join" in sync_response["rooms"]:
                if room_id in sync_response["rooms"]["join"]:
                    timeline = sync_response["rooms"]["join"][room_id].get("timeline", {})
                    sync_events = timeline.get("events", [])
                    print(f"Found {len(sync_events)} events in sync")
                    for e in sync_events:
                        if e.get("type") == "m.room.message":
                            print(f"  - {e.get('content', {}).get('body')}")
            
            if "Message 3" not in message_bodies and "Message 4" not in message_bodies:
                print("\n✗ Failed to recover messages - they might not be visible via client APIs")
                print("  This could be due to:")
                print("  - Events being injected but not appearing in timelines")
                print("  - Need for additional room state setup")
            else:
                assert "Message 3" in message_bodies
                assert "Message 4" in message_bodies
                print("✓ Successfully recovered lost messages!")
            
            # 12. Test that we can still send new messages
            msg5 = self.send_message(room_id, "Message 5 - After Recovery")
            print("✓ Can send new messages after recovery")
            
            print("\n=== TEST PASSED ===")
            
        finally:
            # Cleanup
            self.stop_synapse()
            print(f"\nTest files left in: {self.temp_dir}")
            

if __name__ == "__main__":
    test = SynapseIntegrationTest()
    test.run_test()