#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright (C) 2024 New Vector, Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#

"""Tests for disaster recovery using the bulk event injection admin API.

These tests focus on user experience scenarios - what a Matrix client would see
after disaster recovery. They use valid events created through normal APIs,
simulate database loss, and verify recovery through the bulk injection endpoint.
"""

import json
import sqlite3
import time
from http import HTTPStatus
from typing import Dict, List, Optional, Tuple

from twisted.test.proto_helpers import MemoryReactor

import synapse.rest.admin
from synapse.api.constants import EventTypes, Membership
from synapse.rest.client import login, room, sync
from synapse.server import HomeServer
from synapse.util import Clock

from tests import unittest


class DisasterRecoveryTestCase(unittest.HomeserverTestCase):
    """Test disaster recovery scenarios using valid events."""
    
    servlets = [
        synapse.rest.admin.register_servlets,
        login.register_servlets,
        room.register_servlets,
        sync.register_servlets,
    ]

    def prepare(self, reactor: MemoryReactor, clock: Clock, hs: HomeServer) -> None:
        self.store = hs.get_datastores().main
        
        # Admin user for API access
        self.admin_user = self.register_user("admin", "pass", admin=True)
        self.admin_user_tok = self.login("admin", "pass")
        
        # Regular test users
        self.user1 = self.register_user("alice", "pass")
        self.user1_tok = self.login("alice", "pass")
        
        self.user2 = self.register_user("bob", "pass") 
        self.user2_tok = self.login("bob", "pass")
        
        self.injection_url = "/_synapse/admin/v1/bulk_inject"

    def backup_database(self) -> sqlite3.Connection:
        """Create a backup of the current SQLite database state."""
        # Check if we're using SQLite
        from synapse.storage.engines import Sqlite3Engine
        
        if not isinstance(self.store.db_pool.engine, Sqlite3Engine):
            self.skipTest("Database backup/restore only implemented for SQLite")
            
        # Create an in-memory backup
        backup_conn = sqlite3.connect(":memory:")
        
        # Get a database connection from the pool and do backup
        def do_backup(conn):
            # Get the underlying sqlite3.Connection from Twisted's wrapper
            if hasattr(conn, '_connection'):
                actual_conn = conn._connection
            else:
                actual_conn = conn
            
            # Do backup
            actual_conn.backup(backup_conn)
            return backup_conn
            
        self.get_success(
            self.store.db_pool._db_pool.runWithConnection(do_backup)
        )
        
        return backup_conn

    def restore_database(self, backup_conn: sqlite3.Connection) -> None:
        """Restore database from a backup, simulating data loss."""
        # Get a database connection from the pool
        def do_restore(conn):
            # Get the underlying sqlite3.Connection from Twisted's wrapper
            if hasattr(conn, '_connection'):
                actual_conn = conn._connection
            else:
                actual_conn = conn
                
            # For SQLite, we need to close the connection and restore properly
            # This is a more thorough restore that replaces the entire database
            backup_conn.backup(actual_conn)
            
            return None
            
        self.get_success(
            self.store.db_pool._db_pool.runWithConnection(do_restore)
        )
        
        # Clear all caches to ensure we're reading fresh data  
        self.store._get_event_cache.clear()
        # Clear other critical caches
        if hasattr(self.store, '_event_ref'):
            self.store._event_ref.clear()
        
        # Fix stream positions after restore
        def fix_stream_positions(txn):
            # Get the actual max stream ordering from events
            txn.execute("SELECT COALESCE(MAX(stream_ordering), 0) FROM events")
            max_stream_ordering = txn.fetchone()[0]
            
            # Update ALL stream position entries for 'events' stream
            # This handles both the general entry and instance-specific entries
            txn.execute(
                "UPDATE stream_positions SET stream_id = ? WHERE stream_name = 'events'",
                (max_stream_ordering,)
            )
            
            # Also update other related stream positions
            txn.execute(
                "UPDATE stream_positions SET stream_id = ? WHERE stream_name = 'room_memberships'",
                (max_stream_ordering,)
            )
            
            return max_stream_ordering
            
        max_so = self.get_success(
            self.store.db_pool.runInteraction("fix_stream_positions", fix_stream_positions)
        )
        
        # Reset the stream ID generator to the correct position
        # For MultiWriterIdGenerator we need to clear internal state and reset positions
        if hasattr(self.store._stream_id_gen, '_current_positions'):
            # This is a MultiWriterIdGenerator
            instance_name = self.store._stream_id_gen._instance_name
            
            # Clear internal state with lock held
            with self.store._stream_id_gen._lock:
                # Reset all positions to the restored max
                self.store._stream_id_gen._current_positions.clear()
                self.store._stream_id_gen._current_positions[instance_name] = max_so
                
                # Reset other internal state
                self.store._stream_id_gen._max_seen_allocated_stream_id = max_so
                self.store._stream_id_gen._persisted_upto_position = max_so
                self.store._stream_id_gen._max_position_of_local_instance = max_so
                
                # Clear any in-flight or unfinished IDs
                self.store._stream_id_gen._unfinished_ids.clear()
                self.store._stream_id_gen._finished_ids.clear()
                self.store._stream_id_gen._in_flight_fetches.clear()
                self.store._stream_id_gen._known_persisted_positions.clear()
                
            # Also reset the sequence generator if it's a LocalSequenceGenerator
            if hasattr(self.store._stream_id_gen, '_sequence_gen'):
                seq_gen = self.store._stream_id_gen._sequence_gen
                if hasattr(seq_gen, '_current_max_id'):
                    # LocalSequenceGenerator - reset its internal counter
                    with seq_gen._lock:
                        seq_gen._current_max_id = max_so
        else:
            # For simpler generators, directly set _current
            self.store._stream_id_gen._current = max_so

    def extract_events_for_room(self, room_id: str) -> List[Dict]:
        """Extract all events for a room in a format suitable for re-injection."""
        # Get all events for the room
        def get_events_txn(txn):
            txn.execute(
                """
                SELECT e.event_id, e.type, e.stream_ordering, ej.json
                FROM events e
                INNER JOIN event_json ej USING (event_id)
                WHERE e.room_id = ?
                ORDER BY e.stream_ordering
                """,
                (room_id,)
            )
            
            events = []
            for event_id, event_type, stream_ordering, event_json_str in txn:
                event_dict = json.loads(event_json_str)
                # Ensure event_id is included (it's not always in the JSON)
                event_dict["event_id"] = event_id
                events.append(event_dict)
                
            return events
            
        return self.get_success(
            self.store.db_pool.runInteraction(
                "extract_events_for_room",
                get_events_txn
            )
        )

    def test_recover_messages_after_partial_loss(self) -> None:
        """Test recovering messages after partial data loss using database backup/restore."""
        # 1. Create room and send first message
        room_id = self.helper.create_room_as(self.user1, tok=self.user1_tok)
        self.helper.send(room_id, "Message 1", tok=self.user1_tok)
        
        # 2. Take backup after first message
        backup_conn = self.backup_database()
        
        # 3. Send more messages after backup
        self.helper.send(room_id, "Message 2", tok=self.user1_tok) 
        self.helper.send(room_id, "Message 3", tok=self.user1_tok)
        
        # Extract Message 2 for recovery before we lose it
        all_events = self.extract_events_for_room(room_id)
        message2_event = None
        for event in all_events:
            if (event.get("type") == "m.room.message" and 
                event.get("content", {}).get("body") == "Message 2"):
                message2_event = event
                break
        
        self.assertIsNotNone(message2_event)
        
        # Add event_id if missing
        if "event_id" not in message2_event:
            # Find the event ID from database
            msg2_id = self.get_success(
                self.store.db_pool.simple_select_one_onecol(
                    table="events",
                    keyvalues={
                        "room_id": room_id,
                        "type": "m.room.message",
                    },
                    retcol="event_id",
                    desc="get_message2_id",
                )
            )
            message2_event["event_id"] = msg2_id
        
        # 4. Restore from backup (loses Messages 2 and 3)
        self.restore_database(backup_conn)
        backup_conn.close()
        
        # 5. Verify only Message 1 exists now
        timeline_after_restore = self.make_request(
            "GET",
            f"/_matrix/client/r0/rooms/{room_id}/messages?dir=b&limit=10",
            access_token=self.user1_tok,
        )
        
        messages_after_restore = [
            e["content"]["body"] for e in timeline_after_restore.json_body["chunk"]
            if e["type"] == "m.room.message"
        ]
        
        self.assertEqual(len(messages_after_restore), 1)
        self.assertIn("Message 1", messages_after_restore)
        self.assertNotIn("Message 2", messages_after_restore)
        self.assertNotIn("Message 3", messages_after_restore)
        
        # 6. Recover the missing Message 2
        inject_response = self.make_request(
            "POST",
            self.injection_url,
            content={"events": [message2_event]},
            access_token=self.admin_user_tok,
        )
        
        self.assertEqual(HTTPStatus.OK, inject_response.code)
        self.assertEqual(inject_response.json_body["injected_events"], 1)
        self.assertEqual(inject_response.json_body["failed_events"], 0)
        
        # 7. Verify Message 2 is recovered
        timeline_recovered = self.make_request(
            "GET",
            f"/_matrix/client/r0/rooms/{room_id}/messages?dir=b&limit=10",
            access_token=self.user1_tok,
        )
        
        messages_recovered = [
            e["content"]["body"] for e in timeline_recovered.json_body["chunk"]
            if e["type"] == "m.room.message"
        ]
        
        self.assertEqual(len(messages_recovered), 2)
        self.assertIn("Message 1", messages_recovered)
        self.assertIn("Message 2", messages_recovered)
        # Message 3 should still be gone since it was after the backup
        self.assertNotIn("Message 3", messages_recovered)
        
        # 8. Verify room remains functional
        new_msg = self.helper.send(room_id, "New message after recovery", tok=self.user1_tok)
        self.assertIn("event_id", new_msg)

    def test_recover_lost_membership_event(self) -> None:
        """Test recovering when a user's join event is lost using backup/restore."""
        # 1. Create room and send initial message
        room_id = self.helper.create_room_as(self.user1, tok=self.user1_tok)
        self.helper.send(room_id, "Initial message", tok=self.user1_tok)
        
        # 2. Take backup after room is created but before user2 joins
        backup_conn = self.backup_database()
        
        # 3. User2 joins and sends message after backup
        self.helper.join(room_id, self.user2, tok=self.user2_tok)
        self.helper.send(room_id, "Hello from user2", tok=self.user2_tok)
        
        # Extract user2's events for recovery before we lose them
        all_events = self.extract_events_for_room(room_id)
        
        # Find user2's join and message events
        missing_events = []
        for event in all_events:
            if (event.get("type") == EventTypes.Member and
                event.get("state_key") == self.user2 and
                event.get("content", {}).get("membership") == "join"):
                missing_events.append(event)
            elif (event.get("type") == "m.room.message" and
                  event.get("content", {}).get("body") == "Hello from user2"):
                missing_events.append(event)
        
        self.assertEqual(len(missing_events), 2)  # join + message
        
        # Add event_id if missing
        for event in missing_events:
            if "event_id" not in event:
                if event.get("type") == EventTypes.Member:
                    event_id = self.get_success(
                        self.store.db_pool.simple_select_one_onecol(
                            table="current_state_events",
                            keyvalues={
                                "room_id": room_id,
                                "type": EventTypes.Member,
                                "state_key": self.user2,
                            },
                            retcol="event_id",
                            desc="get_join_event_id",
                        )
                    )
                    event["event_id"] = event_id
                else:
                    # For message events, find by content
                    event_id = self.get_success(
                        self.store.db_pool.simple_select_one_onecol(
                            table="events",
                            keyvalues={
                                "room_id": room_id,
                                "type": "m.room.message",
                                "sender": self.user2,
                            },
                            retcol="event_id", 
                            desc="get_message_event_id",
                        )
                    )
                    event["event_id"] = event_id
        
        # 4. Restore from backup (loses user2's join and message)
        self.restore_database(backup_conn)
        backup_conn.close()
        
        
        # 5. Verify state after restore
        # User1 should still see the room and initial message
        user1_sync = self.make_request(
            "GET",
            "/_matrix/client/r0/sync",
            access_token=self.user1_tok,
        )
        user1_rooms = user1_sync.json_body.get("rooms", {}).get("join", {})
        self.assertIn(room_id, user1_rooms, "User1 should still see room after restore")
        
        # User2 should not see the room anymore
        user2_sync = self.make_request(
            "GET",
            "/_matrix/client/r0/sync",
            access_token=self.user2_tok,
        )
        user2_rooms = user2_sync.json_body.get("rooms", {}).get("join", {})
        self.assertNotIn(room_id, user2_rooms, "User2 should not see room after restore")
        
        # 6. For proper injection, include the room creation event for context
        events_after_restore = self.extract_events_for_room(room_id)
        create_event = None
        for event in events_after_restore:
            if event.get("type") == EventTypes.Create:
                create_event = event
                break
        
        self.assertIsNotNone(create_event, "Room create event should exist after restore")
        
        # Inject create event + missing events for proper context
        events_to_inject = [create_event] + missing_events
        
        inject_response = self.make_request(
            "POST",
            self.injection_url,
            content={"events": events_to_inject},
            access_token=self.admin_user_tok,
        )
        
        self.assertEqual(HTTPStatus.OK, inject_response.code)
        self.assertEqual(inject_response.json_body["injected_events"], 2)  # join + message
        
        # 7. Verify user2 can see the room again
        sync_response_after = self.make_request(
            "GET",
            "/_matrix/client/r0/sync",
            access_token=self.user2_tok,
        )
        
        joined_rooms_after = sync_response_after.json_body.get("rooms", {}).get("join", {})
        self.assertIn(room_id, joined_rooms_after, "User should see room after join event recovered")
        
        # Verify user2 can see their message
        events = joined_rooms_after[room_id]["timeline"]["events"]
        messages = [e for e in events if e["type"] == "m.room.message"]
        user2_messages = [msg for msg in messages if msg["content"]["body"] == "Hello from user2"]
        self.assertTrue(len(user2_messages) > 0, "User should see their recovered message")

    def test_room_remains_functional_after_recovery(self) -> None:
        """Test that rooms work normally after disaster recovery."""
        # 1. Create room with initial state
        room_id = self.helper.create_room_as(
            self.user1, 
            tok=self.user1_tok,
            is_public=True  # Make it public so new users can join
        )
        
        # Send an initial message
        self.helper.send(room_id, "Original message", tok=self.user1_tok)
        
        # 2. Take backup and simulate loss
        backup_conn = self.backup_database()
        self.restore_database(backup_conn)  
        backup_conn.close()
        
        # 3. Extract events from the original state for recovery
        events = self.extract_events_for_room(room_id)
        
        # 4. Recover the room
        inject_response = self.make_request(
            "POST",
            self.injection_url,
            content={"events": events},
            access_token=self.admin_user_tok,
        )
        
        self.assertEqual(HTTPStatus.OK, inject_response.code)
        
        # 5. Test that new operations work
        # Send new message
        new_msg_response = self.helper.send(
            room_id, "New message after recovery", tok=self.user1_tok
        )
        self.assertIn("event_id", new_msg_response)
        
        # New user can join
        self.helper.join(room_id, self.user2, tok=self.user2_tok)
        
        # New user can send messages
        user2_msg_response = self.helper.send(
            room_id, "Hello from new user", tok=self.user2_tok
        )
        self.assertIn("event_id", user2_msg_response)
        
        # 6. Verify timeline has both old and new messages
        timeline_response = self.make_request(
            "GET",
            f"/_matrix/client/r0/rooms/{room_id}/messages?dir=b&limit=10",
            access_token=self.user1_tok,
        )
        
        messages = [
            e["content"]["body"] for e in timeline_response.json_body["chunk"]
            if e["type"] == "m.room.message"
        ]
        
        self.assertIn("Original message", messages)
        self.assertIn("New message after recovery", messages)
        self.assertIn("Hello from new user", messages)

    def test_preserved_timestamps_after_recovery(self) -> None:
        """Test that recovered messages preserve their original timestamps."""
        # 1. Create room with messages at different times
        room_id = self.helper.create_room_as(self.user1, tok=self.user1_tok)
        
        # Send first message
        msg1 = self.helper.send(room_id, "Old message", tok=self.user1_tok)
        
        # 2. Get original timestamps before backup
        timeline_before = self.make_request(
            "GET",
            f"/_matrix/client/r0/rooms/{room_id}/messages?dir=b&limit=10",
            access_token=self.user1_tok,
        )
        
        original_timestamps = {}
        for event in timeline_before.json_body["chunk"]:
            if event["type"] == "m.room.message":
                body = event["content"]["body"]
                original_timestamps[body] = event["origin_server_ts"]
        
        # Extract old message for recovery
        old_message_events = [
            e for e in self.extract_events_for_room(room_id)
            if e.get("type") == "m.room.message" and e.get("content", {}).get("body") == "Old message"
        ]
        self.assertEqual(len(old_message_events), 1)
        old_message_event = old_message_events[0]
        
        # Add event_id if missing
        if "event_id" not in old_message_event:
            old_message_event["event_id"] = msg1["event_id"]
        
        # 3. Take backup after first message, then send second message
        backup_conn = self.backup_database()
        
        time.sleep(0.1)  # Small delay for different timestamp
        msg2 = self.helper.send(room_id, "Recent message", tok=self.user1_tok)
        
        # Get all timestamps including the recent message
        timeline_with_both = self.make_request(
            "GET",
            f"/_matrix/client/r0/rooms/{room_id}/messages?dir=b&limit=10",
            access_token=self.user1_tok,
        )
        
        for event in timeline_with_both.json_body["chunk"]:
            if event["type"] == "m.room.message":
                body = event["content"]["body"]
                original_timestamps[body] = event["origin_server_ts"]
        
        self.assertEqual(len(original_timestamps), 2)
        
        # 4. Restore from backup (loses "Recent message" but keeps "Old message")
        self.restore_database(backup_conn)
        backup_conn.close()
        
        # 5. Verify old message is still there, recent is gone
        timeline_after_restore = self.make_request(
            "GET",
            f"/_matrix/client/r0/rooms/{room_id}/messages?dir=b&limit=10",
            access_token=self.user1_tok,
        )
        
        messages_after_restore = [
            e["content"]["body"] for e in timeline_after_restore.json_body["chunk"]
            if e["type"] == "m.room.message"
        ]
        
        self.assertEqual(len(messages_after_restore), 1)
        self.assertIn("Old message", messages_after_restore)
        self.assertNotIn("Recent message", messages_after_restore)
        
        # 6. Simulate significant time passing and recover the recent message
        time.sleep(1)
        
        # Extract recent message for recovery (need to do this manually since it's lost)
        recent_message_event = {
            "event_id": msg2["event_id"],
            "type": "m.room.message",
            "sender": self.user1,
            "room_id": room_id,
            "content": {"msgtype": "m.text", "body": "Recent message"},
            "origin_server_ts": original_timestamps["Recent message"],
            "auth_events": [],
            "prev_events": [],
        }
        
        inject_response = self.make_request(
            "POST",
            self.injection_url,
            content={"events": [recent_message_event]},
            access_token=self.admin_user_tok,
        )
        
        self.assertEqual(HTTPStatus.OK, inject_response.code)
        self.assertEqual(inject_response.json_body["injected_events"], 1)
        
        # 7. Verify timestamps are preserved
        timeline_recovered = self.make_request(
            "GET",
            f"/_matrix/client/r0/rooms/{room_id}/messages?dir=b&limit=10",
            access_token=self.user1_tok,
        )
        
        recovered_timestamps = {}
        for event in timeline_recovered.json_body["chunk"]:
            if event["type"] == "m.room.message":
                body = event["content"]["body"]
                recovered_timestamps[body] = event["origin_server_ts"]
        
        # Check both messages are present with original timestamps
        self.assertEqual(len(recovered_timestamps), 2)
        self.assertEqual(
            recovered_timestamps["Old message"],
            original_timestamps["Old message"],
            "Old message timestamp should be preserved"
        )
        self.assertEqual(
            recovered_timestamps["Recent message"],
            original_timestamps["Recent message"],
            "Recent message timestamp should be preserved"
        )
        
        # Verify chronological order is maintained
        old_ts = recovered_timestamps["Old message"]
        recent_ts = recovered_timestamps["Recent message"]
        self.assertLess(old_ts, recent_ts, "Messages should maintain chronological order")

    def test_minimal_event_recovery(self) -> None:
        """Test recovery with minimal required fields."""
        # This tests that we can simplify events for disaster recovery
        # by only keeping essential fields
        
        # 1. Create room with message
        room_id = self.helper.create_room_as(self.user1, tok=self.user1_tok)
        self.helper.send(room_id, "Test message", tok=self.user1_tok)
        
        # 2. Extract events and create minimal versions
        full_events = self.extract_events_for_room(room_id)
        
        # Create minimal events with only required fields
        minimal_events = []
        for event in full_events:
            minimal = {
                "type": event["type"],
                "sender": event["sender"],
                "room_id": event["room_id"],
                "content": event["content"],
                "origin_server_ts": event["origin_server_ts"],
            }
            
            # Add state_key for state events
            if "state_key" in event:
                minimal["state_key"] = event["state_key"]
                
            # Add required fields for specific event types
            if event["type"] == EventTypes.Create:
                # Room create needs these
                minimal["auth_events"] = []
                minimal["prev_events"] = []
            else:
                # Other events need proper auth/prev references
                # For testing, we'll include them from original
                minimal["auth_events"] = event.get("auth_events", [])
                minimal["prev_events"] = event.get("prev_events", [])
                
            minimal_events.append(minimal)
        
        # 3. Backup and restore
        backup_conn = self.backup_database()
        self.restore_database(backup_conn)
        backup_conn.close()
        
        # 4. Recover with minimal events
        inject_response = self.make_request(
            "POST",
            self.injection_url,
            content={"events": minimal_events},
            access_token=self.admin_user_tok,
        )
        
        self.assertEqual(HTTPStatus.OK, inject_response.code)
        self.assertEqual(inject_response.json_body["injected_events"], len(minimal_events))
        
        # 5. Verify recovery worked
        sync_response = self.make_request(
            "GET",
            "/_matrix/client/r0/sync",
            access_token=self.user1_tok,
        )
        
        joined_rooms = sync_response.json_body["rooms"]["join"]
        self.assertIn(room_id, joined_rooms)
        
        # Check message is visible
        events = joined_rooms[room_id]["timeline"]["events"]
        messages = [e for e in events if e["type"] == "m.room.message"]
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]["content"]["body"], "Test message")