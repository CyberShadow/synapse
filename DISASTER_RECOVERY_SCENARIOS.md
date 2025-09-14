# Disaster Recovery Test Scenarios

This document outlines the disaster recovery scenarios covered by the Synapse bulk event injection API and their corresponding test implementations.

## Scenario 1: Basic Message Recovery After Partial Data Loss

**Test Location:** `test_disaster_recovery_integration.py::test_basic_recovery()`

1. User creates a room and sends "Message 1"
2. User sends "Message 2" 
3. **Server database is backed up**
4. User sends "Message 3"
5. User sends "Message 4"
6. **Server crashes and database is restored from backup** (loses Messages 3 & 4)
7. Admin extracts Messages 3 & 4 from external source:
   - From federation: Events arrive without internal fields (auth_events/prev_events/depth)
   - From logs: May only have basic event data
   - From backups: May have complete data
8. **Admin injects Messages 3 & 4 via bulk injection API** (fields are auto-populated if missing)
10. **Expected result:** All 4 messages are visible in correct order, room remains functional

## Scenario 2: User Membership Recovery

**Test Location:** `test_disaster_recovery_integration.py::test_membership_recovery()`

1. Alice creates a public room
2. Alice sends "Initial message"
3. **Server database is backed up**
4. Bob joins the room
5. Bob sends "Hello from Bob"
6. **Server crashes and database is restored from backup** (loses Bob's membership)
7. Bob cannot access the room anymore (403 Forbidden)
8. Admin extracts Bob's join event and message from external source
9. **Admin injects Bob's membership event and message**
10. **Expected result:** Bob can access the room again and see all messages

## Scenario 3: Federation Recovery with Missing Fields

**Test Location:** `test_disaster_recovery_integration.py::test_basic_recovery()` (integrated)

1. Room exists with some messages
2. **Server database is backed up**
3. More messages are sent
4. **Server crashes and database is restored from backup**
5. Admin requests missing events from federation partners
6. **Federation servers provide events but only with basic fields:**
   - ✓ event_id, type, sender, room_id, content, origin_server_ts
   - ✗ auth_events (internal field, not sent over federation)
   - ✗ prev_events (internal field, not sent over federation)
   - ✗ depth (internal field, not sent over federation)
7. **Admin injects events as received from federation**
8. **Expected result:** 
   - auth_events are automatically reconstructed based on room state
   - prev_events are set to current room forward extremities
   - depth is calculated from prev_events
   - Events are properly integrated into room timeline

## Scenario 4: Preserved Timestamps After Recovery

**Test Location:** `test_disaster_recovery_integration.py::test_preserved_timestamps()`

1. User sends "Old message" at time T1
2. **Server database is backed up**
3. User waits 2 seconds
4. User sends "Recent message" at time T2
5. **Server crashes and database is restored from backup** (loses "Recent message")
6. Significant time passes (T3, much later than T2)
7. Admin extracts "Recent message" with original timestamp T2
8. **Admin injects "Recent message" with preserved timestamp**
9. **Expected result:** "Recent message" appears with original timestamp T2, not current time T3

## Scenario 5: Room Created After Backup

**Test Location:** `test_disaster_recovery_integration.py::test_room_created_after_backup()`

1. **Server database is backed up**
2. User creates a new room "Room B"
3. User sends messages in Room B
4. Other users join Room B
5. **Server crashes and database is restored from backup**
6. Room B doesn't exist at all (created after backup point)
7. Admin extracts complete Room B history from federation or logs
8. **Admin injects all Room B events** (create event, memberships, messages)
9. **Expected result:**
   - Room B is fully reconstructed from scratch
   - All users who joined can access it again
   - All messages are restored
   - Room continues to function normally

## Scenario 6: Complete Room Recovery (Corruption Case)

**Test Location:** `test_disaster_recovery_integration.py::test_room_functionality_after_recovery()`

1. User creates room with initial state and messages
2. **Room data is corrupted or lost** (database corruption, not backup/restore)
3. Admin has complete room event history from external source
4. **Admin injects all room events** (create event, memberships, state, messages)
5. **Expected result:**
   - Room is fully reconstructed
   - New users can join the room
   - New messages can be sent
   - All historical messages are visible
   - Room state is correct

## Scenario 7: Disaster Recovery from Incomplete Federation Data

**Test Location:** `tests/rest/admin/test_disaster_recovery.py` (unit tests)

1. Server participates in federated room
2. **Local server data is lost**
3. Admin requests event history from remote federation servers
4. Remote servers provide events but some internal fields are missing
5. **Admin injects federated events** with automatic field reconstruction:
   - auth_events: Determined from event type and current room state
   - prev_events: Set to current forward extremities (up to 5)
   - depth: Calculated as max(prev_events depth) + 1
6. **Expected result:** Room is accessible and functional despite incomplete data

## Scenario 8: Batch Recovery of Multiple Rooms

**Test Location:** Demonstrated in integration tests

1. Multiple rooms exist with various events
2. **Server experiences data loss across multiple rooms**
3. Admin collects events from various sources (backups, federation, logs)
4. **Admin injects events in bulk** (single API call with events from multiple rooms)
5. **Expected result:** All affected rooms are recovered in a single operation

## Scenario 9: Minimal Event Recovery

**Test Location:** `test_disaster_recovery_integration.py::test_minimal_event_recovery()`

1. Room exists with full event data
2. **Admin only has access to minimal event fields** (from logs or simplified backups)
3. Events are stripped to only essential fields:
   - ✓ event_id, type, sender, room_id, content, origin_server_ts
   - ✓ state_key (for state events)
   - ✗ auth_events, prev_events, depth, signatures, hashes, etc.
4. **Admin injects minimal events**
5. **Expected result:**
   - Events are accepted and processed
   - Missing fields are automatically populated
   - Room remains functional

## Scenario 10: Missing Events Between Existing

**Test Location:** `test_disaster_recovery_integration.py::test_missing_events_between_existing()`

1. User sends "Message 1" in a room
2. User sends "Message 2", "Message 3", "Message 4"
3. User sends "Message 5"
4. **Messages 2-4 are lost** (partial database corruption, selective data loss)
5. Room timeline shows: Message 1 → Message 5 (gap in conversation)
6. Admin recovers Messages 2-4 from backup/logs
7. **Admin injects missing messages with correct timestamps**
8. **Expected result:**
   - All messages appear in correct chronological order
   - Timeline integrity is restored: Message 1 → 2 → 3 → 4 → 5
   - No duplicate messages
   - Conversation flow is natural

## Scenario 11: Historical Events with Old Timestamps

**Test Location:** `test_disaster_recovery_integration.py::test_historical_events_pagination()`

1. Room exists with current messages
2. **Admin discovers historical messages from months/years ago** (old backups, archives)
3. Historical events have very old timestamps (e.g., 30 days ago)
4. Current timeline shows only recent messages
5. **Admin injects historical events with preserved timestamps**
6. **Expected result:**
   - Historical messages are accessible via pagination
   - Messages appear in correct chronological order
   - Clients can paginate back to see full history
   - No timeline corruption despite large timestamp differences

## Test Implementation Details

### Integration Tests
- **File:** `test_disaster_recovery_integration.py`
- **Purpose:** End-to-end testing with real Synapse process lifecycle
- **Features:**
  - Real database backup/restore using SQLite backup API
  - Process shutdown and restart simulation
  - Multi-user scenarios with separate access tokens
  - Federation recovery simulation

### Unit Tests
- **File:** `tests/rest/admin/test_disaster_recovery.py`
- **Purpose:** Detailed API behavior testing
- **Features:**
  - Event validation
  - Error handling
  - Edge cases
  - Room state management

### Key Test Assertions

1. **Event Visibility:** Recovered events appear in room timeline via `/messages` and `/sync`
2. **Event Ordering:** Events maintain correct chronological order based on timestamps
3. **Room Functionality:** Rooms remain fully functional after recovery (can send messages, invite users, etc.)
4. **State Consistency:** Room state (memberships, power levels, etc.) is correctly maintained
5. **Federation Compatibility:** Events without internal fields are properly integrated
6. **Idempotency:** Re-injecting existing events is a safe no-op

### Running the Tests

```bash
# Run all integration tests
python test_disaster_recovery_integration.py all

# Run specific scenario
python test_disaster_recovery_integration.py basic
python test_disaster_recovery_integration.py membership
python test_disaster_recovery_integration.py timestamps
python test_disaster_recovery_integration.py functionality
python test_disaster_recovery_integration.py room-after-backup
python test_disaster_recovery_integration.py minimal
python test_disaster_recovery_integration.py missing-between
python test_disaster_recovery_integration.py historical

# Run in container
podman run --rm -v ".:/synapse" -w /synapse --entrypoint="" localhost/synapse-dev:latest \
  bash -c "PYTHONPATH=/synapse python -u test_disaster_recovery_integration.py all"
```

## Scenario 12: Encrypted Room Recovery

**Test Location:** `test_disaster_recovery_integration.py::test_encrypted_room_recovery()`

1. User creates an encrypted room (m.room.encryption state event)
2. Users exchange encrypted messages using E2E encryption
3. **Server database is backed up**
4. More encrypted messages are sent
5. Device keys are rotated/updated
6. **Server crashes and database is restored from backup**
7. Admin extracts lost encrypted messages from external source
8. **Admin injects encrypted messages via bulk injection API**
9. **Expected result:**
   - Encrypted messages are stored correctly
   - Messages remain encrypted (server cannot decrypt)
   - Clients with proper keys can decrypt historical messages
   - Room encryption state is preserved
   - New encrypted messages can be sent

## Scenario 13: State Event Conflicts During Recovery

**Test Location:** `test_disaster_recovery_integration.py::test_state_conflict_recovery()`

1. Room has power levels: Admin=100, User=0
2. Admin changes topic to "Important Meeting"
3. **Server database is backed up**
4. Admin promotes User to power level 50
5. User changes topic to "Casual Chat"
6. Admin changes topic to "Executive Meeting"
7. **Server crashes and database is restored from backup**
8. Admin has conflicting topic changes from external source
9. **Admin injects all state events with proper auth chains**
10. **Expected result:**
    - State resolution algorithm correctly resolves conflicts
    - Final topic reflects the event with highest power level
    - Room state remains consistent
    - State timeline is preserved for audit purposes

## Scenario 14: Redaction Event Recovery

**Test Location:** `test_disaster_recovery_integration.py::test_redaction_recovery()`

1. User sends inappropriate message "Confidential data XYZ"
2. User sends normal message "Hello everyone"
3. **Server database is backed up**
4. Moderator redacts the inappropriate message
5. User sends "Thanks for removing that"
6. **Server crashes and database is restored from backup**
7. Inappropriate message is visible again (redaction lost)
8. Admin extracts redaction event from audit logs
9. **Admin injects the redaction event**
10. **Expected result:**
    - Inappropriate message is redacted again
    - Redaction reason is preserved
    - Message content is properly removed
    - Clients see redacted placeholder
    - Subsequent messages remain intact

## Scenario 15: Media Event Recovery

**Test Location:** `test_disaster_recovery_integration.py::test_media_event_recovery()` (to be implemented)

1. User uploads image.jpg via media API
2. User sends m.room.message with m.image referencing the media
3. **Server database is backed up**
4. User uploads document.pdf
5. User sends m.room.message with m.file referencing the document
6. **Server crashes and database is restored from backup**
7. Media events are lost but media files may still exist on disk
8. Admin recovers media events from logs/federation
9. **Admin injects media message events**
10. **Expected result:**
    - Media messages appear in timeline
    - Media URLs remain valid if files exist
    - Missing media files show as unavailable
    - Thumbnails work if still cached
    - New media can be uploaded

## Scenario 16: Ban/Kick Event Recovery  

**Test Location:** `test_disaster_recovery_integration.py::test_ban_kick_recovery()` (to be implemented)

1. Alice creates a room and invites Bob and Charlie
2. Bob and Charlie join and participate
3. **Server database is backed up**
4. Bob becomes disruptive
5. Alice kicks Bob from the room
6. Charlie continues chatting
7. Bob tries to rejoin but Alice bans Bob
8. **Server crashes and database is restored from backup**
9. Bob is back in the room (kick/ban events lost)
10. Admin extracts kick and ban events
11. **Admin injects kick and ban membership events**
12. **Expected result:**
    - Bob is removed from the room
    - Bob cannot rejoin (ban is enforced)
    - Kick/ban reasons are preserved
    - Audit trail shows moderation actions
    - Room membership state is correct

## Scenario 17: Invite-Only Room Access Loss

**Test Location:** `test_disaster_recovery_integration.py::test_invite_only_room_access_loss()`

1. Alice creates private invite-only room
2. Alice invites Bob and Charlie via direct invites
3. Bob and Charlie accept invites and join
4. **Server database is backed up**
5. Alice invites David
6. David joins the room
7. All users exchange messages
8. **Server crashes and database is restored from backup**
9. David loses access (his invite and join are lost)
10. David cannot rejoin without new invite
11. Admin recovers David's invite and join events
12. **Admin injects David's membership events**
13. **Expected result:**
    - David regains access to the room
    - David can see all messages
    - No duplicate invites needed
    - Room privacy settings maintained
    - Invite-only restriction still enforced

## Scenario 18: Large-Scale Batch Recovery

**Test Location:** `test_disaster_recovery_integration.py::test_large_batch_recovery()` (to be implemented)

1. Active server with 50+ rooms, 200+ users
2. Continuous activity across multiple rooms
3. **Server database is backed up**
4. Heavy activity period: 10,000+ events across all rooms
5. Multiple room creations, joins, messages, media uploads
6. **Server crashes and database is restored from backup**
7. Thousands of events lost across dozens of rooms
8. Admin extracts events from distributed sources
9. **Admin injects 10,000+ events in batches**
10. **Expected result:**
    - All rooms restored to correct state
    - No timeouts or memory issues
    - Batch processing completes successfully
    - Event ordering preserved per room
    - Server performance remains acceptable

## Scenario 19: Room Upgrade Chain Recovery

**Test Location:** `test_disaster_recovery_integration.py::test_room_upgrade_recovery()` (to be implemented)

1. Room v1 exists with history and members
2. Admin upgrades room to v6 (creates new room with tombstone)
3. Users migrate to new room, old room is tombstoned
4. **Server database is backed up**
5. Activity continues in v6 room
6. Admin upgrades to v10 for new features
7. More activity in v10 room
8. **Server crashes and database is restored from backup**
9. v10 room doesn't exist, users stuck in v6 room
10. Admin recovers entire upgrade chain and events
11. **Admin injects room upgrade events and new room state**
12. **Expected result:**
    - Room upgrade chain is restored
    - Tombstone events point to correct rooms
    - Users can follow upgrade path
    - Room versions are correct
    - Historical messages accessible

## Scenario 20: Partial Room State Recovery

**Test Location:** `test_disaster_recovery_integration.py::test_partial_state_recovery()` (to be implemented)

1. Complex room with multiple state events:
   - Custom power levels for 10+ users
   - Room avatar and topic
   - Guest access and history visibility settings
   - Server ACLs
2. **Server database is backed up**
3. Various state changes occur
4. **Server crashes with partial database corruption**
5. Some state events corrupted, others intact
6. Room partially functional but state inconsistent
7. Admin identifies corrupted vs intact state
8. Admin extracts only corrupted state events
9. **Admin injects only the corrupted state events**
10. **Expected result:**
    - Corrupted state is repaired
    - Intact state remains unchanged
    - No state duplication
    - State resolution handles conflicts
    - Room fully functional

## Scenario 21: Federation Split-Brain Recovery

**Test Location:** `test_disaster_recovery_integration.py::test_federation_split_brain()` (to be implemented)

1. Federated room between servers A, B, and C
2. Network partition: A can't reach B and C
3. Users on A continue sending messages
4. Users on B and C continue conversation
5. **Two separate event graphs develop**
6. Network partition heals
7. Servers try to reconcile but have conflicts
8. **Server A crashes before full reconciliation**
9. Admin must merge divergent timelines
10. Admin extracts events from both forks
11. **Admin injects events preserving both timelines**
12. **Expected result:**
    - Both conversation forks are preserved
    - Events are ordered by timestamp
    - State conflicts resolved by auth rules
    - Federation continues normally
    - No events are lost from either fork