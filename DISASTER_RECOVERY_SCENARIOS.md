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