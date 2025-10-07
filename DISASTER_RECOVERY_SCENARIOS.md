# Disaster Recovery Test Scenarios

This document outlines the disaster recovery scenarios covered by the Synapse bulk event injection API.

## Implementation Status

**✅ Implemented & Tested (10 scenarios):**
- Scenarios 1, 2, 4, 5, 6, 8, 10, 11, 12-14, 17, 22

**❌ Removed (3 scenarios - based on incorrect assumptions):**
- Scenarios 3, 7, 9: Assumed federation lacks required fields (INCORRECT - see EVENT_ID_PRESERVATION_RESEARCH.md)

**📋 Documented but Not Yet Implemented (6 scenarios):**
- Scenarios 15, 16, 18, 19, 20, 21

## Data Source Requirements

**Supported (Complete Events):**
- ✅ **Database exports**: `event_json` table contains complete PDU data
- ✅ **Federation**: Server-Server API includes ALL required fields per Matrix Spec

**Not Supported (Incomplete Events):**
- ❌ **Client API** (`/messages`, `/sync`): Missing auth_events, prev_events, depth, hashes, signatures

See EVENT_ID_PRESERVATION_RESEARCH.md for spec citations and technical details.

---

## Scenario 1: Basic Message Recovery After Partial Data Loss

**Test Location:** `test_disaster_recovery_integration.py::test_basic_recovery()`

1. User creates a room and sends "Message 1" and "Message 2"
2. **Server database is backed up**
3. User sends "Message 3" and "Message 4"
4. **Server crashes and database is restored from backup** (loses Messages 3 & 4)
5. Admin extracts Messages 3 & 4 from database export or federation
6. **Admin injects complete events via bulk injection API**
7. **Expected result:** All 4 messages visible in correct order, room remains functional

**Data Source:** Database export (event_json table) with complete PDU data

## Scenario 2: User Membership Recovery

**Test Location:** `test_disaster_recovery_integration.py::test_membership_recovery()`

1. Alice creates a public room and sends "Initial message"
2. **Server database is backed up**
3. Bob joins the room and sends "Hello from Bob"
4. **Server crashes and database is restored from backup** (loses Bob's membership)
5. Bob cannot access the room anymore (403 Forbidden)
6. Admin extracts Bob's join event and message from database/federation
7. **Admin injects Bob's membership events**
8. **Expected result:** Bob can access the room again and see all messages

## Scenario 4: Preserved Timestamps After Recovery

**Test Location:** `test_disaster_recovery_integration.py::test_preserved_timestamps()`

1. User sends "Old message" at time T1
2. **Server database is backed up**
3. User waits 2 seconds, then sends "Recent message" at time T2
4. **Server crashes and database is restored from backup**
5. Significant time passes (T3, much later than T2)
6. Admin extracts "Recent message" with original timestamp T2
7. **Admin injects event with preserved timestamp**
8. **Expected result:** "Recent message" appears with original timestamp T2, not T3

## Scenario 5: Room Created After Backup

**Test Location:** `test_disaster_recovery_integration.py::test_room_created_after_backup()`

1. **Server database is backed up**
2. User creates a new room "Room B", sends messages, others join
3. **Server crashes and database is restored from backup**
4. Room B doesn't exist at all (created after backup point)
5. Admin extracts complete Room B history from federation or backup
6. **Admin injects all Room B events** (create event, memberships, messages)
7. **Expected result:**
   - Room B is fully reconstructed from scratch
   - All users who joined can access it again
   - Room continues to function normally

## Scenario 6: Complete Room Recovery (Corruption Case)

**Test Location:** `test_disaster_recovery_integration.py::test_room_functionality_after_recovery()`

1. User creates room with initial state and messages
2. **Room data is corrupted or lost** (database corruption, not backup/restore)
3. Admin has complete room event history from external source
4. **Admin injects all room events** (create event, memberships, state, messages)
5. **Expected result:**
   - Room is fully reconstructed
   - New users can join, new messages can be sent
   - All historical messages are visible
   - Room state is correct

## Scenario 8: Batch Recovery of Multiple Rooms

**Test Location:** Demonstrated in integration tests

1. Multiple rooms exist with various events
2. **Server experiences data loss across multiple rooms**
3. Admin collects events from database exports or federation
4. **Admin injects events in bulk** (single API call with events from multiple rooms)
5. **Expected result:** All affected rooms are recovered in a single operation

## Scenario 10: Missing Events Between Existing

**Test Location:** `test_disaster_recovery_integration.py::test_missing_events_between_existing()`

1. User sends "Message 1", then "Message 2", "Message 3", "Message 4", then "Message 5"
2. **Messages 2-4 are lost** (partial database corruption)
3. Room timeline shows: Message 1 → Message 5 (gap in conversation)
4. Admin recovers Messages 2-4 from backup
5. **Admin injects missing messages with correct timestamps**
6. **Expected result:**
   - All messages appear in correct chronological order
   - Timeline integrity restored: Message 1 → 2 → 3 → 4 → 5
   - No duplicate messages

## Scenario 11: Historical Events with Old Timestamps

**Test Location:** `test_disaster_recovery_integration.py::test_historical_events_pagination()`

1. Room exists with current messages
2. **Admin discovers historical messages from months/years ago** (old backups)
3. Historical events have very old timestamps (e.g., 30 days ago)
4. **Admin injects historical events with preserved timestamps**
5. **Expected result:**
   - Historical messages are accessible via pagination
   - Messages appear in correct chronological order
   - No timeline corruption despite large timestamp differences

## Scenario 12: Encrypted Room Recovery

**Test Location:** `test_disaster_recovery_integration.py::test_encrypted_room_recovery()`

1. User creates an encrypted room (m.room.encryption state event)
2. Users exchange encrypted messages using E2E encryption
3. **Server database is backed up**
4. More encrypted messages are sent, device keys are rotated
5. **Server crashes and database is restored from backup**
6. Admin extracts lost encrypted messages from external source
7. **Admin injects encrypted messages via bulk injection API**
8. **Expected result:**
   - Encrypted messages are stored correctly
   - Messages remain encrypted (server cannot decrypt)
   - Clients with proper keys can decrypt historical messages
   - Room encryption state is preserved

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

## Scenario 14: Redaction Event Recovery

**Test Location:** `test_disaster_recovery_integration.py::test_redaction_recovery()`

1. User sends inappropriate message "Confidential data XYZ"
2. **Server database is backed up**
3. Moderator redacts the inappropriate message
4. **Server crashes and database is restored from backup**
5. Inappropriate message is visible again (redaction lost)
6. Admin extracts redaction event from audit logs
7. **Admin injects the redaction event**
8. **Expected result:**
   - Inappropriate message is redacted again
   - Redaction reason is preserved
   - Message content is properly removed

## Scenario 15: Media Event Recovery

**Status:** 📋 Not yet implemented

1. User uploads media files and sends m.room.message events referencing them
2. **Server crashes and media events are lost**
3. Admin recovers media events from logs/federation
4. **Admin injects media message events**
5. **Expected result:** Media messages appear in timeline with correct URLs

## Scenario 16: Ban/Kick Event Recovery

**Status:** 📋 Not yet implemented

1. Alice creates a room and invites Bob and Charlie
2. **Server database is backed up**
3. Alice kicks Bob, then bans Bob
4. **Server crashes and database is restored from backup**
5. Bob is back in the room (kick/ban events lost)
6. Admin extracts kick and ban events
7. **Admin injects kick and ban membership events**
8. **Expected result:**
   - Bob is removed from the room
   - Bob cannot rejoin (ban is enforced)
   - Audit trail shows moderation actions

## Scenario 17: Invite-Only Room Access Loss

**Test Location:** `test_disaster_recovery_integration.py::test_invite_only_room_access_loss()`

1. Alice creates private invite-only room
2. Alice invites Bob and Charlie, they join
3. **Server database is backed up**
4. Alice invites David, David joins
5. **Server crashes and database is restored from backup**
6. David loses access (his invite and join are lost)
7. Admin recovers David's invite and join events
8. **Admin injects David's membership events**
9. **Expected result:**
   - David regains access to the room
   - No duplicate invites needed
   - Room privacy settings maintained

## Scenario 18: Large-Scale Batch Recovery

**Status:** 📋 Not yet implemented

1. Active server with 50+ rooms, 200+ users
2. **Server database is backed up**
3. Heavy activity period: 10,000+ events across all rooms
4. **Server crashes and database is restored from backup**
5. Admin extracts events from distributed sources
6. **Admin injects 10,000+ events in batches**
7. **Expected result:**
   - All rooms restored to correct state
   - No timeouts or memory issues
   - Server performance remains acceptable

## Scenario 19: Room Upgrade Chain Recovery

**Status:** 📋 Not yet implemented

1. Room v1 exists with history
2. Admin upgrades room to v6, then to v10
3. **Server crashes and v10 room is lost**
4. Admin recovers entire upgrade chain and events
5. **Admin injects room upgrade events and new room state**
6. **Expected result:**
   - Room upgrade chain is restored
   - Tombstone events point to correct rooms
   - Users can follow upgrade path

## Scenario 20: Partial Room State Recovery

**Status:** 📋 Not yet implemented

1. Complex room with multiple state events
2. **Server crashes with partial database corruption**
3. Some state events corrupted, others intact
4. Admin identifies corrupted vs intact state
5. **Admin injects only the corrupted state events**
6. **Expected result:**
   - Corrupted state is repaired
   - Intact state remains unchanged
   - No state duplication

## Scenario 21: Federation Split-Brain Recovery

**Status:** 📋 Not yet implemented

1. Federated room between servers A, B, and C
2. Network partition: A can't reach B and C
3. Users on A and B/C continue separately
4. **Two separate event graphs develop**
5. **Server A crashes before full reconciliation**
6. Admin must merge divergent timelines
7. **Admin injects events preserving both timelines**
8. **Expected result:**
   - Both conversation forks are preserved
   - State conflicts resolved by auth rules
   - No events lost from either fork

## Scenario 22: Event ID Preservation During Recovery

**Test Location:** `test_disaster_recovery_integration.py::test_event_id_preservation()`

**Current Status:** ✅ IMPLEMENTED - Requires complete events

1. User sends messages in a room (room version 3+)
2. **Server database is backed up**
3. More messages are sent with specific event IDs
4. **Server crashes and database is restored from backup**
5. Admin recovers events from database export or federation (complete PDU data)
6. **Admin injects events with their original event IDs**
7. **Expected result:**
   - Recovered events MUST have the same event IDs as originals
   - No duplicate events are created
   - Federation partners recognize events as the same ones
   - Event ID hash validation passes

**Critical Requirement:** For room versions 3+, event IDs are content-addressable (hash of canonical JSON). The bulk injection API validates that provided event_id matches calculated event_id. Mismatches are rejected to prevent federation desynchronization.

**Implementation:** Complete events (with auth_events, prev_events, depth, hashes, signatures) preserve event IDs. The API requires ALL these fields and validates event_id matches.

---

## Test Implementation Details

### Integration Tests
- **File:** `test_disaster_recovery_integration.py`
- **Purpose:** End-to-end testing with real Synapse process lifecycle
- **Features:**
  - Real database backup/restore using SQLite backup API
  - Process shutdown and restart simulation
  - Multi-user scenarios with separate access tokens
  - Complete event injection from database exports

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
3. **Room Functionality:** Rooms remain fully functional after recovery
4. **State Consistency:** Room state (memberships, power levels, etc.) is correctly maintained
5. **Event ID Preservation:** Events maintain their original IDs (room v3+)
6. **Idempotency:** Re-injecting existing events is a safe no-op
7. **Forward Extremities:** Room DAG forward extremities are correctly updated
8. **Current State:** `current_state_events` table is correctly updated

### Running the Tests

```bash
# Run all integration tests
./dependencies/synapse/run_disaster_recovery_integration_test.sh all

# Run specific scenario
./dependencies/synapse/run_disaster_recovery_integration_test.sh basic
./dependencies/synapse/run_disaster_recovery_integration_test.sh membership
./dependencies/synapse/run_disaster_recovery_integration_test.sh timestamps
./dependencies/synapse/run_disaster_recovery_integration_test.sh current-state
./dependencies/synapse/run_disaster_recovery_integration_test.sh event-id

# Run in container (from dependencies/synapse directory)
podman run --rm -v ".:/synapse" -w /synapse --entrypoint="" localhost/synapse-dev:latest \
  bash -c "PYTHONPATH=/synapse python -u test_disaster_recovery_integration.py all"
```

## Scenarios Removed

### ~~Scenario 3: Federation Recovery with Missing Fields~~ ❌ INCORRECT ASSUMPTION

**Why removed:** Based on false assumption that federation lacks internal fields. Per Matrix Spec, federation PDUs MUST include auth_events, prev_events, depth, hashes, and signatures. See EVENT_ID_PRESERVATION_RESEARCH.md for spec citations.

### ~~Scenario 7: Disaster Recovery from Incomplete Federation Data~~ ❌ INCORRECT ASSUMPTION

**Why removed:** Same as Scenario 3. Federation provides complete PDU data per spec.

### ~~Scenario 9: Minimal Event Recovery~~ ❌ NOT SUPPORTED

**Why removed:** The bulk injection API now requires complete events to ensure event ID preservation and prevent federation desynchronization. Incomplete events (e.g., from Client API) cannot preserve event IDs for room v3+ and are rejected.

**Note:** If you have events from Client API sources, they cannot be used for disaster recovery because they lack the cryptographic data needed to preserve event IDs.
