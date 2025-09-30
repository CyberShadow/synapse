# Event ID Preservation Research for Disaster Recovery

## Ultimate Goal
Enable disaster recovery in Synapse with the ability to preserve original event IDs for room versions 3+, preventing duplicate events and maintaining federation consistency.

## Background
In Matrix room versions 3+, event IDs are content-addressable: `event_id = "$" + base64(sha256(canonical_json(event_dict)))`. This means the event ID is deterministically calculated from the event's content.

## Current Findings

### 1. The Core Problem
- **Room v1/v2**: Event IDs can be provided externally and preserved
- **Room v3+**: Event IDs are always calculated from content hash
- Synapse's `FrozenEventV2.__init__` has `assert "event_id" not in event_dict` (line 421)
- Our bulk injection API removes event_id before creating events, causing new IDs

### 2. Event ID Calculation Requirements
For room v3+, the following fields affect the event ID hash:
- `auth_events` - List of auth event IDs
- `content` - The event content
- `depth` - Integer depth in the DAG
- `hashes` - Contains SHA256 hash (circular dependency!)
- `origin` - Origin server name
- `origin_server_ts` - Timestamp
- `prev_events` - List of previous event IDs  
- `room_id` - The room ID
- `sender` - User ID of sender
- `signatures` - Cryptographic signatures
- `type` - Event type
- `state_key` - (if present) For state events

### 3. Why We Can't Preserve IDs Currently

#### Missing Data from Client API
When fetching events via `/rooms/{roomId}/messages`, we only get:
```json
{
    "event_id": "$abc...",
    "type": "m.room.message", 
    "sender": "@user:server",
    "content": {"msgtype": "m.text", "body": "Hello"},
    "origin_server_ts": 1234567890
}
```

Missing: `auth_events`, `prev_events`, `depth`, `origin`, `hashes`, `signatures`

#### Database Contains Complete Data
The `event_json` table DOES contain all fields including hashes and signatures, but even with this complete data, our current implementation generates new IDs.

### 4. Federation Behavior (Open Question)

**Key Question**: How does federation handle event IDs for room v3+?

Hypothesis:
- Federation receives events with all fields needed for ID calculation
- Synapse likely validates: `received_event_id == calculate_event_id(event_content)`
- If validation passes, the event is accepted
- If validation fails, the event is rejected

Evidence:
- `event_from_pdu_json()` uses the same `make_event_from_dict()` that forbids event_id
- This suggests federation events for v3+ don't include event_id in the PDU
- The ID must be calculated from content

## Avenues for Future Research

### 1. Federation Event Processing
- [ ] Trace how `on_receive_pdu` processes incoming federation events
- [ ] Verify if federation PDUs for v3+ rooms include or exclude event_id
- [ ] Check how Synapse validates event ID matches content hash
- [ ] Find where calculated event_id is compared to received event_id

### 2. Alternative Event Creation Paths
- [ ] Investigate if there's a way to create EventBase objects that preserve IDs
- [ ] Check if federation has special event creation logic we can reuse
- [ ] Look for "raw" event insertion methods that bypass validation

### 3. Database Direct Insertion
- [ ] Research if events can be inserted directly into database tables
- [ ] Understand which tables need to be updated (events, event_json, etc.)
- [ ] Identify required indexes and constraints

### 4. Exact Data Reproduction
- [ ] Test if providing byte-perfect event data produces the same ID
- [ ] Investigate canonical JSON ordering requirements
- [ ] Check if signatures/hashes can be preserved exactly

## Potential Solutions

### Option 1: Modify Event Creation Path
Add a "disaster recovery mode" to event creation that:
- Allows event_id in dict for v3+ rooms
- Validates that provided ID matches calculated hash
- Rejects if mismatch (security)

### Option 2: Use Federation Code Path  
Find and use the exact code path federation uses, which might:
- Already handle ID validation properly
- Have fewer restrictions on event format

### Option 3: Direct Database Insertion
Bypass event creation entirely:
- Insert events directly into database
- Update all required tables and indexes
- Risk: Could break invariants if done incorrectly

### Option 4: Two-Phase Recovery
1. First phase: Recover events (new IDs)
2. Second phase: Run migration to fix IDs in database
3. Complex and risky

## Next Steps for Testing

### 1. Test Exact Data Reproduction
Create a test that:
- Gets complete event JSON from database
- Extracts only the fields used in ID calculation
- Ensures canonical JSON ordering
- Injects this exact data
- Checks if IDs match

### 2. Test Direct Event Object Creation
Try to bypass `make_event_from_dict` by:
- Creating FrozenEventV3 objects directly
- Setting internal fields manually
- Using federation persistence methods

### 3. Test Database Direct Insertion
As a last resort:
- Extract complete event data
- Insert directly into event tables
- Update all necessary indexes
- Verify room remains functional

## Implementation Approaches

### Approach 1: Modify FrozenEventV2/V3 Classes
Remove or conditionally bypass the `assert "event_id" not in event_dict` check when in disaster recovery mode.

### Approach 2: Create New Event Class
Create `DisasterRecoveryEvent` class that:
- Extends FrozenEventV3
- Allows event_id in constructor
- Validates ID matches calculated hash

### Approach 3: Use Federation Code Path
Modify bulk injection to use the exact same code path as federation event processing, which must handle pre-existing IDs somehow.

## Test Status

Current test (`test_event_id_preservation`) demonstrates:
- ✅ Successfully gets complete event data from database
- ✅ Events are injected without errors
- ❌ Event IDs change even with complete data
- ❌ Original IDs are not preserved

## Open Questions

1. Does federation include event_id in PDUs for v3+ rooms?
2. Where does Synapse validate that event ID matches content hash?
3. Can we create EventBase objects without going through make_event_from_dict?
4. What's the minimal set of fields needed for byte-perfect ID reproduction?
5. How does Synapse handle the circular dependency (hashes field contains the hash)?

## Federation Research Results

### 1. How Federation Handles Event IDs

**Answer**: Federation does NOT include event_id in PDUs for room v3+ rooms.

- Room v1/v2: event_id is included in the PDU and trusted
- Room v3+: event_id is NOT in the wire format, it's calculated locally from content hash
- When sending events, the computed event_id is included in JSON but ignored by receivers

### 2. Event ID Validation Process

**Answer**: Synapse doesn't directly validate event_id matches content. Instead:

1. **Content Hash Validation** (`_check_sigs_and_hash` in federation_base.py):
   - Computes hash of canonical JSON (excluding signatures, unsigned, etc.)
   - Compares with the `hashes` field in the event
   - If mismatch, event is redacted (not rejected)

2. **Event ID Calculation** (`compute_event_reference_hash` in event_signing.py):
   - Prunes event to get redacted form
   - Removes signatures, age_ts, unsigned
   - Computes SHA256 of canonical JSON
   - Event ID = "$" + base64(hash)

### 3. The Circular Dependency Solution

**Answer**: The `hashes` field is excluded when computing the content hash!

```python
# In compute_content_hash:
event_dict.pop("signatures", None)
event_dict.pop("age_ts", None)  
event_dict.pop("unsigned", None)
event_dict.pop("hashes", None)  # <-- This breaks the circular dependency!
```

### 4. Why Our Test Still Fails

Even with complete database data, IDs change because:

1. **Canonical JSON Ordering**: The exact byte order matters
2. **Pruning Algorithm**: `prune_event()` creates the redacted form used for ID calculation
3. **Field Exclusions**: Various fields are excluded at different stages

### 5. Fields Used in Event ID Calculation

From `prune_event_dict` in events/utils.py, for room v3+ the allowed fields are:
- `event_id` (but removed for v3+ before hashing)
- `sender`
- `room_id` 
- `hashes` (but removed during hash calculation)
- `signatures` (removed in compute_event_reference_hash)
- `content`
- `type`
- `state_key`
- `depth`
- `prev_events`
- `auth_events`
- `origin_server_ts`

For older room versions, these additional fields are included:
- `prev_state`
- `membership` 
- `origin`

The actual hash calculation process:
1. Start with full event
2. Prune to allowed fields only
3. Remove `signatures`, `age_ts`, `unsigned` 
4. Convert to canonical JSON
5. SHA256 hash
6. Event ID = "$" + base64(hash)

### 5. The Real Problem

For disaster recovery to preserve IDs, we need to:
1. Bypass the `assert "event_id" not in event_dict` check
2. Validate that provided event_id matches calculated hash
3. Use the provided ID instead of recalculating

Currently, Synapse's architecture doesn't support this for v3+ rooms.

## Event Fields Hash Comparison

### Fields Included in Each Hash Type and Protocol

| Field               | Included in `hashes` hash | Included in Event ID hash | Sent via Federation | Sent via Client API | Notes                                                                          |
|---------------------|---------------------------|---------------------------|---------------------|---------------------|--------------------------------------------------------------------------------|
| `auth_events`       | ✅ Yes                    | ✅ Yes                    | ✅ Yes              | ❌ No               | List of authorization events                                                   |
| `content`           | ✅ Yes                    | ✅ Yes                    | ✅ Yes              | ✅ Yes              | The actual event content                                                       |
| `depth`             | ✅ Yes                    | ✅ Yes                    | ✅ Yes              | ❌ No               | DAG depth                                                                      |
| `hashes`            | ❌ No                     | ❌ No                     | ✅ Yes              | ❌ No               | Excluded to avoid circular dependency                                          |
| `origin`            | ✅ Yes                    | ❌ No*                    | ✅ Yes              | ❌ No               | Server that created the event (*included for room v1/v2)                       |
| `origin_server_ts`  | ✅ Yes                    | ✅ Yes                    | ✅ Yes              | ✅ Yes              | Timestamp                                                                      |
| `prev_events`       | ✅ Yes                    | ✅ Yes                    | ✅ Yes              | ❌ No               | Previous events in DAG                                                         |
| `room_id`           | ✅ Yes                    | ✅ Yes                    | ✅ Yes              | ✅ Yes              | Room identifier                                                                |
| `sender`            | ✅ Yes                    | ✅ Yes                    | ✅ Yes              | ✅ Yes              | User who sent the event                                                        |
| `signatures`        | ❌ No                     | ❌ No                     | ✅ Yes              | ❌ No               | Excluded from both hashes                                                      |
| `type`              | ✅ Yes                    | ✅ Yes                    | ✅ Yes              | ✅ Yes              | Event type (e.g., m.room.message)                                              |
| `state_key`         | ✅ Yes                    | ✅ Yes                    | ✅ Yes              | ✅ Yes              | For state events only                                                          |
| `event_id`          | ✅ Yes                    | ✅ Yes**                  | ✅ Yes***           | ✅ Yes              | The event identifier (**removed for v3+ before hashing, ***not in PDU for v3+) |
| `unsigned`          | ❌ No                     | ❌ No                     | ✅ Yes              | ✅ Yes              | Excluded from both hashes                                                      |
| `age_ts`            | ❌ No                     | ❌ No                     | ❌ No               | ❌ No               | Excluded from both hashes                                                      |
| `prev_state`        | ✅ Yes                    | ❌ No*                    | ✅ Yes****          | ❌ No               | Previous state (*included for room v1/v2, ****deprecated)                      |
| `membership`        | ✅ Yes                    | ❌ No*                    | ✅ Yes              | ✅ Yes*****         | For membership events (*included for room v1/v2, *****in content)              |
| `redacts`           | ✅ Yes                    | ✅ Yes***                 | ✅ Yes              | ✅ Yes              | Event being redacted (***if in content for v11+)                               |
| `outlier`           | ❌ No                     | ❌ No                     | ❌ No               | ❌ No               | Internal field                                                                 |
| `destinations`      | ❌ No                     | ❌ No                     | ❌ No               | ❌ No               | Internal field                                                                 |
| `internal_metadata` | ✅ Yes                    | ❌ No                     | ❌ No               | ❌ No               | Not part of the pruned event                                                   |
| `age`               | N/A                       | N/A                       | ✅ Yes              | ✅ Yes              | In unsigned, relative timestamp                                                |
| `transaction_id`    | N/A                       | N/A                       | ❌ No               | ✅ Yes******        | In unsigned (******for sender's clients)                                       |
| `replaces_state`    | N/A                       | N/A                       | ✅ Yes              | ✅ Yes              | In unsigned, previous state event                                              |
| `prev_content`      | N/A                       | N/A                       | ❌ No               | ✅ Yes              | In unsigned, for state changes                                                 |

### Key Insights:
1. **Content hash** (`hashes` field) includes almost everything - it's a snapshot of the event as transmitted
2. **Event ID hash** includes only fields that survive redaction - it's the permanent identity  
3. **Federation** sends internal fields like `auth_events`, `prev_events`, `depth`, `hashes`, `signatures`
4. **Client API** only sends user-visible fields - no internal DAG structure
5. This is why events can have the same ID but different content hashes if non-essential fields differ

### Critical for Disaster Recovery:
The fields needed to preserve event IDs (`auth_events`, `prev_events`, `depth`) are:
- ✅ Available via federation
- ✅ Available in the database 
- ❌ NOT available via client API

This is why disaster recovery from client API data alone cannot preserve event IDs!