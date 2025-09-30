# Event ID Preservation for Disaster Recovery

## Status: ✅ IMPLEMENTED AND WORKING

Event ID preservation for room versions 3+ is now fully functional in the bulk injection API.

## How It Works

### Room v3+ Event IDs are Content-Addressable
```
event_id = "$" + base64(sha256(canonical_json(pruned_event)))
```

The event ID is deterministically calculated from the event's content.

### Implementation Strategy

Rather than bypassing Synapse's event creation, we leverage the fact that **identical input produces identical output**:

1. **Complete event data is provided** (including `auth_events`, `prev_events`, `depth`, `hashes`, `signatures`)
2. **Event ID is removed** before calling `make_event_from_dict()` (required by `FrozenEventV2` assertion at line 421)
3. **Synapse recalculates the event ID** from the complete cryptographic data
4. **If data is byte-perfect, the calculated ID matches the original**
5. **Validation enforces this** - mismatches are rejected to prevent desynchronization

### Validation (Security)

The bulk injection API validates event ID preservation (synapse/rest/admin/rooms.py:1227-1262):

```python
if was_complete_event and room_version.event_format >= 3:
    if original_event_id != actual_event_id:
        raise SynapseError(400, "Event ID mismatch: ...")
```

This ensures that:
- Complete events with cryptographic data MUST produce matching IDs
- Tampered or corrupted data is rejected
- Federation desynchronization is prevented

## Data Requirements

### For Event ID Preservation (Room v3+)
**Required fields** (must be byte-perfect from original event):
- `auth_events` - List of auth event IDs
- `prev_events` - List of previous event IDs
- `depth` - DAG depth
- `hashes` - SHA256 content hash
- `signatures` - Cryptographic signatures
- `origin` - Origin server name
- `origin_server_ts` - Timestamp
- `content` - Event content
- `type` - Event type
- `sender` - User ID
- `room_id` - Room ID
- `state_key` - (for state events)

### For Partial Recovery (ID changes expected)
**Minimum fields** (IDs will change, mapping provided):
- `type`, `sender`, `content`, `origin_server_ts`, `room_id`
- API auto-populates: `auth_events`, `prev_events`, `depth`

## Technical Details

### Federation Behavior (Room v3+)

Federation does NOT include event_id in PDUs for room v3+ rooms:
- Room v1/v2: `event_id` is included in the PDU and trusted
- Room v3+: `event_id` is NOT in the wire format, calculated locally from content hash
- Receivers independently calculate the ID and use that

### Event ID Calculation Process

From `compute_event_reference_hash` in event_signing.py:
1. Prune event to get redacted form (`prune_event_dict`)
2. Remove `signatures`, `age_ts`, `unsigned`
3. Convert to canonical JSON
4. SHA256 hash
5. Event ID = "$" + base64(hash)

### Fields Used in Event ID Hash (Room v3+)

From `prune_event_dict` in events/utils.py:
- `sender`, `room_id`, `content`, `type`, `state_key`
- `depth`, `prev_events`, `auth_events`, `origin_server_ts`
- `hashes`, `signatures` (included in pruned form, but removed before hashing)

**Circular dependency resolution**: The `hashes` field is excluded when computing the event ID hash, breaking the circular dependency.

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

### Data Source Requirements:

The fields needed to preserve event IDs for room v3+ are:
- ✅ **Database exports** (event_json table): Contains complete data including hashes/signatures
- ✅ **Federation sources**: Includes all internal fields needed for ID calculation
- ❌ **Client API** (`/messages`, `/sync`): Only user-visible fields, missing auth_events/prev_events/depth/hashes/signatures

**Current implementation**: The bulk injection API accepts events from any source. With complete data (database/federation), IDs are preserved. With partial data (client API), IDs change and a mapping is returned.