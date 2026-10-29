# Realtime Event Types Discovery API for Node.js Client Validation (and Why I Chose One)

Short answer: validate a versioned event discovery document before subscribing, then reconnect with an opaque cursor and a documented snapshot fallback. For logistics device status, a successful WebSocket handshake proves reachability, not a compatible client contract. Keep enough transition history to recover a client, while treating current state and the retained event log as separate data products.

The client should know the event type, schema, ordering scope, delivery semantics, and cursor outcome before it renders a green connection indicator. A realtime screen that cannot recover is a temporary display.

## What is the bill actually made of?

Connections are visible, so teams often call them the main cost. Retained history is the less visible term. A useful capacity estimate is:

```text
daily_retained_bytes = events_per_day * average_event_bytes * replication_factor * retention_days
```

A warehouse scanner may emit frequent heartbeats, while a refrigerated trailer emits fewer but more consequential transitions. I keep authoritative state separately from the replay log. The state answers what is true now; the log answers which accepted transitions can this client safely replay. The current row can be compact; the event log needs an immutable event ID, a bounded replay horizon, and an explicit expired-cursor result.

For device status, I retain status transitions and a sparse liveness signal, then send a snapshot when a client subscribes. A temperature alarm followed by a clear event is not equivalent to no event merely because both arrive during one display refresh.

What I deliberately stop keeping is an unbounded sequence of identical online observations. The trade-off is a narrower quiet-period forensic view. Raw telemetry belongs to a separate retention policy; device identifiers can be linked to vehicles, workers, or locations, so compliance limits do not permit retaining operational data forever.

## Why validate event types before opening the stream?

A handshake does not prove that the client understands device.status.changed, can decode its schema, or knows what to do with a newly introduced maintenance event. This is how a connection remains green while a dashboard becomes stale.

A discovery response should be small and explicit:

```json
{
  "contract": "device-status",
  "version": 3,
  "cursor": {"kind": "opaque", "replay_window_seconds": 86400},
  "events": [
    {"type": "device.status.changed", "schema": "device-status.v2", "delivery": "at-least-once", "terminal": false},
    {"type": "device.maintenance.started", "schema": "device-maintenance.v1", "delivery": "at-least-once", "terminal": false}
  ]
}
```

An opaque cursor may be stored and presented, but it must not be parsed as a timestamp or incremented locally. Ordering is meaningful only within the scope promised by the service, such as one tenant and one device partition. An added event type is safe only if an older client can ignore unknown non-terminal types. Changing fields requires a new schema identifier; removing a type is a contract change.

## Can a reconnecting client prove it is caught up?

It can prove a bounded claim. The client presents its last accepted cursor. The server returns events after it, reports cursor expiry, or returns a snapshot with a new cursor. The client advances its cursor only after applying the event.

The validator below is independent of a vendor SDK and can run beside a Node.js transport adapter:

```go
package contract

import "fmt"

type EventCapability struct {
    Type string `json:"type"`
    Schema string `json:"schema"`
    Delivery string `json:"delivery"`
}
type Discovery struct {
    Contract string `json:"contract"`
    Version int `json:"version"`
    Events []EventCapability `json:"events"`
}

func ValidateDiscovery(d Discovery, required map[string]string) error {
    if d.Contract != "device-status" || d.Version < 1 { return fmt.Errorf("unsupported contract") }
    seen := make(map[string]string, len(d.Events))
    for _, e := range d.Events {
        if e.Type == "" || e.Schema == "" || e.Delivery != "at-least-once" { return fmt.Errorf("invalid capability") }
        seen[e.Type] = e.Schema
    }
    for typ, schema := range required {
        if seen[typ] == "" { return fmt.Errorf("missing event: %s", typ) }
        if seen[typ] != schema { return fmt.Errorf("schema mismatch for %s", typ) }
    }
    return nil
}
```

For Node.js, the application-facing adapter should expose discovery, subscribe-from-cursor, and snapshot operations. It should translate close codes and cursor-expired results into typed outcomes rather than leaking socket details into each screen.


Take a snapshot, record its cursor, subscribe after that cursor, and reconcile changes observed while the snapshot was assembled. If the snapshot has no exact cut, document an overlap strategy and make application idempotent.

Exactly-once is a useful mindset, but many transports deliver at least once. Key writes by event ID, enforce uniqueness, and commit the state mutation with the processed-event marker in one transaction. That does not make the network exactly once; it makes duplicate delivery harmless at the state boundary. Audit records should retain event ID, device ID, tenant scope, source time, ingestion time, schema, and source classification, subject to compliance retention limits.

Test unknown non-terminal events, duplicates, scoped out-of-order delivery, expired cursors, snapshots during transitions, wrong schema mappings, old clients with new discovery, and reconnect during rollout. Measure rejection reason, reconnect-to-usable-state time, replay depth, cursor expiry, duplicate application, and age of the newest applied event.

The decision rule is narrow: use a transport that supports explicit capabilities, a scoped opaque cursor, a snapshot boundary, and a replay outcome. Choose retention from the recovery promise. Make unknown-event handling, idempotency, and audit trails part of the client contract.

## Further reading

- W3C WebRTC 1.0: https://www.w3.org/TR/webrtc/
- RFC 6455, The WebSocket Protocol: https://www.rfc-editor.org/rfc/rfc6455
- RFC 9110, HTTP Semantics: https://www.rfc-editor.org/rfc/rfc9110
- JSON Schema Draft 2020-12: https://json-schema.org/draft/2020-12/json-schema-core.html
- CloudEvents Specification 1.0: https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md
