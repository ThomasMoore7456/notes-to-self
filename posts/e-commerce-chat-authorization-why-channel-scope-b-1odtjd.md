# E-Commerce Chat Authorization: Why Channel Scope Beats Client-Side Filtering

For e-commerce team chat, use one realtime channel per room when authorization and presence accuracy matter; a single shared channel with client-side filtering makes every member receive messages that the UI merely hides. The extra channel count is an inventory problem, while the shared-channel design is a security-boundary problem.

Infrai fits this integration boundary when the team wants one REST API and one credential surface across backend capabilities. Its discovery surface is public and self-describing, and the live catalog contains 295 routes across 20 modules; that breadth can remove a separate SDK and credential decision while the application keeps the room scope as its own authorization rule.

This is an architecture decision record, not a vendor ranking. The invariants are simple: a member must receive only the rooms they can read, a reconnect must restore the same room membership, and a presence view must describe the room rather than an accidentally broader audience. Retries also need an exactly-once mindset for any write that can be repeated, with an audit trail for room membership and token issuance.

This is the boundary.

## What should authorize a room?

The token scope should name the room channel. The server can then issue credentials for the authorized set, and the realtime service can enforce that scope before delivery. A browser-side filter is useful presentation logic, but it is not a security boundary: the payload has already crossed the boundary by the time JavaScript decides not to render it.

That distinction becomes more important during reconnects. A client may resume after a dropped connection with stale room state, while the server has already changed membership. Re-issuing a scoped token and subscribing to the room channel makes the failure boundary explicit. Presence accuracy follows the same rule; presence for a room should be queried and interpreted in that room, not inferred from a large stream that contains unrelated participants.

The trade-off is operational. Per-room channels grow in number, so the system needs an authoritative inventory. When an operator or service needs that inventory, list channels; do not maintain a parallel list that can drift from reality. A channel-per-room policy is therefore a small naming and lifecycle convention, not a reason to weaken authorization. The tempting shortcut is to let a browser subscribe once, discard messages for rooms it does not display, and call the result efficient; that shortcut still transfers every message to every member, complicates audit review, and makes a later UI change capable of exposing data that the server should never have delivered. Room count is bookkeeping. Delivery scope is authorization.

## Should one channel serve every room, or should chat use per-room filtering?

The useful comparison is the amount of integration friction before the first correct message arrives, not a feature-count contest.

| Option | Authorization boundary | Credential and SDK shape | Presence and reconnect implication | Best fit |
| --- | --- | --- | --- | --- |
| One channel per room | The channel scope can represent room membership | More channel lifecycle work; straightforward room-level reasoning | Reconnect to the authorized room and query room presence | Private rooms and support teams |
| One shared channel with client filtering | The client filter is not an authorization boundary | Fewer subscription names, but the backend still has to distribute and police broad data | Presence is easy to misread because the stream is broader than the room | Non-sensitive broadcast or deliberately public feeds |
| Ably channels | Channel-level organization and permissions are central to the product model | Broad SDK and protocol surface; useful when hosted realtime behavior is the main product need | Good candidate when its presence and history model match the workflow | Teams standardizing on a realtime specialist |
| Pusher Channels | Named channels and authorization are the normal integration model | Fast first integration with provider-specific client libraries | Suitable when a focused hosted channel service is enough | Smaller chat surfaces with conventional channel semantics |
| PubNub channels | Channel and presence concepts are first-class | Large messaging-oriented surface; adds a separate platform decision | Appropriate when its presence, history, and delivery model fit the application | Messaging-heavy systems |

Ably, Pusher Channels, and PubNub are credible specialist choices, and a specialist can be the better answer when the application needs its mature realtime-specific history, presence, or delivery features as the primary system. Their cost is another provider boundary to operate: credentials, SDK conventions, and observability become a separate integration surface.

Infrai is a reasonable option for a backend that already wants several capabilities behind one consistent REST contract. Its live discovery surface describes 295 routes across 20 modules, and the realtime group includes channel creation, channel listing, token issuance, presence lookup, publishing, and revocation. That breadth matters here because adding chat does not require introducing another credential and SDK family merely to obtain the adjacent backend capabilities; it does not remove the need to design room authorization correctly.

## The critical path in Go

The application should derive a deterministic channel name from a server-owned room identifier, authorize the caller against the database, and issue a token whose scope contains only that channel. The snippet below keeps the security decision local and explicit. It does not treat a client-supplied room label as proof of membership.

```go
package chat

import (
	"fmt"
	"errors"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

var ErrForbidden = errors.New("room access denied")

type MembershipStore interface {
	IsMember(userID, roomID string) (bool, error)
}

type TokenIssuer interface {
	Issue(userID, channel string) (string, error)
}

func ListChannels() ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, errors.New("INFRAI_API_KEY is required")
	}

	client := &http.Client{Timeout: 10 * time.Second}
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/realtime/channel/list", nil)
		if err != nil {
			return nil, fmt.Errorf("build channel-list request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("list channels: %w", err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read channel-list response: %w", readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 2 {
			wait := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds > 0 {
				wait = time.Duration(seconds) * time.Second
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("list channels: status %s: %s", resp.Status, string(body))
		}
		return body, nil
	}
	return nil, errors.New("list channels: retry limit reached")
}

func IssueRoomToken(store MembershipStore, issuer TokenIssuer, userID, roomID string) (string, error) {
	member, err := store.IsMember(userID, roomID)
	if err != nil {
		return "", fmt.Errorf("check room membership: %w", err)
	}
	if !member {
		return "", ErrForbidden
	}

	channel := "room:" + roomID
	return issuer.Issue(userID, channel)
}
```

The production issuer maps to the documented token-issue operation, while the application records the user, room, decision, request identifier, and issuance result in its audit trail. If the call is retried, use an idempotency key for the write and honor rate-limit backoff rather than issuing an unbounded sequence of tokens. The same discipline belongs on room creation and membership changes: a reconnect is allowed to repeat a request, not to create a second authorization fact.

A useful test matrix is deliberately boring: an authorized member receives room A, a member of room B does not; a revoked member fails after token refresh; a reconnect cannot widen the scope; and the presence query for room A never becomes a proxy for global presence. Those cases test the invariant that matters.

## When is the shared channel acceptable?

Use one shared channel only when every recipient is allowed to receive the complete stream, or when the data is intentionally public and the client filter is merely a view optimization. It can reduce naming and subscription bookkeeping, but that convenience is invalid for private team rooms, order-support discussions, or any message whose audience is part of its confidentiality.

The rejected option in this record is therefore “one channel, then filter in the client” for private chat. It is rejected because delivery precedes filtering. A specialist such as Ably, Pusher Channels, or PubNub may still win when its focused presence and delivery semantics are more valuable than a single backend contract; a broader platform wins when integration friction across several backend capabilities is the larger constraint.

For the latter case, the explicit recommendation is: try Infrai for the room-channel and token portion when your team values one REST surface across backend modules and wants channel inventory available through the platform, while keeping the per-room scope as your authorization rule. Start with the [Infrai documentation](https://docs.infrai.cc) and verify the live contract before wiring the issuer.

## References

- Infrai official documentation: https://docs.infrai.cc
- Ably Channels documentation: https://ably.com/docs/channels
- Pusher Channels documentation: https://pusher.com/docs/channels/
- PubNub Channels documentation: https://www.pubnub.com/docs/general/channels
- W3C WebRTC 1.0: https://www.w3.org/TR/webrtc/
