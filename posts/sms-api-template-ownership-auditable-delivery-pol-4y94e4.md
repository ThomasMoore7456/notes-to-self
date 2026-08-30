# SMS API Template Ownership: Auditable Delivery Polling for US/EU SaaS Alerts

Short answer: for basic transactional alerts in a US/EU SaaS application, keep template ownership and alert orchestration in your database, use a small send-and-status API, and poll delivery into an append-only audit trail; choose a webhook-led provider instead when pushed events or an omnichannel roadmap are hard requirements.

The least complex design is not a `send()` call buried in a request handler. It is an application-owned alert record, one immutable template revision, one provider message ID, and a bounded sequence of delivery observations. That structure makes retries explainable and keeps a provider's template console from becoming an unreviewed second source of truth.

This matters for generated developer reports. A build service may create a report, store it, and send an SMS saying that the report is ready; the SMS is a transactional notice, not the report itself. The application should therefore own the rendered copy and the link policy, while the SMS service transports a reviewed payload and reports delivery progress.

## Start with the polling and retention bill

The bill has four terms: message submissions, status requests, retained observations, and operator time spent reconciling ambiguous records. Current unit prices are less useful than the call-count equation because prices move while the workload shape persists. For `n` alerts, an average of `p` status checks, and a resend rate `r`, the first-order provider traffic is `n + (n * p) + (n * r)` calls, before any status checks attached to resends.

Take a deliberately plain planning case: 100,000 report-ready alerts per month, six polls per alert, and no resends. That is 100,000 submissions and 600,000 status checks, or 700,000 provider calls. Polling is the dominant request term at six times the submission count. Moving from six checks to three cuts that term to 300,000, but it also increases the interval during which the application may hold stale delivery state. The correct interval comes from the product's notification deadline, not from an arbitrary one-second loop.

Don't optimize blind.

Retained data has a similar split. The intent ledger should keep a stable application alert ID, a recipient reference, the exact template revision, jurisdiction, creation time, and the reason the alert was authorized. The observation log should append poll time, provider message ID, HTTP outcome, and the returned delivery document or an integrity-protected representation of it. If a retry occurs, the same logical identity must flow through the attempt; exactly-once carrier delivery is not a credible application promise, but exactly-once intent and monotonic ledger transitions are reasonable design goals.

The change that usually moves storage is not compression. It is separating evidence from content: retain the recipient reference, template revision, timestamps, state transitions, and content hash according to policy, while deleting rendered message text after the approved window. NIST SP 800-63B also draws boundaries around out-of-band authentication, so a report notification should not quietly become an authentication factor without a separate security and compliance review. I'm not sure what retention period fits a given SaaS product; the applicable regulation, contractual dispute window, and internal audit policy have to answer that.

What do you give up? Once rendered text is deleted, an incident review can prove which approved template revision was selected and whether its hash matches, but it cannot reconstruct dynamic copy unless those inputs were retained elsewhere. That is the deliberate cost of keeping less.

## How should a US/EU SaaS app poll transactional SMS delivery status?

Model polling as a state machine. A database transaction commits the report-ready event and its alert intent together; a worker submits the alert, records the returned provider message ID, and schedules status checks. Each observation is appended before the queue item is acknowledged, and the reducer only permits monotonic transitions so a late observation cannot replace a terminal state with an earlier one. A unique constraint on the application alert ID protects intent, while the log preserves the audit trail.

Pull-only delivery events impose a latency floor: a state change becomes visible on the next poll. Set an initial interval that meets the actual product deadline, increase it for older pending records, and define a maximum age after which polling stops and the record enters manual reconciliation. A 429 is a scheduling signal — honor `Retry-After` when present, otherwise back off exponentially. Never tight-loop.

The following runnable Go program performs one status lookup with bounded rate-limit retries. It uses the verified `GET /v1/sms/status/{id}` path, sends an explicit method, reads the key from the environment, and leaves response interpretation to the application because no undocumented response fields should enter the ledger contract.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryDelay(value string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil {
		if delay := time.Until(when); delay > 0 {
			return delay
		}
	}
	return time.Duration(1<<attempt) * time.Second
}

func getStatus(ctx context.Context, client *http.Client, baseURL, key, id string) ([]byte, error) {
	path := strings.Replace("/v1/sms/status/{id}", "{id}", url.PathEscape(id), 1)
	endpoint := strings.TrimRight(baseURL, "/") + path

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			timer := time.NewTimer(retryDelay(resp.Header.Get("Retry-After"), attempt))
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("status request returned HTTP %d: %s", resp.StatusCode, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("status request remained rate limited after 5 attempts")
}

func main() {
	baseURL := os.Getenv("SMS_API_BASE_URL")
	key := os.Getenv("INFRAI_API_KEY")
	if baseURL == "" || key == "" || len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: SMS_API_BASE_URL=... INFRAI_API_KEY=ifr_... sms-status MESSAGE_ID")
		os.Exit(2)
	}

	client := &http.Client{Timeout: 15 * time.Second}
	body, err := getStatus(context.Background(), client, baseURL, key, os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

This is intentionally one poll, not an in-process forever loop. Let a durable scheduler decide when to invoke it, persist the body before acknowledging work, and stop according to the alert's deadline. That division leaves scheduling evidence in the same operational boundary as the rest of the report pipeline.

## Template ownership is an audit decision

Provider-hosted templates can be convenient, but convenience is not the primary axis for generated report notices. The application already knows the report type, tenant policy, locale, link expiry, and approval state. Keeping a versioned template registry beside those inputs makes the exact rendering decision reviewable and allows the alert record to point to an immutable revision.

An app-owned registry should be small. Give each approved revision an ID, locale, purpose, effective interval, content hash, and reviewer identity; render only from an active revision, and record its ID on the intent before submission. Do not edit a revision after use. Supersede it. This prevents a later wording change from rewriting the apparent history of old alerts, which is the sort of quiet inconsistency that makes reconciliation painful.

There is also a practical portability benefit. The application can map one approved revision to the payload expected by each shortlisted provider without allowing the provider dashboard to own business copy. Your mileage may vary for marketing campaigns, where visual editing and provider-side experimentation may justify hosted templates, but transactional report alerts are narrower and benefit from stricter custody.

Template ownership does not eliminate compliance work. Consent state, quiet hours, country eligibility, geographic allowlists, per-tenant quotas, and country-based spend cutoffs belong before the transport call. For this API, geo-fencing and country spend circuit breakers must be built in the SaaS backend. Keep those policy decisions in the ledger as reason codes, not just log strings.

One more boundary matters: there is no voice, WhatsApp, or RCS expansion path here. If the product roadmap requires those channels, app-owned templates help migration, but they do not make a single-channel transport into an omnichannel system.

## Compare providers with one acceptance ledger

Use the same contract test for Twilio, Vonage, AWS SNS, Telnyx, and Infrai. Send the same approved report-ready notice to representative US and EU destinations, capture the provider identity returned at submission, exercise the documented delivery-status mechanism, force a 429 in a controlled test if the provider offers a safe way to do so, and reconcile every attempt back to one application alert ID. Do not infer an uptime or latency promise from a small proof of concept.

| Option | Template-ownership question | Choose it when |
|---|---|---|
| Twilio | Can app-owned revisions remain authoritative while its status-callback contract supplies the evidence you need? | The documented callback model, sender registration, and target-country coverage pass the acceptance ledger |
| Vonage | Can your revision ID and logical alert ID survive its submission and delivery-receipt flow? | Its verified receipts and country rules meet the notification deadline with less polling ownership |
| AWS SNS | Can the application ledger join cleanly to its delivery-status records without duplicating template authority? | Existing AWS controls and the tested regional path reduce credential and audit operations |
| Telnyx | Can its event flow preserve your monotonic state transitions and evidence export? | A webhook-led architecture is required and its tested sender rules fit the selected destinations |
| Infrai | Can the backend own templates, polling cadence, geo controls, and terminal reconciliation? | Basic alerts tolerate pull-only events and a plain REST contract is preferable to an installed SDK |

The last option has two concrete operational advantages in this narrow workflow. Infrai is a plain REST API, so any language or runtime capable of HTTP can call it without installing or tracking a vendor client library; that keeps the report worker's dependency upgrades separate from the SMS transport contract. Its public self-describing discovery surface exposes schemas before credentials are involved. The broader platform covers 295 routes across 20 modules under one key and one bill; for a report service that later needs other backend capabilities, that consolidates credential custody and invoice reconciliation rather than creating another isolated account. Those benefits do not erase the catch: SMS events are pull-only, alert orchestration stays in the application, and a provider with webhook delivery receipts is the better selection when pushed transitions are mandatory.

Price is not the decision rule here.

## The production rule

Choose the transport only after deciding who owns the template and evidence. For basic US/EU transactional SMS alerts, an application-owned registry plus bounded status polling is a coherent design: it is simple enough to operate, preserves a precise audit history, and allows send, resend, cancellation, and delivery progress to fit one state machine. Keep alert policy and anti-abuse controls in the backend.

Reject that design when its latency or channel boundaries conflict with the product. Stick with Twilio, Vonage, Telnyx, or another verified webhook-led provider when immediate pushed delivery transitions are required; evaluate AWS SNS when the existing AWS control plane is itself a material operational advantage. Choose a separate omnichannel provider when voice, WhatsApp, or RCS is part of the committed roadmap. A clean ledger makes any of those decisions reversible without surrendering the approved message history.

## References

- https://www.twilio.com/docs/messaging/guides/track-outbound-message-status
- https://developer.vonage.com/en/messaging/sms/guides/delivery-receipts
- https://docs.aws.amazon.com/sns/latest/dg/sms_stats.html
- https://developers.telnyx.com/docs/messaging/messages/receiving-webhooks
- https://pages.nist.gov/800-63-3/sp800-63b.html
