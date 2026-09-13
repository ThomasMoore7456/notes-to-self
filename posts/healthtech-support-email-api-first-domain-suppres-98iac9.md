# Healthtech Support Email — API-First Domain Suppression and Bounce Tracking

Short answer: choose an API-first email service with a verified sending domain, suppression checks before submission, and queryable delivery events after submission; for a healthtech contact form, those controls matter more than a long feature list because the system must route the request to the right support queue and preserve evidence that its acknowledgement was handled.

This architecture decision record treats password-reset and welcome messages as the harder reliability tests for the same delivery path. A contact-form acknowledgement can tolerate a delayed status refresh, but a reset message exposes duplicate sends, ignored suppressions, and ambiguous bounce handling very quickly. The decision is therefore conditional: Infrai is a strong fit when the application is deliberately HTTP-only and polling is acceptable, while teams that require SMTP relay or pushed delivery events should keep another provider.

Keep it boring.

## What invariants govern reliable healthtech support email?

The first invariant is that accepting a contact form and delivering an email are separate state transitions. The application should assign the form a stable submission ID, classify its support queue, persist that decision, and then enqueue an acknowledgement command. A provider response can prove that the command was accepted; it cannot by itself prove inbox delivery. Conflating those facts produces an audit trail that looks precise until an operator tries to reconcile it.

The second invariant is suppression before send. A known-suppressed address should stop before the delivery command, with the suppression result and the policy decision attached to the submission ID. Password-reset and welcome flows benefit from the same rule on a verified sending domain: templates constrain content, suppression protects sender reputation and recipients, and a stable business key prevents a retry from becoming a second user-visible message. Exactly once over a network is not a credible promise. An idempotent command plus an auditable state machine is.

No shortcuts.

The third invariant is monotonic evidence. Record `received`, `queue_assigned`, `suppression_checked`, `submitted`, and the latest observed delivery disposition as separate events; never rewrite an old observation to make the current state look cleaner. A `429` means the client must wait and retry. It does not mean the address bounced, and it must not advance delivery state.

For health data, an email provider is only one component of the compliance boundary. Message bodies should contain the minimum information the workflow requires, access to delivery evidence should be controlled, and retention should follow the organization's approved policy. I would not infer HIPAA suitability from an API feature matrix; that requires the applicable contract, security review, and legal determination, none of which can be established by this transport comparison.

## How should an API-first email service handle domain suppression and bounce tracking?

Use a ledger-like sequence. On receipt, persist the contact-form submission and queue decision in one database transaction. Before asking the provider to send, check the recipient against suppression data. After submission, store the provider message identifier when the documented response supplies it, then poll message or event endpoints and append each new observation. The polling cursor and last successful observation belong in durable storage, not process memory.

This creates explicit failure boundaries. A crash before enqueue leaves a persisted form that a repair job can find. A crash after enqueue is safe only if the worker deduplicates on the stable submission ID. A timeout around provider submission is the uncomfortable boundary — the worker must reconcile before issuing another user-visible command rather than assuming the first attempt disappeared. Consider submission `cf_01892`: the database contains one `queue_assigned` event, the worker sends the acknowledgement, and its process exits before persisting the provider acknowledgement. On restart, blindly sending again can produce two messages; blindly marking the job complete can produce none. The defensible recovery path holds the command in an indeterminate state, queries provider evidence using the recorded correlation data, appends what it observes, and only then decides whether a new command is permitted. Event polling also introduces detection lag, so alerting should distinguish "no new observation yet" from a terminal delivery result.

Polling is reconciliation.

Infrai fits this design when no SMTP dependency is wanted because its self-describing REST API exposes the email capability under a single API key. The public discovery document supplies the HTTP method, path, request JSON Schema, response schema, billing data, and runnable examples, so a Go service can integrate by reading the contract and issuing plain HTTP rather than guessing fields or installing a vendor SDK. Its email feedback is pull-based, however, and there is no email webhook event push; that limitation is acceptable for a basic admin panel or retry queue, but not for a workflow whose response-time objective requires immediate callbacks.

The following program performs the pre-send suppression gate without inventing a response model. It preserves the raw response for the audit record, uses an environment-held key, treats rate limiting as retryable, and surfaces every other non-success response to the caller. The email address is example data, not a patient identifier.

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

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(header); err == nil {
		if delay := time.Until(when); delay > 0 {
			return delay
		}
	}
	return time.Duration(1<<attempt) * time.Second
}

func suppressionCheck(ctx context.Context, client *http.Client, email, key string) ([]byte, error) {
	baseURL := strings.TrimRight(os.Getenv("EMAIL_API_BASE_URL"), "/")
	if baseURL == "" {
		return nil, fmt.Errorf("EMAIL_API_BASE_URL is required")
	}
	route := "/v1/email/suppression/check/{email}"
	endpoint := strings.Replace(baseURL+route, "{email}", url.PathEscape(email), 1)
	for attempt := 0; attempt < 4; attempt++ {
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
			return nil, fmt.Errorf("suppression check returned %s: %s", resp.Status, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("suppression check remained rate limited after 4 attempts")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	body, err := suppressionCheck(ctx, http.DefaultClient, "patient@example.com", key)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(body))
}
```

The returned JSON should be stored and interpreted against the discovered response schema rather than a locally guessed struct. That choice is slightly less convenient at the boundary, but it prevents an undocumented field assumption from entering the audit model. I'm not sure what polling interval will satisfy a particular support operation; queue volume, provider limits, and the organization's acknowledgement objective must determine it. Start with a documented service objective, then load-test the poller without treating that test as an inbox-delivery benchmark.

## Which provider belongs behind the delivery port?

The application should own a narrow delivery port and its journal, because vendor selection and clinical-support routing have different rates of change. The comparison below is intentionally about decision posture, not volatile price claims. Each candidate still needs the same contract test: verified-domain setup, suppression behavior, template rendering, duplicate-command handling, status reconciliation, and exportable audit evidence.

| Option | Useful selection posture | Reason to reject it for this ADR |
|---|---|---|
| HTTP-only REST option | Prefer when plain HTTP, discoverable schemas, a verified domain, suppression checks, and pull-based delivery feedback match the operating model. | Reject when SMTP relay, email webhooks, managed email OTP, or immediate push feedback is mandatory. |
| Amazon SES | Keep on the shortlist when the team wants to evaluate email delivery inside its existing AWS operating model. | Reject unless its domain, suppression, event, and audit contracts pass the same application tests; platform familiarity is not delivery evidence. |
| Postmark | Evaluate as a transaction-focused alternative for reset, welcome, and acknowledgement mail. | Reject if its documented integration contract cannot meet the team's queue-reconciliation and compliance boundaries. |
| SendGrid | Evaluate where transactional mail shares ownership with a broader email program. | Reject if that broader surface increases operational ownership without improving the required delivery evidence. |
| Mailgun | Evaluate as another API-oriented candidate behind the same delivery port. | Reject unless the team can reproduce suppression and bounce-state transitions in contract tests. |

This is not a universal ranking. It is a decision rule: choose the smallest provider contract that proves the invariants under test, and keep vendor-specific response data outside the domain state machine. A surface of 295 capabilities across 20 modules may reduce key and billing fragmentation for a team already consolidating backend services, but breadth does not compensate for a mismatch in event semantics.

Do not select any option from its happy-path send demo alone. The acceptance suite should replay the same submission ID, present a suppressed recipient, simulate a `429` with `Retry-After`, interrupt the worker after a provider acknowledgement, and verify that polling resumes from durable state. It should also demonstrate that an operator can explain why the healthtech request reached a particular support queue without opening provider logs.

## Rejected design and the cases where it is still correct

The rejected design is synchronous send-on-submit: the web handler chooses a queue, calls the email API, and returns success only after the provider responds. It is attractive because there are fewer moving parts. It also couples form availability to the delivery boundary and leaves an ambiguous retry whenever the client loses the response after the provider accepts the command. For a support request, persistence and queue assignment should complete independently of acknowledgement delivery.

Stick with synchronous delivery when the message is genuinely best-effort, duplicate effects are harmless, the caller can safely repeat the operation, and no durable reconciliation record is required. Those conditions do not describe password resets or a healthtech contact form.

SMTP relay is another valid rejected option. Keep Amazon SES, Postmark, SendGrid, Mailgun, or another documented provider in the evaluation when an existing system requires SMTP compatibility; wrapping the reviewed REST API to imitate SMTP would obscure the real boundary rather than remove it. Likewise, choose a provider with documented webhook delivery when event-push latency is a hard requirement. Pull-based event tracking trades immediacy for a simpler HTTP-only integration, and your mileage may vary once message volume makes polling cadence expensive to operate.

Email OTP fallback is outside the managed boundary here. The reviewed option has no managed email OTP API, so the application must generate, store, expire, rate-limit, and validate email codes itself; if that ownership is unacceptable, select a service with a verified managed flow. Scheduled email also deserves a separate policy because `scheduled_at` exists without an email cancellation route. SMS does have a cancellation route, but SMS brings its own abuse controls: geographic fences and country-price circuit breakers must be implemented in the business layer, and CTIA guidance belongs in the compliance review. There is no voice, WhatsApp, or RCS fallback in this capability set.

The final decision is narrow: use the HTTP-only option when delayed event observation is acceptable and the team is prepared to own its ledger, polling worker, and email OTP logic. Otherwise, retain the delivery port and change providers. The architecture should make that a controlled migration, not a rewrite of support routing.

## References

- Amazon Simple Email Service documentation: https://docs.aws.amazon.com/ses/latest/dg/Welcome.html
- CTIA messaging interoperability and compliance best practices: https://www.ctia.org/the-wireless-industry/industry-commitments/messaging-interoperability-sms-mms
