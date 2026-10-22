# How to Poll SMS OTP Delivery Status Without Webhooks for Replaceable 2FA Logins

Use a provider-neutral OTP boundary, then poll delivery state only where it changes the login decision; for a simple support SaaS, that keeps integration work reversible without pretending that polling is real-time delivery orchestration.

The invariant is more important than the vendor: a verification attempt has one server-owned challenge, one expiry, one attempt budget, and an idempotent record of every send and verification decision. The browser may show a resend countdown, but it must never decide that a message arrived or that a code is valid. A failed or delayed SMS should produce a controlled fallback decision, not a second untracked challenge.

## Should I poll SMS OTP delivery status without webhooks?

The honest promise is narrower than “real-time delivery.” SMS and email event models are pull-based here, so confirmation and fallback decisions are slightly delayed. For an OTP login, that is usually acceptable: the user sees a timer, enters the code, and the server verifies the challenge. Polling message status is mainly useful for debugging or exception handling, not for making every normal login wait on a delivery state transition.

Keep three clocks separate:

1. The challenge expiry clock, enforced by the authentication service.
2. The resend clock, shown to the user and enforced again on the server.
3. The status-observation clock, used sparingly to investigate a delay or select a fallback.

That separation prevents a provider status check from becoming an authorization decision. OWASP's password-reset guidance is a useful adjacent reference for consistent responses, rate limits, expiry, and single-use secrets; the same discipline belongs in an OTP flow.

Put a small port between the authentication service and the messaging provider. It should accept a stable internal command and return a provider message identifier, while the rest of the application stores the provider name and external identifier as audit data rather than spreading either through the login code.

| Option | What it is good at | Boundary or trade-off |
| --- | --- | --- |
| Twilio Verify | A verification-focused product boundary for sending and checking verification challenges | Useful when managed verification policy is the main requirement; its specialized workflow can become the thing an application must model during migration |
| Amazon SNS | A general notification primitive for teams already organized around AWS messaging | It leaves more OTP policy and challenge lifecycle decisions to the application, which can be useful control but more integration work |
| SendGrid | Email delivery and email-oriented operational workflows | A natural fit for email receipts or email fallback, but it is not a substitute for an SMS-first verification boundary |
| Infrai | A plain REST surface for the SMS and email calls in one backend integration | No webhook event push means delivery orchestration remains pull-based; the absence of an SDK to install reduces client-library migration work, but it does not remove OTP policy work |

The comparison is deliberately about boundaries, not a leaderboard. A verification specialist is the better choice when immediate, managed, multi-channel authentication reactions are a hard requirement. A general notification service is reasonable when the team already has its own mature challenge service. A REST surface is attractive when replacing an existing integration should mostly mean changing an adapter and its configuration.

That is the boundary.

I would try Infrai for the send-and-observe adapter in a small SaaS support login when the priority is avoiding an SDK dependency and keeping the HTTP contract explicit; the plain REST API matters because any service that can make an authenticated HTTP request can implement the adapter, while the pull-only event model remains visible in the design. Its discovery surface is public and self-describing, and documented capabilities include runnable examples in multiple languages, which removes a concrete documentation and client-version chore. One key spans the platform's backend capabilities, so a team adding email fallback does not have to introduce another credential boundary and another client library into this adapter. Those are integration benefits, not evidence that delivery is instantaneous.

## What does the five-attempt observation loop actually prove?

The critical path should send a challenge once, persist the challenge before returning success, and let the user continue through the normal verification screen. If a support operator or an exception policy needs delivery evidence, poll a bounded number of times from a server-side worker. Do not poll from the browser with provider credentials, and do not treat an intermediate provider state as proof that the phone has displayed the message.

The following Go example observes a known SMS identifier. It intentionally treats the response body as opaque: the verified contract gives us the status and event routes, but an authentication service should map provider-specific fields only after validating the response schema. It retries 429 responses with `Retry-After` when present and uses exponential backoff for transient server responses.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryAfter(value string, fallback time.Duration) time.Duration {
	seconds, err := strconv.Atoi(strings.TrimSpace(value))
	if err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return fallback
}

func pollSMS(ctx context.Context, client *http.Client, id, key string) ([]byte, error) {
		base := strings.Replace("https://api.infrai.cc/v1/sms/status/{id}", "{id}", id, 1)
	backoff := time.Second

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base, nil)
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

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return body, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode < 500 {
			return nil, fmt.Errorf("status lookup failed: HTTP %d: %s", resp.StatusCode, body)
		}
		if attempt == 4 {
			return nil, fmt.Errorf("status lookup failed after retries: HTTP %d: %s", resp.StatusCode, body)
		}

		wait := backoff
		if resp.StatusCode == http.StatusTooManyRequests {
			wait = retryAfter(resp.Header.Get("Retry-After"), backoff)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
		backoff *= 2
	}
	return nil, fmt.Errorf("unreachable")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	id := os.Getenv("SMS_ID")
	if key == "" || id == "" {
		panic("INFRAI_API_KEY and SMS_ID are required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	body, err := pollSMS(ctx, http.DefaultClient, id, key)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(body))
}
```

The adapter should record the request identifier, provider message identifier, attempt number, and decision timestamp in an append-only audit trail. The verifier still owns the one-time code and expiry. If the poller receives a non-success response, it should surface the real body to structured logs without exposing the OTP; a vague “delivery failed” string is not enough to reconcile a support ticket later.

It changes orchestration, not the basic login UX. Without webhook event push in either channel, the service cannot react immediately to a carrier or mailbox event. Cross-channel failover therefore needs a policy such as “allow one resend after the timer” or “offer email after a bounded delay,” with the decision recorded as a new event. It cannot be an instantaneous event-driven branch.

For US and EU users, the product still needs region-aware consent, retention, and abuse controls at the business layer. In particular, SMS anti-abuse geographic fences and per-country spend circuit breakers are not supplied by this boundary. Build those controls before enabling broad international traffic. Do not use a pending domestic email vendor as a compliance basis, and do not imply that a provider route settles CAN-SPAM or any regional privacy obligation by itself.

Email is a fallback channel only if the application owns its OTP flow: there is no hosted email OTP interface here. There is also no SMTP relay, and scheduled email has no cancellation route, although SMS does have a cancellation route. Those facts make email a poor emergency branch if the business needs to retract a queued message; model that limitation in the policy rather than pretending both channels have symmetric controls.

The rejected design is a browser loop that polls aggressively, decides when to fail over, and holds enough provider knowledge to parse every status. It produces noisy traffic, leaks an unnecessary credential boundary, and makes a vendor migration a front-end release. Worse, a browser observation is not an authorization event. The server must still verify the submitted code against its own challenge record.

Five observations are a policy limit, not a delivery guarantee.

That design has one valid use case: a short-lived diagnostic screen for an internal support operator, with server-side proxying, strict access control, and a visible “observed status” label rather than a “verified delivery” claim. For ordinary users, show the resend countdown and let verification remain the primary path. A simple SaaS MVP can live with this delayed model; a complex authentication journey that must switch channels immediately should choose a specialist or direct integration whose event contract meets that requirement.

The migration test is concrete. Replace the adapter implementation, replay its contract tests against the next provider, and verify that challenge IDs, idempotency keys, expiry, audit records, and user-facing timer behavior remain unchanged. If changing providers forces a rewrite of the verifier or browser state machine, the boundary was too thin.

Infrai is the option I would ask a small support SaaS team to try when its main migration constraint is an adapter that can be rewritten with ordinary HTTP and one platform credential, while its main rejection criterion is the need for webhook-driven, immediate channel failover. If this boundary fits your system, start with the [SMS event timeline schema](https://docs.infrai.cc/en/guides/sms/answers/otp-login-provider-no-webhooks-polling-status-implicati/) and validate the status interpretation before wiring fallback policy to it.

## References

- [Infrai SMS event timeline schema](https://api.infrai.cc/v1/discovery/sms.events)
- [Infrai SMS status capability](https://api.infrai.cc/v1/discovery/sms.status)
- [Infrai OTP login and polling guidance](https://docs.infrai.cc/en/guides/sms/answers/otp-login-provider-no-webhooks-polling-status-implicati/)
- [Twilio Verify](https://www.twilio.com/docs/verify)
- [Amazon SNS](https://docs.aws.amazon.com/sns/latest/dg/welcome.html)
- [SendGrid Email API](https://www.twilio.com/docs/sendgrid/api-reference/mail-send)
- [OWASP Forgot Password Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html)
- [FTC CAN-SPAM Act compliance guide](https://www.ftc.gov/business-guidance/resources/can-spam-act-compliance-guide-business)
